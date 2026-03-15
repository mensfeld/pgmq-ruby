# frozen_string_literal: true

RSpec.describe PGMQ::Transaction do
  context "with mocked connections" do
    let(:client) { PGMQ::Client.new(TEST_DB_PARAMS) }
    let(:mock_conn) { instance_double(PG::Connection) }
    let(:mock_pool_conn) { instance_spy(PG::Connection) }

    before do
      allow(client.connection).to receive(:with_connection).and_yield(mock_pool_conn)
    end

    describe "#transaction" do
      it "yields a transactional client" do
        allow(mock_pool_conn).to receive(:transaction).and_yield

        expect { |b| client.transaction(&b) }.to yield_with_args(
          an_instance_of(PGMQ::Transaction::TransactionalClient)
        )
      end

      it "executes block within a database transaction" do
        allow(mock_pool_conn).to receive(:transaction).and_yield

        result = client.transaction do |_txn_client|
          "transaction_result"
        end

        expect(result).to eq("transaction_result")
        expect(mock_pool_conn).to have_received(:transaction)
      end

      it "rolls back on error" do
        allow(mock_pool_conn).to receive(:transaction).and_raise(PG::Error.new("test error"))

        expect do
          client.transaction do |txn_client|
            # This should rollback
          end
        end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)
      end
    end

    describe PGMQ::Transaction::TransactionalClient do
      let(:parent_client) { PGMQ::Client.new(TEST_DB_PARAMS) }
      let(:txn_conn) { instance_double(PG::Connection) }
      let(:txn_client) { described_class.new(parent_client, txn_conn) }

      describe "#method_missing" do
        it "delegates to parent client" do
          allow(parent_client).to receive(:respond_to?).with(:list_queues, true).and_return(true)
          allow(parent_client).to receive(:list_queues).and_return([])

          result = txn_client.list_queues
          expect(result).to eq([])
        end

        it "passes through method arguments" do
          allow(parent_client).to receive(:respond_to?).with(:create, true).and_return(true)
          allow(parent_client).to receive(:create).with("test_queue")

          txn_client.create("test_queue")

          expect(parent_client).to have_received(:create).with("test_queue")
        end

        it "raises NoMethodError for undefined methods" do
          expect { txn_client.undefined_method }.to raise_error(NoMethodError)
        end
      end

      describe "#respond_to_missing?" do
        it "returns true for methods parent responds to" do
          allow(parent_client).to receive(:respond_to?).with(:create, false).and_return(true)

          expect(txn_client.respond_to?(:create)).to be true
        end

        it "returns false for methods parent does not respond to" do
          allow(parent_client).to receive(:respond_to?).with(:undefined, false).and_return(false)

          expect(txn_client.respond_to?(:undefined)).to be false
        end
      end
    end
  end

  context "with real database", :integration do
    let(:client) { create_test_client }
    let(:queue1) { test_queue_name("txn1") }
    let(:queue2) { test_queue_name("txn2") }

    before do
      ensure_test_queue(client, queue1)
      ensure_test_queue(client, queue2)
    end

    after do
      begin
        client.drop_queue(queue1)
      rescue
        nil
      end
      begin
        client.drop_queue(queue2)
      rescue
        nil
      end
      client.close
    end

    describe "successful transactions" do
      it "commits messages to multiple queues atomically" do
        client.transaction do |txn|
          txn.produce(queue1, to_json_msg({ data: "message1" }))
          txn.produce(queue2, to_json_msg({ data: "message2" }))
        end

        # Both messages should be committed
        msg1 = client.read(queue1, vt: 30)
        msg2 = client.read(queue2, vt: 30)

        expect(msg1).not_to be_nil
        expect(JSON.parse(msg1.message)["data"]).to eq("message1")
        expect(msg2).not_to be_nil
        expect(JSON.parse(msg2.message)["data"]).to eq("message2")
      end

      it "allows read and delete within transaction" do
        client.produce(queue1, to_json_msg({ order_id: 123 }))

        client.transaction do |txn|
          msg = txn.read(queue1, vt: 30)
          expect(msg).not_to be_nil
          txn.delete(queue1, msg.msg_id)
        end

        # Message should be deleted
        msg = client.read(queue1, vt: 30)
        expect(msg).to be_nil
      end

      it "supports archive operations in transaction" do
        msg_id = client.produce(queue1, to_json_msg({ data: "to_archive" }))

        client.transaction do |txn|
          txn.archive(queue1, msg_id)
        end

        # Message should be archived (not in main queue)
        msg = client.read(queue1, vt: 30)
        expect(msg).to be_nil
      end

      it "returns transaction block result" do
        result = client.transaction do |txn|
          txn.produce(queue1, { test: "data" })
          "transaction_result"
        end

        expect(result).to eq("transaction_result")
      end
    end

    describe "transaction rollback" do
      it "rolls back on raised exception" do
        expect do
          client.transaction do |txn|
            txn.produce(queue1, { data: "will_rollback" })
            raise "Test error"
          end
        end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

        # Message should not be persisted
        msg = client.read(queue1, vt: 30)
        expect(msg).to be_nil
      end

      it "rolls back all queue operations on error" do
        expect do
          client.transaction do |txn|
            txn.produce(queue1, { data: "message1" })
            txn.produce(queue2, { data: "message2" })
            raise StandardError, "Rollback trigger"
          end
        end.to raise_error(PGMQ::Errors::ConnectionError)

        # Neither message should be persisted
        msg1 = client.read(queue1, vt: 30)
        msg2 = client.read(queue2, vt: 30)

        expect(msg1).to be_nil
        expect(msg2).to be_nil
      end

      it "does not persist messages after rollback" do
        # Send message outside transaction first
        client.produce(queue1, to_json_msg({ data: "before_txn" }))

        expect do
          client.transaction do |txn|
            txn.produce(queue1, { data: "in_txn" })
            raise "Rollback"
          end
        end.to raise_error(PGMQ::Errors::ConnectionError)

        # Only the first message should exist
        msg1 = client.read(queue1, vt: 30)
        expect(JSON.parse(msg1.message)["data"]).to eq("before_txn")

        msg2 = client.read(queue1, vt: 30)
        expect(msg2).to be_nil
      end

      it "properly handles PG::Error during transaction" do
        expect do
          client.transaction do |txn|
            txn.produce(queue1, { data: "test" })
            # Trigger a PG error by using invalid SQL
            txn.connection.with_connection do |conn|
              conn.exec("INVALID SQL STATEMENT")
            end
          end
        end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

        # Message should not be persisted
        msg = client.read(queue1, vt: 30)
        expect(msg).to be_nil
      end
    end

    describe "connection management" do
      it "does not leak connections from transaction" do
        pool_size_before = client.connection.pool.available

        10.times do
          client.transaction do |txn|
            txn.produce(queue1, { iteration: "test" })
          end
        end

        # Clean up messages
        client.purge_queue(queue1)

        pool_size_after = client.connection.pool.available
        expect(pool_size_after).to eq(pool_size_before)
      end
    end
  end
end
