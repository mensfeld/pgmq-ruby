# frozen_string_literal: true

require "test_helper"

describe PGMQ::Transaction do
  context "with mocked connections" do
    before do
      @client = PGMQ::Client.new(TEST_DB_PARAMS)
      @mock_pool_conn = stub_everything("pool_conn")
      @client.connection.stubs(:with_connection).yields(@mock_pool_conn)
    end

    describe "#transaction" do
      it "yields a transactional client" do
        @mock_pool_conn.stubs(:transaction).yields

        yielded = nil
        @client.transaction { |txn| yielded = txn }

        assert_kind_of PGMQ::Transaction::TransactionalClient, yielded
      end

      it "executes block within a database transaction" do
        # Need proper block return value propagation (mocha yields doesn't do this)
        conn = @mock_pool_conn
        conn.define_singleton_method(:transaction) { |&blk| blk.call }
        @client.connection.unstub(:with_connection)
        @client.connection.define_singleton_method(:with_connection) { |&blk| blk.call(conn) }

        result = @client.transaction do |_txn_client|
          "transaction_result"
        end

        assert_equal "transaction_result", result
      end

      it "rolls back on error" do
        @mock_pool_conn.stubs(:transaction).raises(PG::Error.new("test error"))

        e = assert_raises(PGMQ::Errors::ConnectionError) do
          @client.transaction { |txn_client| }
        end
        assert_match(/Transaction failed/, e.message)
      end
    end

    describe PGMQ::Transaction::TransactionalClient do
      before do
        @parent_client = PGMQ::Client.new(TEST_DB_PARAMS)
        @txn_conn = mock("txn_conn")
        @txn_client = PGMQ::Transaction::TransactionalClient.new(@parent_client, @txn_conn)
      end

      describe "#method_missing" do
        it "delegates to parent client" do
          @parent_client.stubs(:respond_to?).with(:list_queues, true).returns(true)
          @parent_client.stubs(:list_queues).returns([])

          result = @txn_client.list_queues

          assert_equal [], result
        end

        it "passes through method arguments" do
          @parent_client.stubs(:respond_to?).with(:create, true).returns(true)
          @parent_client.expects(:create).with("test_queue")

          @txn_client.create("test_queue")
        end

        it "raises NoMethodError for undefined methods" do
          assert_raises(NoMethodError) { @txn_client.undefined_method }
        end
      end

      describe "#respond_to_missing?" do
        it "returns true for methods parent responds to" do
          @parent_client.stubs(:respond_to?).with(:create, false).returns(true)

          assert_respond_to @txn_client, :create
        end

        it "returns false for methods parent does not respond to" do
          @parent_client.stubs(:respond_to?).with(:undefined, false).returns(false)

          refute_respond_to @txn_client, :undefined
        end
      end
    end
  end

  context "with real database" do
    before do
      @client = create_test_client
      @queue1 = unique_queue_name("txn1")
      @queue2 = unique_queue_name("txn2")
      ensure_test_queue(@client, @queue1)
      ensure_test_queue(@client, @queue2)
    end

    after do
      begin
        @client.drop_queue(@queue1)
      rescue
        nil
      end
      begin
        @client.drop_queue(@queue2)
      rescue
        nil
      end
      @client.close
    end

    describe "successful transactions" do
      it "commits messages to multiple queues atomically" do
        @client.transaction do |txn|
          txn.produce(@queue1, to_json_msg({ data: "message1" }))
          txn.produce(@queue2, to_json_msg({ data: "message2" }))
        end

        # Both messages should be committed
        msg1 = @client.read(@queue1, vt: 30)
        msg2 = @client.read(@queue2, vt: 30)

        refute_nil msg1
        assert_equal "message1", JSON.parse(msg1.message)["data"]
        refute_nil msg2
        assert_equal "message2", JSON.parse(msg2.message)["data"]
      end

      it "allows read and delete within transaction" do
        @client.produce(@queue1, to_json_msg({ order_id: 123 }))

        @client.transaction do |txn|
          msg = txn.read(@queue1, vt: 30)

          refute_nil msg
          txn.delete(@queue1, msg.msg_id)
        end

        # Message should be deleted
        msg = @client.read(@queue1, vt: 30)

        assert_nil msg
      end

      it "supports archive operations in transaction" do
        msg_id = @client.produce(@queue1, to_json_msg({ data: "to_archive" }))

        @client.transaction do |txn|
          txn.archive(@queue1, msg_id)
        end

        # Message should be archived (not in main queue)
        msg = @client.read(@queue1, vt: 30)

        assert_nil msg
      end

      it "returns transaction block result" do
        result = @client.transaction do |txn|
          txn.produce(@queue1, { test: "data" })
          "transaction_result"
        end

        assert_equal "transaction_result", result
      end
    end

    describe "transaction rollback" do
      it "rolls back on raised exception" do
        e = assert_raises(PGMQ::Errors::ConnectionError) do
          @client.transaction do |txn|
            txn.produce(@queue1, { data: "will_rollback" })
            raise "Test error"
          end
        end
        assert_match(/Transaction failed/, e.message)

        # Message should not be persisted
        msg = @client.read(@queue1, vt: 30)

        assert_nil msg
      end

      it "rolls back all queue operations on error" do
        assert_raises(PGMQ::Errors::ConnectionError) do
          @client.transaction do |txn|
            txn.produce(@queue1, { data: "message1" })
            txn.produce(@queue2, { data: "message2" })
            raise StandardError, "Rollback trigger"
          end
        end

        # Neither message should be persisted
        msg1 = @client.read(@queue1, vt: 30)
        msg2 = @client.read(@queue2, vt: 30)

        assert_nil msg1
        assert_nil msg2
      end

      it "does not persist messages after rollback" do
        # Send message outside transaction first
        @client.produce(@queue1, to_json_msg({ data: "before_txn" }))

        assert_raises(PGMQ::Errors::ConnectionError) do
          @client.transaction do |txn|
            txn.produce(@queue1, { data: "in_txn" })
            raise "Rollback"
          end
        end

        # Only the first message should exist
        msg1 = @client.read(@queue1, vt: 30)

        assert_equal "before_txn", JSON.parse(msg1.message)["data"]

        msg2 = @client.read(@queue1, vt: 30)

        assert_nil msg2
      end

      it "properly handles PG::Error during transaction" do
        e = assert_raises(PGMQ::Errors::ConnectionError) do
          @client.transaction do |txn|
            txn.produce(@queue1, { data: "test" })
            # Trigger a PG error by using invalid SQL
            txn.connection.with_connection do |conn|
              conn.exec("INVALID SQL STATEMENT")
            end
          end
        end
        assert_match(/Transaction failed/, e.message)

        # Message should not be persisted
        msg = @client.read(@queue1, vt: 30)

        assert_nil msg
      end
    end

    describe "connection management" do
      it "does not leak connections from transaction" do
        pool_size_before = @client.connection.pool.available

        10.times do
          @client.transaction do |txn|
            txn.produce(@queue1, { iteration: "test" })
          end
        end

        # Clean up messages
        @client.purge_queue(@queue1)

        pool_size_after = @client.connection.pool.available

        assert_equal pool_size_before, pool_size_after
      end
    end
  end
end
