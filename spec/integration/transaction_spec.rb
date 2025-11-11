# frozen_string_literal: true

RSpec.describe PGMQ::Transaction, :integration do
  let(:client) { create_test_client }
  let(:queue1) { test_queue_name('txn1') }
  let(:queue2) { test_queue_name('txn2') }

  before do
    ensure_test_queue(client, queue1)
    ensure_test_queue(client, queue2)
  end

  after do
    begin
      client.drop_queue(queue1)
    rescue StandardError
      nil
    end
    begin
      client.drop_queue(queue2)
    rescue StandardError
      nil
    end
    client.close
  end

  describe 'successful transactions' do
    it 'commits messages to multiple queues atomically' do
      client.transaction do |txn|
        txn.send(queue1, to_json_msg({ data: 'message1' }))
        txn.send(queue2, to_json_msg({ data: 'message2' }))
      end

      # Both messages should be committed
      msg1 = client.read(queue1, vt: 30)
      msg2 = client.read(queue2, vt: 30)

      expect(msg1).not_to be_nil
      expect(JSON.parse(msg1.message)['data']).to eq('message1')
      expect(msg2).not_to be_nil
      expect(JSON.parse(msg2.message)['data']).to eq('message2')
    end

    it 'allows read and delete within transaction' do
      client.send(queue1, to_json_msg({ order_id: 123 }))

      client.transaction do |txn|
        msg = txn.read(queue1, vt: 30)
        expect(msg).not_to be_nil
        txn.delete(queue1, msg.msg_id)
      end

      # Message should be deleted
      msg = client.read(queue1, vt: 30)
      expect(msg).to be_nil
    end

    it 'supports archive operations in transaction' do
      msg_id = client.send(queue1, to_json_msg({ data: 'to_archive' }))

      client.transaction do |txn|
        txn.archive(queue1, msg_id)
      end

      # Message should be archived (not in main queue)
      msg = client.read(queue1, vt: 30)
      expect(msg).to be_nil
    end

    it 'returns transaction block result' do
      result = client.transaction do |txn|
        txn.send(queue1, { test: 'data' })
        'transaction_result'
      end

      expect(result).to eq('transaction_result')
    end
  end

  describe 'transaction rollback' do
    it 'rolls back on raised exception' do
      expect do
        client.transaction do |txn|
          txn.send(queue1, { data: 'will_rollback' })
          raise 'Test error'
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

      # Message should not be persisted
      msg = client.read(queue1, vt: 30)
      expect(msg).to be_nil
    end

    it 'rolls back all queue operations on error' do
      expect do
        client.transaction do |txn|
          txn.send(queue1, { data: 'message1' })
          txn.send(queue2, { data: 'message2' })
          raise StandardError, 'Rollback trigger'
        end
      end.to raise_error(PGMQ::Errors::ConnectionError)

      # Neither message should be persisted
      msg1 = client.read(queue1, vt: 30)
      msg2 = client.read(queue2, vt: 30)

      expect(msg1).to be_nil
      expect(msg2).to be_nil
    end

    it 'does not persist messages after rollback' do
      # Send message outside transaction first
      client.send(queue1, to_json_msg({ data: 'before_txn' }))

      expect do
        client.transaction do |txn|
          txn.send(queue1, { data: 'in_txn' })
          raise 'Rollback'
        end
      end.to raise_error(PGMQ::Errors::ConnectionError)

      # Only the first message should exist
      msg1 = client.read(queue1, vt: 30)
      expect(JSON.parse(msg1.message)['data']).to eq('before_txn')

      msg2 = client.read(queue1, vt: 30)
      expect(msg2).to be_nil
    end

    it 'properly handles PG::Error during transaction' do
      expect do
        client.transaction do |txn|
          txn.send(queue1, { data: 'test' })
          # Trigger a PG error by using invalid SQL
          txn.connection.with_connection do |conn|
            conn.exec('INVALID SQL STATEMENT')
          end
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

      # Message should not be persisted
      msg = client.read(queue1, vt: 30)
      expect(msg).to be_nil
    end
  end

  describe 'connection management' do
    it 'does not leak connections from transaction' do
      pool_size_before = client.connection.pool.available

      10.times do
        client.transaction do |txn|
          txn.send(queue1, { iteration: 'test' })
        end
      end

      # Clean up messages
      client.purge_queue(queue1)

      pool_size_after = client.connection.pool.available
      expect(pool_size_after).to eq(pool_size_before)
    end
  end
end
