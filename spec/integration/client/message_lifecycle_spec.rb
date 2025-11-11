# frozen_string_literal: true

RSpec.describe PGMQ::Client::MessageLifecycle, :integration do
  let(:client) { create_test_client }
  let(:queue_name) { test_queue_name }

  after do
    begin
      client.drop_queue(queue_name)
    rescue StandardError
      nil
    end
    client.close
  end

  before { ensure_test_queue(client, queue_name) }

  describe '#pop' do
    it 'pops a message (atomic read+delete)' do
      client.send(queue_name, to_json_msg({ test: 'pop' }))

      msg = client.pop(queue_name)
      expect(msg).to be_a(PGMQ::Message)
      expect(JSON.parse(msg.message)).to eq({ 'test' => 'pop' })

      # Message should be deleted
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it 'returns nil for empty queue' do
      msg = client.pop(queue_name)
      expect(msg).to be_nil
    end
  end

  describe '#delete' do
    it 'deletes a message' do
      client.send(queue_name, to_json_msg({ test: 'data' }))
      msg = client.read(queue_name, vt: 30)

      result = client.delete(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be readable again
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it 'returns false for non-existent message' do
      result = client.delete(queue_name, 99_999)
      expect(result).to be false
    end
  end

  describe '#delete_batch' do
    it 'deletes multiple messages' do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      client.send_batch(queue_name, batch)
      messages = client.read_batch(queue_name, vt: 30, qty: 3)

      deleted_ids = client.delete_batch(queue_name, messages.map(&:msg_id))
      expect(deleted_ids.size).to eq(3)

      # No messages should remain
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it 'handles empty array' do
      deleted_ids = client.delete_batch(queue_name, [])
      expect(deleted_ids).to eq([])
    end
  end

  describe '#archive' do
    it 'archives a message' do
      client.send(queue_name, to_json_msg({ test: 'archive' }))
      msg = client.read(queue_name, vt: 30)

      result = client.archive(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be in main queue
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it 'returns false for non-existent message' do
      result = client.archive(queue_name, 99_999)
      expect(result).to be false
    end
  end

  describe '#archive_batch' do
    it 'archives multiple messages' do
      client.send_batch(queue_name, [to_json_msg({ a: 1 }), to_json_msg({ b: 2 })])
      messages = client.read_batch(queue_name, vt: 30, qty: 2)

      archived_ids = client.archive_batch(queue_name, messages.map(&:msg_id))
      expect(archived_ids.size).to eq(2)

      # Messages should not be in main queue
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it 'handles empty array' do
      archived_ids = client.archive_batch(queue_name, [])
      expect(archived_ids).to eq([])
    end
  end

  describe '#set_vt' do
    it 'updates visibility timeout' do
      client.send(queue_name, to_json_msg({ test: 'vt' }))
      msg = client.read(queue_name, vt: 5)

      updated_msg = client.set_vt(queue_name, msg.msg_id, vt_offset: 60)
      expect(updated_msg).to be_a(PGMQ::Message)
      expect(updated_msg.vt).to be > msg.vt
    end

    it 'returns nil for non-existent message' do
      updated_msg = client.set_vt(queue_name, 99_999, vt_offset: 60)
      expect(updated_msg).to be_nil
    end
  end
end
