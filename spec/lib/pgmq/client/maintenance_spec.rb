# frozen_string_literal: true

RSpec.describe PGMQ::Client::Maintenance, :integration do
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

  describe '#purge_queue' do
    it 'purges all messages from queue' do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      client.send_batch(queue_name, batch)

      count = client.purge_queue(queue_name)
      expect(count).to eq('3')

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it 'returns 0 for empty queue' do
      count = client.purge_queue(queue_name)
      expect(count).to eq('0')
    end
  end

  describe '#detach_archive' do
    it 'detaches archive table from queue management' do
      # Send and archive a message first
      msg_id = client.send(queue_name, to_json_msg({ test: 'archive' }))
      client.archive(queue_name, msg_id)

      # Detach the archive
      client.detach_archive(queue_name)

      # Queue should still exist and be usable
      new_msg_id = client.send(queue_name, to_json_msg({ test: 'after_detach' }))
      msg = client.read(queue_name, vt: 30)
      expect(msg.msg_id).to eq(new_msg_id)
    end

    it 'raises error for invalid queue name' do
      expect { client.detach_archive('123invalid') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end
end
