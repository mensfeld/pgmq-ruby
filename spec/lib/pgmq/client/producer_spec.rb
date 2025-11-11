# frozen_string_literal: true

RSpec.describe PGMQ::Client::Producer, :integration do
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

  describe '#send' do
    it 'sends a message to the queue' do
      message_data = { order_id: 123, status: 'pending' }
      msg_id = client.send(queue_name, to_json_msg(message_data))

      expect(msg_id).to be_a(String)
      expect(msg_id.to_i).to be > 0

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_a(PGMQ::Message)
      expect(msg.msg_id).to eq(msg_id)
      expect(JSON.parse(msg.message)).to eq({ 'order_id' => 123, 'status' => 'pending' })
    end

    it 'sends message with delay' do
      msg_id = client.send(queue_name, to_json_msg({ test: 'data' }), delay: 2)
      expect(msg_id).to be_a(String)

      # Message should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      # Wait for delay
      sleep 2.5

      # Now message should be visible
      msg = client.read(queue_name, vt: 30)
      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)).to eq({ 'test' => 'data' })
    end
  end

  describe '#send_batch' do
    it 'sends multiple messages to the queue' do
      messages = [
        to_json_msg({ id: 1, data: 'first' }),
        to_json_msg({ id: 2, data: 'second' }),
        to_json_msg({ id: 3, data: 'third' })
      ]

      msg_ids = client.send_batch(queue_name, messages)
      expect(msg_ids).to be_an(Array)
      expect(msg_ids.size).to eq(3)

      read_messages = client.read_batch(queue_name, vt: 30, qty: 3)
      expect(read_messages.size).to eq(3)
      parsed_data = read_messages.map { |m| JSON.parse(m.message)['data'] }
      expect(parsed_data).to contain_exactly('first', 'second', 'third')
    end

    it 'handles empty batch' do
      msg_ids = client.send_batch(queue_name, [])
      expect(msg_ids).to eq([])
    end

    it 'sends batch with delay' do
      messages = [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })]

      client.send_batch(queue_name, messages, delay: 2)

      # Messages should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      # Wait for delay
      sleep 2.5

      # Now messages should be visible
      read_messages = client.read_batch(queue_name, vt: 30, qty: 2)
      expect(read_messages.size).to eq(2)
    end
  end
end
