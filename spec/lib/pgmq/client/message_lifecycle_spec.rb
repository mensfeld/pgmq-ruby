# frozen_string_literal: true

RSpec.describe PGMQ::Client::MessageLifecycle, :integration do
  let(:client) { create_test_client }
  let(:queue_name) { test_queue_name }

  after do
    begin
      client.drop_queue(queue_name)
    rescue
      nil
    end
    client.close
  end

  before { ensure_test_queue(client, queue_name) }

  describe "#pop" do
    it "pops a message (atomic read+delete)" do
      client.produce(queue_name, to_json_msg({ test: "pop" }))

      msg = client.pop(queue_name)
      expect(msg).to be_a(PGMQ::Message)
      expect(JSON.parse(msg.message)).to eq({ "test" => "pop" })

      # Message should be deleted
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it "returns nil for empty queue" do
      msg = client.pop(queue_name)
      expect(msg).to be_nil
    end
  end

  describe "#pop_batch" do
    it "pops multiple messages atomically" do
      client.produce_batch(queue_name, [
        to_json_msg({ n: 1 }),
        to_json_msg({ n: 2 }),
        to_json_msg({ n: 3 })
      ])

      messages = client.pop_batch(queue_name, 3)

      expect(messages.size).to eq(3)
      expect(messages).to all(be_a(PGMQ::Message))

      # All messages should be deleted
      remaining = client.read(queue_name, vt: 30)
      expect(remaining).to be_nil
    end

    it "returns only available messages when qty exceeds queue size" do
      client.produce_batch(queue_name, [to_json_msg({ n: 1 }), to_json_msg({ n: 2 })])

      messages = client.pop_batch(queue_name, 10)

      expect(messages.size).to eq(2)
    end

    it "returns empty array for empty queue" do
      messages = client.pop_batch(queue_name, 5)
      expect(messages).to eq([])
    end

    it "returns empty array when qty is zero" do
      client.produce(queue_name, to_json_msg({ test: "data" }))
      messages = client.pop_batch(queue_name, 0)
      expect(messages).to eq([])

      # Message should still be there
      msg = client.pop(queue_name)
      expect(msg).not_to be_nil
    end

    it "raises error for invalid queue name" do
      expect { client.pop_batch("123invalid", 5) }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end

  describe "#delete" do
    it "deletes a message" do
      client.produce(queue_name, to_json_msg({ test: "data" }))
      msg = client.read(queue_name, vt: 30)

      result = client.delete(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be readable again
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it "returns false for non-existent message" do
      result = client.delete(queue_name, 99_999)
      expect(result).to be false
    end
  end

  describe "#delete_batch" do
    it "deletes multiple messages" do
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      client.produce_batch(queue_name, batch)
      messages = client.read_batch(queue_name, vt: 30, qty: 3)

      deleted_ids = client.delete_batch(queue_name, messages.map(&:msg_id))
      expect(deleted_ids.size).to eq(3)

      # No messages should remain
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it "handles empty array" do
      deleted_ids = client.delete_batch(queue_name, [])
      expect(deleted_ids).to eq([])
    end
  end

  describe "#archive" do
    it "archives a message" do
      client.produce(queue_name, to_json_msg({ test: "archive" }))
      msg = client.read(queue_name, vt: 30)

      result = client.archive(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be in main queue
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it "returns false for non-existent message" do
      result = client.archive(queue_name, 99_999)
      expect(result).to be false
    end
  end

  describe "#archive_batch" do
    it "archives multiple messages" do
      client.produce_batch(queue_name, [to_json_msg({ a: 1 }), to_json_msg({ b: 2 })])
      messages = client.read_batch(queue_name, vt: 30, qty: 2)

      archived_ids = client.archive_batch(queue_name, messages.map(&:msg_id))
      expect(archived_ids.size).to eq(2)

      # Messages should not be in main queue
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it "handles empty array" do
      archived_ids = client.archive_batch(queue_name, [])
      expect(archived_ids).to eq([])
    end
  end

  describe "#set_vt" do
    it "updates visibility timeout with integer offset" do
      client.produce(queue_name, to_json_msg({ test: "vt" }))
      msg = client.read(queue_name, vt: 5)

      updated_msg = client.set_vt(queue_name, msg.msg_id, vt: 60)
      expect(updated_msg).to be_a(PGMQ::Message)
      expect(updated_msg.vt).to be > msg.vt
    end

    it "updates visibility timeout with absolute timestamp" do
      skip "PGMQ v1.11.0+ required for timestamp support" unless pgmq_supports_set_vt_timestamp?

      client.produce(queue_name, to_json_msg({ test: "vt_timestamp" }))
      msg = client.read(queue_name, vt: 5)

      future_time = Time.now + 120
      updated_msg = client.set_vt(queue_name, msg.msg_id, vt: future_time)

      expect(updated_msg).to be_a(PGMQ::Message)
      expect(updated_msg.vt).to be > msg.vt
    end

    it "returns nil for non-existent message" do
      updated_msg = client.set_vt(queue_name, 99_999, vt: 60)
      expect(updated_msg).to be_nil
    end

  end

  describe "#set_vt_batch" do
    it "updates visibility timeout for multiple messages with integer offset" do
      client.produce_batch(queue_name, [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })])
      messages = client.read_batch(queue_name, vt: 5, qty: 3)
      original_vts = messages.map(&:vt)

      updated_messages = client.set_vt_batch(queue_name, messages.map(&:msg_id), vt: 120)

      expect(updated_messages.size).to eq(3)
      expect(updated_messages).to all(be_a(PGMQ::Message))
      updated_messages.each_with_index do |msg, i|
        expect(msg.vt).to be > original_vts[i]
      end
    end

    it "updates visibility timeout with absolute timestamp" do
      skip "PGMQ v1.11.0+ required for timestamp support" unless pgmq_supports_set_vt_timestamp?

      client.produce_batch(queue_name, [to_json_msg({ a: 1 }), to_json_msg({ b: 2 })])
      messages = client.read_batch(queue_name, vt: 5, qty: 2)
      original_vts = messages.map(&:vt)

      future_time = Time.now + 180
      updated_messages = client.set_vt_batch(queue_name, messages.map(&:msg_id), vt: future_time)

      expect(updated_messages.size).to eq(2)
      expect(updated_messages).to all(be_a(PGMQ::Message))
      updated_messages.each_with_index do |msg, i|
        expect(msg.vt).to be > original_vts[i]
      end
    end

    it "handles empty array" do
      updated_messages = client.set_vt_batch(queue_name, [], vt: 60)
      expect(updated_messages).to eq([])
    end

    it "returns empty array for non-existent messages" do
      updated_messages = client.set_vt_batch(queue_name, [99_998, 99_999], vt: 60)
      expect(updated_messages).to eq([])
    end

  end

  describe "#set_vt_multi" do
    let(:queue2) { test_queue_name("vt_multi2") }

    before { ensure_test_queue(client, queue2) }

    after do
      client.drop_queue(queue2)
    rescue
      nil
    end

    it "updates visibility timeout for messages from multiple queues with integer offset" do
      # Send messages to both queues
      client.produce_batch(queue_name, [to_json_msg({ a: 1 }), to_json_msg({ a: 2 })])
      client.produce_batch(queue2, [to_json_msg({ b: 1 })])

      # Read messages
      msgs1 = client.read_batch(queue_name, vt: 5, qty: 2)
      msgs2 = client.read_batch(queue2, vt: 5, qty: 1)

      original_vts1 = msgs1.map(&:vt)
      original_vts2 = msgs2.map(&:vt)

      # Update visibility timeout for all
      result = client.set_vt_multi({
        queue_name => msgs1.map(&:msg_id),
        queue2 => msgs2.map(&:msg_id)
      }, vt: 120)

      expect(result.keys).to contain_exactly(queue_name, queue2)
      expect(result[queue_name].size).to eq(2)
      expect(result[queue2].size).to eq(1)

      result[queue_name].each_with_index do |msg, i|
        expect(msg.vt).to be > original_vts1[i]
      end

      result[queue2].each do |msg|
        expect(msg.vt).to be > original_vts2.first
      end
    end

    it "updates visibility timeout with absolute timestamp" do
      skip "PGMQ v1.11.0+ required for timestamp support" unless pgmq_supports_set_vt_timestamp?

      client.produce_batch(queue_name, [to_json_msg({ a: 1 })])
      client.produce_batch(queue2, [to_json_msg({ b: 1 })])

      msgs1 = client.read_batch(queue_name, vt: 5, qty: 1)
      msgs2 = client.read_batch(queue2, vt: 5, qty: 1)

      original_vts1 = msgs1.map(&:vt)
      original_vts2 = msgs2.map(&:vt)

      future_time = Time.now + 200
      result = client.set_vt_multi({
        queue_name => msgs1.map(&:msg_id),
        queue2 => msgs2.map(&:msg_id)
      }, vt: future_time)

      result[queue_name].each_with_index do |msg, i|
        expect(msg.vt).to be > original_vts1[i]
      end

      result[queue2].each do |msg|
        expect(msg.vt).to be > original_vts2.first
      end
    end

    it "returns empty hash for empty input" do
      result = client.set_vt_multi({}, vt: 60)
      expect(result).to eq({})
    end

    it "raises ArgumentError when updates is not a hash" do
      expect { client.set_vt_multi([], vt: 60) }.to raise_error(ArgumentError, /must be a hash/)
    end

    it "skips queues with empty message arrays" do
      client.produce(queue_name, to_json_msg({ test: 1 }))
      msg = client.read(queue_name, vt: 5)

      result = client.set_vt_multi({
        queue_name => [msg.msg_id],
        queue2 => []
      }, vt: 60)

      expect(result.keys).to contain_exactly(queue_name)
      expect(result[queue_name].size).to eq(1)
    end
  end
end
