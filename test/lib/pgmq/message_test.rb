# frozen_string_literal: true

require "test_helper"

describe PGMQ::Message do
  before do
    @row = {
      "msg_id" => "123",
      "read_ct" => "2",
      "enqueued_at" => "2025-01-15 10:00:00 UTC",
      "last_read_at" => "2025-01-15 10:01:00 UTC",
      "vt" => "2025-01-15 10:00:30 UTC",
      "message" => '{"order_id":456,"status":"pending"}',
      "headers" => '{"trace_id":"abc123"}',
      "queue_name" => nil
    }
    @message = PGMQ::Message.new(@row)
  end

  describe "#initialize" do
    it "returns raw message ID as string" do
      assert_equal "123", @message.msg_id
    end

    it "returns raw read count as string" do
      assert_equal "2", @message.read_ct
    end

    it "returns raw enqueued_at as string" do
      assert_equal "2025-01-15 10:00:00 UTC", @message.enqueued_at
    end

    it "returns raw last_read_at as string" do
      assert_equal "2025-01-15 10:01:00 UTC", @message.last_read_at
    end

    it "returns raw vt as string" do
      assert_equal "2025-01-15 10:00:30 UTC", @message.vt
    end

    it "returns raw JSON message string" do
      assert_equal '{"order_id":456,"status":"pending"}', @message.message
    end

    it "returns raw headers as JSON string" do
      assert_equal '{"trace_id":"abc123"}', @message.headers
    end

    it "returns queue_name when present" do
      row_with_queue = @row.merge("queue_name" => "test_queue")
      msg = PGMQ::Message.new(row_with_queue)

      assert_equal "test_queue", msg.queue_name
    end

    it "returns nil for last_read_at when message has never been read" do
      row_without_read = @row.merge("last_read_at" => nil, "read_ct" => "0")
      msg = PGMQ::Message.new(row_without_read)

      assert_nil msg.last_read_at
    end
  end

  describe "#id" do
    it "returns message ID (alias for msg_id)" do
      assert_equal "123", @message.id
    end
  end

  describe "Data immutability" do
    it "is immutable (Data object)" do
      assert_raises(NoMethodError) { @message.msg_id = "456" }
    end

    it "provides equality based on values" do
      msg1 = PGMQ::Message.new(@row)
      msg2 = PGMQ::Message.new(@row)

      assert_equal msg1, msg2
    end
  end

  describe "#to_h" do
    it "returns hash representation from Data" do
      hash = @message.to_h

      assert_equal "123", hash[:msg_id]
      assert_equal "2", hash[:read_ct]
      assert_equal '{"order_id":456,"status":"pending"}', hash[:message]
    end
  end

  describe "#inspect" do
    it "returns Data string representation" do
      assert_includes @message.inspect, "PGMQ::Message"
      assert_includes @message.inspect, 'msg_id="123"'
    end
  end

  context "with Hash message (from PostgreSQL JSONB)" do
    it "returns the hash as-is (pg gem can return JSONB as Hash)" do
      row_with_hash = @row.merge("message" => { "already" => "parsed" })
      msg = PGMQ::Message.new(row_with_hash)

      assert_equal({ "already" => "parsed" }, msg.message)
    end
  end
end
