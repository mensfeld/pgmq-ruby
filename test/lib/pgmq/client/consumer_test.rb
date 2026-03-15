# frozen_string_literal: true

describe PGMQ::Client::Consumer do
  before { setup_client_and_queue }
  after { teardown_client_and_queue }

  describe "#read" do
    it "reads a message from the queue" do
      message_data = { order_id: 123, status: "pending" }
      msg_id = @client.produce(@queue_name, to_json_msg(message_data))

      msg = @client.read(@queue_name, vt: 30)

      assert_kind_of PGMQ::Message, msg
      assert_equal msg_id, msg.msg_id
      assert_equal({ "order_id" => 123, "status" => "pending" }, JSON.parse(msg.message))
    end

    it "returns last_read_at as nil or timestamp depending on PGMQ version" do
      # The last_read_at field was added in PGMQ v1.8.1
      # For older versions it will be nil, for newer versions it will be a timestamp
      @client.produce(@queue_name, to_json_msg({ test: 1 }))

      msg = @client.read(@queue_name, vt: 30)

      assert_respond_to msg, :last_read_at
      # last_read_at can be nil (older PGMQ) or a timestamp string (PGMQ v1.8.1+)
      assert(msg.last_read_at.nil? || msg.last_read_at.is_a?(String))
    end

    it "returns nil when queue is empty" do
      msg = @client.read(@queue_name, vt: 30)

      assert_nil msg
    end

    it "handles conditional JSONB filtering by single key-value" do
      @client.produce(@queue_name, to_json_msg({ status: "pending", priority: "high" }))
      @client.produce(@queue_name, to_json_msg({ status: "completed", priority: "low" }))

      msg = @client.read(@queue_name, vt: 30, conditional: { status: "pending" })

      refute_nil msg
      assert_equal "pending", JSON.parse(msg.message)["status"]
    end

    it "handles conditional filtering with multiple conditions (AND logic)" do
      @client.produce(@queue_name, to_json_msg({ status: "pending", priority: "high" }))
      @client.produce(@queue_name, to_json_msg({ status: "pending", priority: "low" }))
      @client.produce(@queue_name, to_json_msg({ status: "completed", priority: "high" }))

      msg = @client.read(@queue_name, vt: 30, conditional: { status: "pending", priority: "high" })

      refute_nil msg
      assert_equal "pending", JSON.parse(msg.message)["status"]
      assert_equal "high", JSON.parse(msg.message)["priority"]
    end

    it "handles conditional filtering with nested objects" do
      @client.produce(@queue_name, to_json_msg({ user: { role: "admin", active: true } }))
      @client.produce(@queue_name, to_json_msg({ user: { role: "user", active: true } }))

      msg = @client.read(@queue_name, vt: 30, conditional: { user: { role: "admin" } })

      refute_nil msg
      assert_equal "admin", JSON.parse(msg.message)["user"]["role"]
    end

    it "returns nil when no messages match condition" do
      @client.produce(@queue_name, to_json_msg({ status: "pending" }))

      msg = @client.read(@queue_name, vt: 30, conditional: { status: "completed" })

      assert_nil msg
    end

    it "preserves visibility timeout with filtering" do
      @client.produce(@queue_name, to_json_msg({ status: "pending" }))

      msg1 = @client.read(@queue_name, vt: 2, conditional: { status: "pending" })

      refute_nil msg1

      msg2 = @client.read(@queue_name, vt: 2, conditional: { status: "pending" })

      assert_nil msg2

      sleep 2.5
      msg3 = @client.read(@queue_name, vt: 30, conditional: { status: "pending" })

      refute_nil msg3
    end
  end

  describe "#read_batch" do
    it "reads multiple messages from the queue" do
      messages = [
        to_json_msg({ id: 1, data: "first" }),
        to_json_msg({ id: 2, data: "second" }),
        to_json_msg({ id: 3, data: "third" })
      ]

      @client.produce_batch(@queue_name, messages)

      read_messages = @client.read_batch(@queue_name, vt: 30, qty: 3)

      assert_equal 3, read_messages.size
      parsed_data = read_messages.map { |m| JSON.parse(m.message)["data"] }

      assert_equal %w[first second third].sort, parsed_data.sort
    end

    it "returns only matching messages up to qty limit with conditional" do
      @client.produce(@queue_name, to_json_msg({ priority: "high", id: 1 }))
      @client.produce(@queue_name, to_json_msg({ priority: "low", id: 2 }))
      @client.produce(@queue_name, to_json_msg({ priority: "high", id: 3 }))
      @client.produce(@queue_name, to_json_msg({ priority: "high", id: 4 }))

      messages = @client.read_batch(@queue_name, vt: 30, qty: 2, conditional: { priority: "high" })

      assert_equal 2, messages.length
      messages.each do |msg|
        assert_equal "high", JSON.parse(msg.message)["priority"]
      end
    end

    it "returns empty array when no matches with conditional" do
      @client.produce(@queue_name, to_json_msg({ priority: "low" }))

      messages = @client.read_batch(@queue_name, vt: 30, qty: 10, conditional: { priority: "high" })

      assert_empty messages
    end
  end

  describe "#read_with_poll" do
    it "waits for messages with long-polling" do
      Thread.new do
        sleep 1
        @client.produce(@queue_name, to_json_msg({ delayed: "message" }))
      end

      start_time = Time.now
      messages = @client.read_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 3,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      refute_empty messages
      assert_operator elapsed, :>=, 1
      assert_equal({ "delayed" => "message" }, JSON.parse(messages.first.message))
    end

    it "returns empty array if no messages within timeout" do
      start_time = Time.now
      messages = @client.read_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      assert_empty messages
      assert_operator elapsed, :>=, 1
    end

    it "polls until matching message arrives with conditional" do
      Thread.new do
        sleep 0.5
        @client.produce(@queue_name, to_json_msg({ type: "urgent", data: "test" }))
      end

      start_time = Time.now
      messages = @client.read_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 2,
        poll_interval_ms: 100,
        conditional: { type: "urgent" }
      )
      elapsed = Time.now - start_time

      refute_empty messages
      assert_equal "urgent", JSON.parse(messages.first.message)["type"]
      assert_operator elapsed, :>=, 0.5
      assert_operator elapsed, :<=, 2.0
    end

    it "times out if no matching messages with conditional" do
      @client.produce(@queue_name, to_json_msg({ type: "normal" }))

      start_time = Time.now
      messages = @client.read_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100,
        conditional: { type: "urgent" }
      )
      elapsed = Time.now - start_time

      assert_empty messages
      assert_operator elapsed, :>=, 1
    end
  end

  describe "visibility timeout behavior" do
    it "makes message invisible during visibility timeout" do
      @client.produce(@queue_name, to_json_msg({ test: "vt" }))

      msg1 = @client.read(@queue_name, vt: 3)

      refute_nil msg1

      msg2 = @client.read(@queue_name, vt: 3)

      assert_nil msg2

      sleep 3.5

      msg3 = @client.read(@queue_name, vt: 3)

      refute_nil msg3
      assert_equal msg1.msg_id, msg3.msg_id
      assert_equal "2", msg3.read_ct
    end
  end

  describe "#read_grouped_rr" do
    it "reads messages in round-robin order across groups" do
      # Send messages from different "users" (grouped by first key value)
      @client.produce(@queue_name, to_json_msg({ user_id: "user1", seq: 1 }))
      @client.produce(@queue_name, to_json_msg({ user_id: "user1", seq: 2 }))
      @client.produce(@queue_name, to_json_msg({ user_id: "user2", seq: 1 }))
      @client.produce(@queue_name, to_json_msg({ user_id: "user3", seq: 1 }))

      messages = @client.read_grouped_rr(@queue_name, vt: 30, qty: 4)

      assert_equal 4, messages.size
      messages.each { |m| assert_kind_of PGMQ::Message, m }

      # All users should be represented in the results
      user_ids = messages.map { |m| JSON.parse(m.message)["user_id"] }

      assert_includes user_ids, "user1"
      assert_includes user_ids, "user2"
      assert_includes user_ids, "user3"
    end

    it "returns empty array for empty queue" do
      messages = @client.read_grouped_rr(@queue_name, vt: 30, qty: 5)

      assert_equal [], messages
    end

    it "respects visibility timeout" do
      @client.produce(@queue_name, to_json_msg({ user_id: "user1", data: "test" }))

      messages1 = @client.read_grouped_rr(@queue_name, vt: 2, qty: 1)

      assert_equal 1, messages1.size

      # Should not be visible immediately
      messages2 = @client.read_grouped_rr(@queue_name, vt: 2, qty: 1)

      assert_empty messages2

      sleep 2.5

      # Should be visible again
      messages3 = @client.read_grouped_rr(@queue_name, vt: 30, qty: 1)

      assert_equal 1, messages3.size
    end
  end

  describe "#read_grouped_rr_with_poll" do
    it "waits for messages with long-polling in round-robin order" do
      Thread.new do
        sleep 0.5
        @client.produce(@queue_name, to_json_msg({ user_id: "user1", delayed: true }))
      end

      start_time = Time.now
      messages = @client.read_grouped_rr_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 2,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      refute_empty messages
      assert_operator elapsed, :>=, 0.5
      assert_operator elapsed, :<, 2.0
    end

    it "returns empty array if no messages within timeout" do
      start_time = Time.now
      messages = @client.read_grouped_rr_with_poll(
        @queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      assert_empty messages
      assert_operator elapsed, :>=, 1
    end

    it "provides fair ordering with multiple groups" do
      # Pre-populate queue
      @client.produce(@queue_name, to_json_msg({ user_id: "userA", n: 1 }))
      @client.produce(@queue_name, to_json_msg({ user_id: "userA", n: 2 }))
      @client.produce(@queue_name, to_json_msg({ user_id: "userB", n: 1 }))

      messages = @client.read_grouped_rr_with_poll(
        @queue_name,
        vt: 30,
        qty: 3,
        max_poll_seconds: 1,
        poll_interval_ms: 50
      )

      assert_equal 3, messages.size
      user_ids = messages.map { |m| JSON.parse(m.message)["user_id"] }
      # Both users should be represented in the results
      assert_includes user_ids, "userA"
      assert_includes user_ids, "userB"
    end
  end
end
