# frozen_string_literal: true

RSpec.describe PGMQ::Client::Consumer, :integration do
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

  describe "#read" do
    it "reads a message from the queue" do
      message_data = { order_id: 123, status: "pending" }
      msg_id = client.produce(queue_name, to_json_msg(message_data))

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_a(PGMQ::Message)
      expect(msg.msg_id).to eq(msg_id)
      expect(JSON.parse(msg.message)).to eq({ "order_id" => 123, "status" => "pending" })
    end

    it "returns last_read_at as nil or timestamp depending on PGMQ version" do
      # The last_read_at field was added in PGMQ v1.8.1
      # For older versions it will be nil, for newer versions it will be a timestamp
      client.produce(queue_name, to_json_msg({ test: 1 }))

      msg = client.read(queue_name, vt: 30)
      expect(msg).to respond_to(:last_read_at)
      # last_read_at can be nil (older PGMQ) or a timestamp string (PGMQ v1.8.1+)
      expect(msg.last_read_at).to be_nil.or(be_a(String))
    end

    it "returns nil when queue is empty" do
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it "handles conditional JSONB filtering by single key-value" do
      client.produce(queue_name, to_json_msg({ status: "pending", priority: "high" }))
      client.produce(queue_name, to_json_msg({ status: "completed", priority: "low" }))

      msg = client.read(queue_name, vt: 30, conditional: { status: "pending" })

      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)["status"]).to eq("pending")
    end

    it "handles conditional filtering with multiple conditions (AND logic)" do
      client.produce(queue_name, to_json_msg({ status: "pending", priority: "high" }))
      client.produce(queue_name, to_json_msg({ status: "pending", priority: "low" }))
      client.produce(queue_name, to_json_msg({ status: "completed", priority: "high" }))

      msg = client.read(queue_name, vt: 30, conditional: { status: "pending", priority: "high" })

      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)["status"]).to eq("pending")
      expect(JSON.parse(msg.message)["priority"]).to eq("high")
    end

    it "handles conditional filtering with nested objects" do
      client.produce(queue_name, to_json_msg({ user: { role: "admin", active: true } }))
      client.produce(queue_name, to_json_msg({ user: { role: "user", active: true } }))

      msg = client.read(queue_name, vt: 30, conditional: { user: { role: "admin" } })

      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)["user"]["role"]).to eq("admin")
    end

    it "returns nil when no messages match condition" do
      client.produce(queue_name, to_json_msg({ status: "pending" }))

      msg = client.read(queue_name, vt: 30, conditional: { status: "completed" })

      expect(msg).to be_nil
    end

    it "preserves visibility timeout with filtering" do
      client.produce(queue_name, to_json_msg({ status: "pending" }))

      msg1 = client.read(queue_name, vt: 2, conditional: { status: "pending" })
      expect(msg1).not_to be_nil

      msg2 = client.read(queue_name, vt: 2, conditional: { status: "pending" })
      expect(msg2).to be_nil

      sleep 2.5
      msg3 = client.read(queue_name, vt: 30, conditional: { status: "pending" })
      expect(msg3).not_to be_nil
    end
  end

  describe "#read_batch" do
    it "reads multiple messages from the queue" do
      messages = [
        to_json_msg({ id: 1, data: "first" }),
        to_json_msg({ id: 2, data: "second" }),
        to_json_msg({ id: 3, data: "third" })
      ]

      client.produce_batch(queue_name, messages)

      read_messages = client.read_batch(queue_name, vt: 30, qty: 3)
      expect(read_messages.size).to eq(3)
      parsed_data = read_messages.map { |m| JSON.parse(m.message)["data"] }
      expect(parsed_data).to contain_exactly("first", "second", "third")
    end

    it "returns only matching messages up to qty limit with conditional" do
      client.produce(queue_name, to_json_msg({ priority: "high", id: 1 }))
      client.produce(queue_name, to_json_msg({ priority: "low", id: 2 }))
      client.produce(queue_name, to_json_msg({ priority: "high", id: 3 }))
      client.produce(queue_name, to_json_msg({ priority: "high", id: 4 }))

      messages = client.read_batch(queue_name, vt: 30, qty: 2, conditional: { priority: "high" })

      expect(messages.length).to eq(2)
      messages.each do |msg|
        expect(JSON.parse(msg.message)["priority"]).to eq("high")
      end
    end

    it "returns empty array when no matches with conditional" do
      client.produce(queue_name, to_json_msg({ priority: "low" }))

      messages = client.read_batch(queue_name, vt: 30, qty: 10, conditional: { priority: "high" })

      expect(messages).to be_empty
    end
  end

  describe "#read_with_poll" do
    it "waits for messages with long-polling" do
      Thread.new do
        sleep 1
        client.produce(queue_name, to_json_msg({ delayed: "message" }))
      end

      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 3,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).not_to be_empty
      expect(elapsed).to be >= 1
      expect(JSON.parse(messages.first.message)).to eq({ "delayed" => "message" })
    end

    it "returns empty array if no messages within timeout" do
      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).to be_empty
      expect(elapsed).to be >= 1
    end

    it "polls until matching message arrives with conditional" do
      Thread.new do
        sleep 0.5
        client.produce(queue_name, to_json_msg({ type: "urgent", data: "test" }))
      end

      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 2,
        poll_interval_ms: 100,
        conditional: { type: "urgent" }
      )
      elapsed = Time.now - start_time

      expect(messages).not_to be_empty
      expect(JSON.parse(messages.first.message)["type"]).to eq("urgent")
      expect(elapsed).to be_between(0.5, 2.0)
    end

    it "times out if no matching messages with conditional" do
      client.produce(queue_name, to_json_msg({ type: "normal" }))

      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100,
        conditional: { type: "urgent" }
      )
      elapsed = Time.now - start_time

      expect(messages).to be_empty
      expect(elapsed).to be >= 1
    end
  end

  describe "visibility timeout behavior" do
    it "makes message invisible during visibility timeout" do
      client.produce(queue_name, to_json_msg({ test: "vt" }))

      msg1 = client.read(queue_name, vt: 3)
      expect(msg1).not_to be_nil

      msg2 = client.read(queue_name, vt: 3)
      expect(msg2).to be_nil

      sleep 3.5

      msg3 = client.read(queue_name, vt: 3)
      expect(msg3).not_to be_nil
      expect(msg3.msg_id).to eq(msg1.msg_id)
      expect(msg3.read_ct).to eq("2")
    end
  end

  describe "#read_grouped_rr" do
    it "reads messages in round-robin order across groups" do
      # Send messages from different "users" (grouped by first key value)
      client.produce(queue_name, to_json_msg({ user_id: "user1", seq: 1 }))
      client.produce(queue_name, to_json_msg({ user_id: "user1", seq: 2 }))
      client.produce(queue_name, to_json_msg({ user_id: "user2", seq: 1 }))
      client.produce(queue_name, to_json_msg({ user_id: "user3", seq: 1 }))

      messages = client.read_grouped_rr(queue_name, vt: 30, qty: 4)

      expect(messages.size).to eq(4)
      expect(messages).to all(be_a(PGMQ::Message))

      # All users should be represented in the results
      user_ids = messages.map { |m| JSON.parse(m.message)["user_id"] }
      expect(user_ids).to include("user1", "user2", "user3")
    end

    it "returns empty array for empty queue" do
      messages = client.read_grouped_rr(queue_name, vt: 30, qty: 5)
      expect(messages).to eq([])
    end

    it "respects visibility timeout" do
      client.produce(queue_name, to_json_msg({ user_id: "user1", data: "test" }))

      messages1 = client.read_grouped_rr(queue_name, vt: 2, qty: 1)
      expect(messages1.size).to eq(1)

      # Should not be visible immediately
      messages2 = client.read_grouped_rr(queue_name, vt: 2, qty: 1)
      expect(messages2).to be_empty

      sleep 2.5

      # Should be visible again
      messages3 = client.read_grouped_rr(queue_name, vt: 30, qty: 1)
      expect(messages3.size).to eq(1)
    end
  end

  describe "#read_grouped_rr_with_poll" do
    it "waits for messages with long-polling in round-robin order" do
      Thread.new do
        sleep 0.5
        client.produce(queue_name, to_json_msg({ user_id: "user1", delayed: true }))
      end

      start_time = Time.now
      messages = client.read_grouped_rr_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 2,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).not_to be_empty
      expect(elapsed).to be >= 0.5
      expect(elapsed).to be < 2.0
    end

    it "returns empty array if no messages within timeout" do
      start_time = Time.now
      messages = client.read_grouped_rr_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).to be_empty
      expect(elapsed).to be >= 1
    end

    it "provides fair ordering with multiple groups" do
      # Pre-populate queue
      client.produce(queue_name, to_json_msg({ user_id: "userA", n: 1 }))
      client.produce(queue_name, to_json_msg({ user_id: "userA", n: 2 }))
      client.produce(queue_name, to_json_msg({ user_id: "userB", n: 1 }))

      messages = client.read_grouped_rr_with_poll(
        queue_name,
        vt: 30,
        qty: 3,
        max_poll_seconds: 1,
        poll_interval_ms: 50
      )

      expect(messages.size).to eq(3)
      user_ids = messages.map { |m| JSON.parse(m.message)["user_id"] }
      # Both users should be represented in the results
      expect(user_ids).to include("userA", "userB")
    end
  end
end
