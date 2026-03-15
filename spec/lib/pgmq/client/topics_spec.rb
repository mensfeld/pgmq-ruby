# frozen_string_literal: true

RSpec.describe PGMQ::Client::Topics, :integration do
  let(:client) { create_test_client }
  let(:queue_name) { test_queue_name("topic") }
  let(:queue2) { test_queue_name("topic2") }

  before do
    skip "PGMQ v1.11.0+ required for topic routing" unless pgmq_supports_topic_routing?
    ensure_test_queue(client, queue_name)
    ensure_test_queue(client, queue2)
  end

  after do
    begin
      client.drop_queue(queue_name)
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

  describe "#bind_topic" do
    it "binds a topic pattern to a queue" do
      client.bind_topic("orders.*", queue_name)

      bindings = client.list_topic_bindings(queue_name: queue_name)
      expect(bindings.size).to eq(1)
      expect(bindings.first[:pattern]).to eq("orders.*")
    end

    it "allows binding multiple patterns to same queue" do
      client.bind_topic("orders.new", queue_name)
      client.bind_topic("orders.update", queue_name)

      bindings = client.list_topic_bindings(queue_name: queue_name)
      expect(bindings.size).to eq(2)
    end

    it "allows binding same pattern to multiple queues" do
      client.bind_topic("orders.*", queue_name)
      client.bind_topic("orders.*", queue2)

      all_bindings = client.list_topic_bindings
      orders_bindings = all_bindings.select { |b| b[:pattern] == "orders.*" }
      expect(orders_bindings.size).to eq(2)
    end
  end

  describe "#unbind_topic" do
    before do
      client.bind_topic("orders.new", queue_name)
    end

    it "unbinds a topic pattern from a queue" do
      result = client.unbind_topic("orders.new", queue_name)

      expect(result).to be true
      bindings = client.list_topic_bindings(queue_name: queue_name)
      expect(bindings).to be_empty
    end

    it "returns false when binding does not exist" do
      result = client.unbind_topic("nonexistent.pattern", queue_name)
      expect(result).to be false
    end
  end

  describe "#produce_topic" do
    before do
      client.bind_topic("orders.*", queue_name)
    end

    it "sends message to matching queues" do
      count = client.produce_topic("orders.new", to_json_msg({ order_id: 123 }))

      expect(count).to eq(1)

      msg = client.read(queue_name, vt: 30)
      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)["order_id"]).to eq(123)
    end

    it "sends message to multiple matching queues" do
      client.bind_topic("orders.*", queue2)

      count = client.produce_topic("orders.new", to_json_msg({ order_id: 456 }))

      expect(count).to eq(2)

      msg1 = client.read(queue_name, vt: 30)
      msg2 = client.read(queue2, vt: 30)
      expect(msg1).not_to be_nil
      expect(msg2).not_to be_nil
    end

    it "returns 0 when no queues match" do
      count = client.produce_topic("unmatched.key", to_json_msg({ data: "test" }))
      expect(count).to eq(0)
    end

    it "supports headers" do
      client.produce_topic("orders.new",
        to_json_msg({ order_id: 789 }),
        headers: to_json_msg({ trace_id: "abc123" }))

      msg = client.read(queue_name, vt: 30)
      expect(msg.headers).not_to be_nil
      expect(JSON.parse(msg.headers)["trace_id"]).to eq("abc123")
    end

    it "supports delay" do
      client.produce_topic("orders.new", to_json_msg({ delayed: true }), delay: 2)

      # Message should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      sleep 2.5
      msg = client.read(queue_name, vt: 30)
      expect(msg).not_to be_nil
    end
  end

  describe "#produce_batch_topic" do
    before do
      client.bind_topic("orders.*", queue_name)
    end

    it "sends multiple messages to matching queues" do
      results = client.produce_batch_topic("orders.new", [
        to_json_msg({ id: 1 }),
        to_json_msg({ id: 2 }),
        to_json_msg({ id: 3 })
      ])

      expect(results.size).to eq(3)
      expect(results).to all(include(:queue_name, :msg_id))
    end

    it "returns empty array when no messages provided" do
      results = client.produce_batch_topic("orders.new", [])
      expect(results).to eq([])
    end

    it "raises error when headers array length doesn't match messages" do
      expect {
        client.produce_batch_topic("orders.new",
          [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })],
          headers: [to_json_msg({ h: 1 })])
      }.to raise_error(ArgumentError, /headers array length/)
    end
  end

  describe "#list_topic_bindings" do
    before do
      client.bind_topic("orders.new", queue_name)
      client.bind_topic("orders.update", queue_name)
      client.bind_topic("users.*", queue2)
    end

    it "lists all bindings" do
      bindings = client.list_topic_bindings

      expect(bindings.size).to be >= 3
      expect(bindings).to all(include(:pattern, :queue_name, :bound_at))
    end

    it "filters by queue name" do
      bindings = client.list_topic_bindings(queue_name: queue_name)

      expect(bindings.size).to eq(2)
      expect(bindings.map { |b| b[:queue_name] }.uniq).to eq([queue_name])
    end
  end

  describe "#test_routing" do
    before do
      client.bind_topic("orders.*", queue_name)
      client.bind_topic("orders.new.#", queue2)
    end

    it "returns matching bindings for routing key" do
      matches = client.test_routing("orders.new")

      expect(matches.size).to be >= 1
      expect(matches).to all(include(:pattern, :queue_name))
    end

    it "returns empty array when no patterns match" do
      matches = client.test_routing("users.create")
      expect(matches).to be_empty
    end

    it "matches multi-word wildcard patterns" do
      matches = client.test_routing("orders.new.priority.urgent")

      patterns = matches.map { |m| m[:pattern] }
      expect(patterns).to include("orders.new.#")
    end
  end

  describe "#validate_routing_key" do
    it "returns true for valid routing keys" do
      expect(client.validate_routing_key("orders.new")).to be true
      expect(client.validate_routing_key("orders.new.priority")).to be true
    end

    it "returns false for routing keys with wildcards" do
      expect(client.validate_routing_key("orders.*")).to be false
      expect(client.validate_routing_key("orders.#")).to be false
    end
  end

  describe "#validate_topic_pattern" do
    it "returns true for valid patterns" do
      expect(client.validate_topic_pattern("orders.new")).to be true
      expect(client.validate_topic_pattern("orders.*")).to be true
      expect(client.validate_topic_pattern("orders.#")).to be true
      expect(client.validate_topic_pattern("#.important")).to be true
    end
  end
end
