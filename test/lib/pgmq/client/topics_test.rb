# frozen_string_literal: true

require "test_helper"

describe PGMQ::Client::Topics do
  before do
    @client = create_test_client
    @queue_name = unique_queue_name("topic")
    @queue2 = unique_queue_name("topic2")
    skip "PGMQ v1.11.0+ required for topic routing" unless pgmq_supports_topic_routing?
    ensure_test_queue(@client, @queue_name)
    ensure_test_queue(@client, @queue2)
  end

  after do
    begin
      @client.drop_queue(@queue_name)
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

  describe "#bind_topic" do
    it "binds a topic pattern to a queue" do
      @client.bind_topic("orders.*", @queue_name)

      bindings = @client.list_topic_bindings(queue_name: @queue_name)

      assert_equal 1, bindings.size
      assert_equal "orders.*", bindings.first[:pattern]
    end

    it "allows binding multiple patterns to same queue" do
      @client.bind_topic("orders.new", @queue_name)
      @client.bind_topic("orders.update", @queue_name)

      bindings = @client.list_topic_bindings(queue_name: @queue_name)

      assert_equal 2, bindings.size
    end

    it "allows binding same pattern to multiple queues" do
      @client.bind_topic("orders.*", @queue_name)
      @client.bind_topic("orders.*", @queue2)

      all_bindings = @client.list_topic_bindings
      orders_bindings = all_bindings.select { |b| b[:pattern] == "orders.*" }

      assert_equal 2, orders_bindings.size
    end
  end

  describe "#unbind_topic" do
    before do
      @client.bind_topic("orders.new", @queue_name)
    end

    it "unbinds a topic pattern from a queue" do
      result = @client.unbind_topic("orders.new", @queue_name)

      assert result
      bindings = @client.list_topic_bindings(queue_name: @queue_name)

      assert_empty bindings
    end

    it "returns false when binding does not exist" do
      result = @client.unbind_topic("nonexistent.pattern", @queue_name)

      refute result
    end
  end

  describe "#produce_topic" do
    before do
      @client.bind_topic("orders.*", @queue_name)
    end

    it "sends message to matching queues" do
      count = @client.produce_topic("orders.new", to_json_msg({ order_id: 123 }))

      assert_equal 1, count

      msg = @client.read(@queue_name, vt: 30)

      refute_nil msg
      assert_equal 123, JSON.parse(msg.message)["order_id"]
    end

    it "sends message to multiple matching queues" do
      @client.bind_topic("orders.*", @queue2)

      count = @client.produce_topic("orders.new", to_json_msg({ order_id: 456 }))

      assert_equal 2, count

      msg1 = @client.read(@queue_name, vt: 30)
      msg2 = @client.read(@queue2, vt: 30)

      refute_nil msg1
      refute_nil msg2
    end

    it "returns 0 when no queues match" do
      count = @client.produce_topic("unmatched.key", to_json_msg({ data: "test" }))

      assert_equal 0, count
    end

    it "supports headers" do
      @client.produce_topic("orders.new",
        to_json_msg({ order_id: 789 }),
        headers: to_json_msg({ trace_id: "abc123" }))

      msg = @client.read(@queue_name, vt: 30)

      refute_nil msg.headers
      assert_equal "abc123", JSON.parse(msg.headers)["trace_id"]
    end

    it "supports delay" do
      @client.produce_topic("orders.new", to_json_msg({ delayed: true }), delay: 2)

      # Message should not be visible immediately
      msg = @client.read(@queue_name, vt: 30)

      assert_nil msg

      sleep 2.5
      msg = @client.read(@queue_name, vt: 30)

      refute_nil msg
    end
  end

  describe "#produce_batch_topic" do
    before do
      @client.bind_topic("orders.*", @queue_name)
    end

    it "sends multiple messages to matching queues" do
      results = @client.produce_batch_topic("orders.new", [
        to_json_msg({ id: 1 }),
        to_json_msg({ id: 2 }),
        to_json_msg({ id: 3 })
      ])

      assert_equal 3, results.size
      results.each do |r|
        assert_includes r.keys, :queue_name
        assert_includes r.keys, :msg_id
      end
    end

    it "returns empty array when no messages provided" do
      results = @client.produce_batch_topic("orders.new", [])

      assert_equal [], results
    end

    it "raises error when headers array length doesn't match messages" do
      e = assert_raises(ArgumentError) do
        @client.produce_batch_topic("orders.new",
          [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })],
          headers: [to_json_msg({ h: 1 })])
      end
      assert_match(/headers array length/, e.message)
    end
  end

  describe "#list_topic_bindings" do
    before do
      @client.bind_topic("orders.new", @queue_name)
      @client.bind_topic("orders.update", @queue_name)
      @client.bind_topic("users.*", @queue2)
    end

    it "lists all bindings" do
      bindings = @client.list_topic_bindings

      assert_operator bindings.size, :>=, 3
      bindings.each do |b|
        assert_includes b.keys, :pattern
        assert_includes b.keys, :queue_name
        assert_includes b.keys, :bound_at
      end
    end

    it "filters by queue name" do
      bindings = @client.list_topic_bindings(queue_name: @queue_name)

      assert_equal 2, bindings.size
      assert_equal [@queue_name], bindings.map { |b| b[:queue_name] }.uniq
    end
  end

  describe "#test_routing" do
    before do
      @client.bind_topic("orders.*", @queue_name)
      @client.bind_topic("orders.new.#", @queue2)
    end

    it "returns matching bindings for routing key" do
      matches = @client.test_routing("orders.new")

      assert_operator matches.size, :>=, 1
      matches.each do |m|
        assert_includes m.keys, :pattern
        assert_includes m.keys, :queue_name
      end
    end

    it "returns empty array when no patterns match" do
      matches = @client.test_routing("users.create")

      assert_empty matches
    end

    it "matches multi-word wildcard patterns" do
      matches = @client.test_routing("orders.new.priority.urgent")

      patterns = matches.map { |m| m[:pattern] }

      assert_includes patterns, "orders.new.#"
    end
  end

  describe "#validate_routing_key" do
    it "returns true for valid routing keys" do
      assert @client.validate_routing_key("orders.new")
      assert @client.validate_routing_key("orders.new.priority")
    end

    it "returns false for routing keys with wildcards" do
      refute @client.validate_routing_key("orders.*")
      refute @client.validate_routing_key("orders.#")
    end
  end

  describe "#validate_topic_pattern" do
    it "returns true for valid patterns" do
      assert @client.validate_topic_pattern("orders.new")
      assert @client.validate_topic_pattern("orders.*")
      assert @client.validate_topic_pattern("orders.#")
      assert @client.validate_topic_pattern("#.important")
    end
  end
end
