# frozen_string_literal: true

describe PGMQ::Client::Producer do
  before { setup_client_and_queue }
  after { teardown_client_and_queue }

  describe "#send" do
    it "sends a message to the queue" do
      message_data = { order_id: 123, status: "pending" }
      msg_id = @client.produce(@queue_name, to_json_msg(message_data))

      assert_kind_of String, msg_id
      assert_operator msg_id.to_i, :>, 0

      msg = @client.read(@queue_name, vt: 30)

      assert_kind_of PGMQ::Message, msg
      assert_equal msg_id, msg.msg_id
      assert_equal({ "order_id" => 123, "status" => "pending" }, JSON.parse(msg.message))
    end

    it "sends message with delay" do
      msg_id = @client.produce(@queue_name, to_json_msg({ test: "data" }), delay: 2)

      assert_kind_of String, msg_id

      # Message should not be visible immediately
      msg = @client.read(@queue_name, vt: 30)

      assert_nil msg

      # Wait for delay
      sleep 2.5

      # Now message should be visible
      msg = @client.read(@queue_name, vt: 30)

      refute_nil msg
      assert_equal({ "test" => "data" }, JSON.parse(msg.message))
    end

    context "with headers" do
      it "sends a message with headers" do
        message = to_json_msg({ order_id: 456 })
        headers = to_json_msg({ trace_id: "abc123", priority: "high" })

        msg_id = @client.produce(@queue_name, message, headers: headers)

        assert_kind_of String, msg_id

        msg = @client.read(@queue_name, vt: 30)

        assert_kind_of PGMQ::Message, msg
        assert_equal msg_id, msg.msg_id
        assert_equal({ "order_id" => 456 }, JSON.parse(msg.message))
        assert_equal({ "trace_id" => "abc123", "priority" => "high" }, JSON.parse(msg.headers))
      end

      it "sends a message with headers and delay" do
        message = to_json_msg({ order_id: 789 })
        headers = to_json_msg({ correlation_id: "req-001" })

        msg_id = @client.produce(@queue_name, message, headers: headers, delay: 2)

        assert_kind_of String, msg_id

        # Message should not be visible immediately
        msg = @client.read(@queue_name, vt: 30)

        assert_nil msg

        # Wait for delay
        sleep 2.5

        # Now message should be visible with headers
        msg = @client.read(@queue_name, vt: 30)

        refute_nil msg
        assert_equal({ "order_id" => 789 }, JSON.parse(msg.message))
        assert_equal({ "correlation_id" => "req-001" }, JSON.parse(msg.headers))
      end

      it "sends a message with complex nested headers" do
        message = to_json_msg({ data: "test" })
        headers = to_json_msg({
          trace: { id: "trace-123", span_id: "span-456" },
          routing: { region: "us-east", priority: 1 },
          content_type: "application/json"
        })

        @client.produce(@queue_name, message, headers: headers)
        msg = @client.read(@queue_name, vt: 30)

        parsed_headers = JSON.parse(msg.headers)

        assert_equal "trace-123", parsed_headers["trace"]["id"]
        assert_equal "us-east", parsed_headers["routing"]["region"]
        assert_equal "application/json", parsed_headers["content_type"]
      end

      it "sends a message with empty headers object" do
        message = to_json_msg({ data: "test" })
        headers = "{}"

        @client.produce(@queue_name, message, headers: headers)
        msg = @client.read(@queue_name, vt: 30)

        assert_equal "{}", msg.headers
      end

      it "returns nil headers when sent without headers" do
        message = to_json_msg({ data: "no headers" })

        @client.produce(@queue_name, message)
        msg = @client.read(@queue_name, vt: 30)

        assert_nil msg.headers
      end
    end
  end

  describe "#send_batch" do
    it "sends multiple messages to the queue" do
      messages = [
        to_json_msg({ id: 1, data: "first" }),
        to_json_msg({ id: 2, data: "second" }),
        to_json_msg({ id: 3, data: "third" })
      ]

      msg_ids = @client.produce_batch(@queue_name, messages)

      assert_kind_of Array, msg_ids
      assert_equal 3, msg_ids.size

      read_messages = @client.read_batch(@queue_name, vt: 30, qty: 3)

      assert_equal 3, read_messages.size
      parsed_data = read_messages.map { |m| JSON.parse(m.message)["data"] }

      assert_equal %w[first second third].sort, parsed_data.sort
    end

    it "handles empty batch" do
      msg_ids = @client.produce_batch(@queue_name, [])

      assert_equal [], msg_ids
    end

    it "sends batch with delay" do
      messages = [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })]

      @client.produce_batch(@queue_name, messages, delay: 2)

      # Messages should not be visible immediately
      msg = @client.read(@queue_name, vt: 30)

      assert_nil msg

      # Wait for delay
      sleep 2.5

      # Now messages should be visible
      read_messages = @client.read_batch(@queue_name, vt: 30, qty: 2)

      assert_equal 2, read_messages.size
    end

    context "with headers" do
      it "sends batch with headers" do
        messages = [
          to_json_msg({ order_id: 1 }),
          to_json_msg({ order_id: 2 }),
          to_json_msg({ order_id: 3 })
        ]
        headers = [
          to_json_msg({ priority: "high", trace_id: "trace-1" }),
          to_json_msg({ priority: "medium", trace_id: "trace-2" }),
          to_json_msg({ priority: "low", trace_id: "trace-3" })
        ]

        msg_ids = @client.produce_batch(@queue_name, messages, headers: headers)

        assert_equal 3, msg_ids.size

        read_messages = @client.read_batch(@queue_name, vt: 30, qty: 3)

        assert_equal 3, read_messages.size

        # Verify each message has its corresponding headers
        read_messages.each do |msg|
          parsed_msg = JSON.parse(msg.message)
          parsed_headers = JSON.parse(msg.headers)

          case parsed_msg["order_id"]
          when 1
            assert_equal "high", parsed_headers["priority"]
            assert_equal "trace-1", parsed_headers["trace_id"]
          when 2
            assert_equal "medium", parsed_headers["priority"]
            assert_equal "trace-2", parsed_headers["trace_id"]
          when 3
            assert_equal "low", parsed_headers["priority"]
            assert_equal "trace-3", parsed_headers["trace_id"]
          end
        end
      end

      it "sends batch with headers and delay" do
        messages = [
          to_json_msg({ id: 1 }),
          to_json_msg({ id: 2 })
        ]
        headers = [
          to_json_msg({ correlation_id: "corr-1" }),
          to_json_msg({ correlation_id: "corr-2" })
        ]

        @client.produce_batch(@queue_name, messages, headers: headers, delay: 2)

        # Messages should not be visible immediately
        msg = @client.read(@queue_name, vt: 30)

        assert_nil msg

        # Wait for delay
        sleep 2.5

        # Now messages should be visible with headers
        read_messages = @client.read_batch(@queue_name, vt: 30, qty: 2)

        assert_equal 2, read_messages.size
        read_messages.each do |msg|
          refute_nil msg.headers
          parsed_headers = JSON.parse(msg.headers)

          assert_match(/corr-\d/, parsed_headers["correlation_id"])
        end
      end

      it "raises ArgumentError when headers length does not match messages length" do
        messages = [
          to_json_msg({ id: 1 }),
          to_json_msg({ id: 2 }),
          to_json_msg({ id: 3 })
        ]
        headers = [
          to_json_msg({ priority: "high" }),
          to_json_msg({ priority: "low" })
        ]

        e = assert_raises(ArgumentError) do
          @client.produce_batch(@queue_name, messages, headers: headers)
        end
        assert_match(/headers array length \(2\) must match messages array length \(3\)/, e.message)
      end

      it "raises ArgumentError when headers is shorter than messages" do
        messages = [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })]
        headers = [to_json_msg({ h: 1 })]

        e = assert_raises(ArgumentError) do
          @client.produce_batch(@queue_name, messages, headers: headers)
        end
        assert_match(/headers array length \(1\) must match messages array length \(2\)/, e.message)
      end

      it "raises ArgumentError when headers is longer than messages" do
        messages = [to_json_msg({ id: 1 })]
        headers = [to_json_msg({ h: 1 }), to_json_msg({ h: 2 }), to_json_msg({ h: 3 })]

        e = assert_raises(ArgumentError) do
          @client.produce_batch(@queue_name, messages, headers: headers)
        end
        assert_match(/headers array length \(3\) must match messages array length \(1\)/, e.message)
      end

      it "sends batch without headers (backward compatibility)" do
        messages = [
          to_json_msg({ id: 1 }),
          to_json_msg({ id: 2 })
        ]

        msg_ids = @client.produce_batch(@queue_name, messages)

        assert_equal 2, msg_ids.size

        read_messages = @client.read_batch(@queue_name, vt: 30, qty: 2)

        assert_equal 2, read_messages.size
        read_messages.each do |msg|
          assert_nil msg.headers
        end
      end

      it "handles empty headers array with empty messages array" do
        msg_ids = @client.produce_batch(@queue_name, [], headers: [])

        assert_equal [], msg_ids
      end

      it "sends batch with complex nested headers" do
        messages = [
          to_json_msg({ event: "user.created" }),
          to_json_msg({ event: "order.placed" })
        ]
        headers = [
          to_json_msg({
            metadata: { version: "1.0", source: "api" },
            tracing: { trace_id: "t1", span_id: "s1" }
          }),
          to_json_msg({
            metadata: { version: "1.0", source: "web" },
            tracing: { trace_id: "t2", span_id: "s2" }
          })
        ]

        @client.produce_batch(@queue_name, messages, headers: headers)
        read_messages = @client.read_batch(@queue_name, vt: 30, qty: 2)

        read_messages.each do |msg|
          parsed_headers = JSON.parse(msg.headers)

          assert_equal "1.0", parsed_headers["metadata"]["version"]
          assert_kind_of Hash, parsed_headers["tracing"]
        end
      end
    end
  end
end
