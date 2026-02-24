# frozen_string_literal: true

RSpec.describe PGMQ::Client::Producer, :integration do
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

  describe "#send" do
    it "sends a message to the queue" do
      message_data = { order_id: 123, status: "pending" }
      msg_id = client.produce(queue_name, to_json_msg(message_data))

      expect(msg_id).to be_a(String)
      expect(msg_id.to_i).to be > 0

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_a(PGMQ::Message)
      expect(msg.msg_id).to eq(msg_id)
      expect(JSON.parse(msg.message)).to eq({ "order_id" => 123, "status" => "pending" })
    end

    it "sends message with delay" do
      msg_id = client.produce(queue_name, to_json_msg({ test: "data" }), delay: 2)
      expect(msg_id).to be_a(String)

      # Message should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      # Wait for delay
      sleep 2.5

      # Now message should be visible
      msg = client.read(queue_name, vt: 30)
      expect(msg).not_to be_nil
      expect(JSON.parse(msg.message)).to eq({ "test" => "data" })
    end

    context "with headers" do
      it "sends a message with headers" do
        message = to_json_msg({ order_id: 456 })
        headers = to_json_msg({ trace_id: "abc123", priority: "high" })

        msg_id = client.produce(queue_name, message, headers: headers)
        expect(msg_id).to be_a(String)

        msg = client.read(queue_name, vt: 30)
        expect(msg).to be_a(PGMQ::Message)
        expect(msg.msg_id).to eq(msg_id)
        expect(JSON.parse(msg.message)).to eq({ "order_id" => 456 })
        expect(JSON.parse(msg.headers)).to eq({ "trace_id" => "abc123", "priority" => "high" })
      end

      it "sends a message with headers and delay" do
        message = to_json_msg({ order_id: 789 })
        headers = to_json_msg({ correlation_id: "req-001" })

        msg_id = client.produce(queue_name, message, headers: headers, delay: 2)
        expect(msg_id).to be_a(String)

        # Message should not be visible immediately
        msg = client.read(queue_name, vt: 30)
        expect(msg).to be_nil

        # Wait for delay
        sleep 2.5

        # Now message should be visible with headers
        msg = client.read(queue_name, vt: 30)
        expect(msg).not_to be_nil
        expect(JSON.parse(msg.message)).to eq({ "order_id" => 789 })
        expect(JSON.parse(msg.headers)).to eq({ "correlation_id" => "req-001" })
      end

      it "sends a message with complex nested headers" do
        message = to_json_msg({ data: "test" })
        headers = to_json_msg({
          trace: { id: "trace-123", span_id: "span-456" },
          routing: { region: "us-east", priority: 1 },
          content_type: "application/json"
        })

        client.produce(queue_name, message, headers: headers)
        msg = client.read(queue_name, vt: 30)

        parsed_headers = JSON.parse(msg.headers)
        expect(parsed_headers["trace"]["id"]).to eq("trace-123")
        expect(parsed_headers["routing"]["region"]).to eq("us-east")
        expect(parsed_headers["content_type"]).to eq("application/json")
      end

      it "sends a message with empty headers object" do
        message = to_json_msg({ data: "test" })
        headers = "{}"

        client.produce(queue_name, message, headers: headers)
        msg = client.read(queue_name, vt: 30)

        expect(msg.headers).to eq("{}")
      end

      it "returns nil headers when sent without headers" do
        message = to_json_msg({ data: "no headers" })

        client.produce(queue_name, message)
        msg = client.read(queue_name, vt: 30)

        expect(msg.headers).to be_nil
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

      msg_ids = client.produce_batch(queue_name, messages)
      expect(msg_ids).to be_an(Array)
      expect(msg_ids.size).to eq(3)

      read_messages = client.read_batch(queue_name, vt: 30, qty: 3)
      expect(read_messages.size).to eq(3)
      parsed_data = read_messages.map { |m| JSON.parse(m.message)["data"] }
      expect(parsed_data).to contain_exactly("first", "second", "third")
    end

    it "handles empty batch" do
      msg_ids = client.produce_batch(queue_name, [])
      expect(msg_ids).to eq([])
    end

    it "sends batch with delay" do
      messages = [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })]

      client.produce_batch(queue_name, messages, delay: 2)

      # Messages should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      # Wait for delay
      sleep 2.5

      # Now messages should be visible
      read_messages = client.read_batch(queue_name, vt: 30, qty: 2)
      expect(read_messages.size).to eq(2)
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

        msg_ids = client.produce_batch(queue_name, messages, headers: headers)
        expect(msg_ids.size).to eq(3)

        read_messages = client.read_batch(queue_name, vt: 30, qty: 3)
        expect(read_messages.size).to eq(3)

        # Verify each message has its corresponding headers
        read_messages.each do |msg|
          parsed_msg = JSON.parse(msg.message)
          parsed_headers = JSON.parse(msg.headers)

          case parsed_msg["order_id"]
          when 1
            expect(parsed_headers["priority"]).to eq("high")
            expect(parsed_headers["trace_id"]).to eq("trace-1")
          when 2
            expect(parsed_headers["priority"]).to eq("medium")
            expect(parsed_headers["trace_id"]).to eq("trace-2")
          when 3
            expect(parsed_headers["priority"]).to eq("low")
            expect(parsed_headers["trace_id"]).to eq("trace-3")
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

        client.produce_batch(queue_name, messages, headers: headers, delay: 2)

        # Messages should not be visible immediately
        msg = client.read(queue_name, vt: 30)
        expect(msg).to be_nil

        # Wait for delay
        sleep 2.5

        # Now messages should be visible with headers
        read_messages = client.read_batch(queue_name, vt: 30, qty: 2)
        expect(read_messages.size).to eq(2)
        read_messages.each do |msg|
          expect(msg.headers).not_to be_nil
          parsed_headers = JSON.parse(msg.headers)
          expect(parsed_headers["correlation_id"]).to match(/corr-\d/)
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

        expect do
          client.produce_batch(queue_name, messages, headers: headers)
        end.to raise_error(ArgumentError, /headers array length \(2\) must match messages array length \(3\)/)
      end

      it "raises ArgumentError when headers is shorter than messages" do
        messages = [to_json_msg({ id: 1 }), to_json_msg({ id: 2 })]
        headers = [to_json_msg({ h: 1 })]

        expect do
          client.produce_batch(queue_name, messages, headers: headers)
        end.to raise_error(ArgumentError, /headers array length \(1\) must match messages array length \(2\)/)
      end

      it "raises ArgumentError when headers is longer than messages" do
        messages = [to_json_msg({ id: 1 })]
        headers = [to_json_msg({ h: 1 }), to_json_msg({ h: 2 }), to_json_msg({ h: 3 })]

        expect do
          client.produce_batch(queue_name, messages, headers: headers)
        end.to raise_error(ArgumentError, /headers array length \(3\) must match messages array length \(1\)/)
      end

      it "sends batch without headers (backward compatibility)" do
        messages = [
          to_json_msg({ id: 1 }),
          to_json_msg({ id: 2 })
        ]

        msg_ids = client.produce_batch(queue_name, messages)
        expect(msg_ids.size).to eq(2)

        read_messages = client.read_batch(queue_name, vt: 30, qty: 2)
        expect(read_messages.size).to eq(2)
        read_messages.each do |msg|
          expect(msg.headers).to be_nil
        end
      end

      it "handles empty headers array with empty messages array" do
        msg_ids = client.produce_batch(queue_name, [], headers: [])
        expect(msg_ids).to eq([])
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

        client.produce_batch(queue_name, messages, headers: headers)
        read_messages = client.read_batch(queue_name, vt: 30, qty: 2)

        read_messages.each do |msg|
          parsed_headers = JSON.parse(msg.headers)
          expect(parsed_headers["metadata"]["version"]).to eq("1.0")
          expect(parsed_headers["tracing"]).to be_a(Hash)
        end
      end
    end
  end
end
