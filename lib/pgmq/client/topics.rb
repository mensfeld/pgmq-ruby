# frozen_string_literal: true

module PGMQ
  class Client
    # Topic routing operations (AMQP-like patterns)
    #
    # This module provides AMQP-style topic routing for PGMQ, allowing messages to be routed to multiple queues based on
    # pattern matching.
    #
    # Topic patterns support wildcards:
    # - `*` matches exactly one word between dots (e.g., `orders.*` matches `orders.new`)
    # - `#` matches zero or more words (e.g., `orders.#` matches `orders.new.urgent`)
    #
    # @note Requires PGMQ v1.11.0+
    module Topics
      # Binds a topic pattern to a queue
      #
      # Messages sent with routing keys matching this pattern will be delivered to the specified queue.
      #
      # @param pattern [String] topic pattern with optional wildcards (* or #)
      # @param queue_name [String] name of the queue to bind
      # @return [void]
      #
      # @example Bind exact routing key
      #   client.bind_topic("orders.new", "new_orders")
      #
      # @example Bind with single-word wildcard
      #   client.bind_topic("orders.*", "all_order_events")
      #
      # @example Bind with multi-word wildcard
      #   client.bind_topic("orders.#", "order_audit_log")
      def bind_topic(pattern, queue_name)
        validate_queue_name!(queue_name)

        with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.bind_topic($1::text, $2::text)",
            [pattern, queue_name]
          )
        end

        nil
      end

      # Unbinds a topic pattern from a queue
      #
      # @param pattern [String] topic pattern to unbind
      # @param queue_name [String] name of the queue to unbind from
      # @return [Boolean] true if the binding was removed, false if it didn't exist
      #
      # @example
      #   client.unbind_topic("orders.new", "new_orders")
      def unbind_topic(pattern, queue_name)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.unbind_topic($1::text, $2::text)",
            [pattern, queue_name]
          )
        end

        result[0]["unbind_topic"] == "t"
      end

      # Sends a message via topic routing
      #
      # The message will be delivered to all queues whose bound patterns match the routing key.
      #
      # @param routing_key [String] dot-separated routing key (e.g., "orders.new.priority")
      # @param message [String] message as JSON string
      # @param headers [String, nil] optional headers as JSON string
      # @param delay [Numeric] delay in seconds before message becomes visible (integer or float; a float such as
      #   1.5 needs PGMQ v1.10.0+)
      # @return [Integer] count of queues the message was delivered to
      #
      # @example Basic topic send
      #   count = client.produce_topic("orders.new", '{"order_id":123}')
      #
      # @example With headers and delay
      #   count = client.produce_topic("orders.new.priority",
      #     '{"order_id":123}',
      #     headers: '{"trace_id":"abc"}',
      #     delay: 30)
      def produce_topic(routing_key, message, headers: nil, delay: 0)
        # send_topic takes whole seconds only, so a fractional delay goes through send_batch_topic
        if delay.is_a?(Numeric) && !delay.integer?
          return produce_batch_topic(routing_key, [message], headers: headers && [headers], delay: delay).size
        end

        result = with_connection do |conn|
          if headers
            conn.exec_params(
              "SELECT pgmq.send_topic($1::text, $2::jsonb, $3::jsonb, $4::integer)",
              [routing_key, message, headers, delay]
            )
          elsif delay > 0
            conn.exec_params(
              "SELECT pgmq.send_topic($1::text, $2::jsonb, $3::integer)",
              [routing_key, message, delay]
            )
          else
            conn.exec_params(
              "SELECT pgmq.send_topic($1::text, $2::jsonb)",
              [routing_key, message]
            )
          end
        end

        result[0]["send_topic"].to_i
      end

      # Sends multiple messages via topic routing
      #
      # All messages will be delivered to all queues whose bound patterns match the routing key.
      #
      # @param routing_key [String] dot-separated routing key
      # @param messages [Array<String>] array of message payloads as JSON strings
      # @param headers [Array<String>, nil] optional array of headers as JSON strings
      # @param delay [Numeric, Time] delay in seconds before messages become visible (integer or float;
      #   a float such as 1.5 needs PGMQ v1.10.0+), or an absolute Time at which the messages become visible,
      #   including ActiveSupport::TimeWithZone (PGMQ v1.10.0+)
      # @return [Array<Hash>] array of hashes with :queue_name and :msg_id
      #
      # @example Batch topic send
      #   results = client.produce_batch_topic("orders.new", [
      #     '{"order_id":1}',
      #     '{"order_id":2}'
      #   ])
      #   # => [{ queue_name: "new_orders", msg_id: "1" }, ...]
      def produce_batch_topic(routing_key, messages, headers: nil, delay: 0)
        BATCH_CONTRACT.validate!({ messages: messages, headers: headers }, ArgumentError)
        return [] if messages.empty?

        result = with_connection do |conn|
          encoder = PG::TextEncoder::Array.new
          encoded_messages = encoder.encode(messages)

          delay_sql, delay_value = delay_argument(delay, headers ? 4 : 3)

          if headers
            encoded_headers = encoder.encode(headers)
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch_topic($1::text, $2::jsonb[], $3::jsonb[], #{delay_sql})",
              [routing_key, encoded_messages, encoded_headers, delay_value]
            )
          elsif delay.is_a?(Integer) && delay <= 0
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch_topic($1::text, $2::jsonb[])",
              [routing_key, encoded_messages]
            )
          else
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch_topic($1::text, $2::jsonb[], #{delay_sql})",
              [routing_key, encoded_messages, delay_value]
            )
          end
        end

        result.map do |row|
          { queue_name: row["queue_name"], msg_id: row["msg_id"] }
        end
      end

      # Lists all topic bindings
      #
      # @param queue_name [String, nil] optional queue name to filter by
      # @return [Array<Hash>] array of binding hashes with pattern, queue_name, bound_at
      #
      # @example List all bindings
      #   bindings = client.list_topic_bindings
      #   # => [{ pattern: "orders.*", queue_name: "orders", bound_at: "..." }, ...]
      #
      # @example List bindings for specific queue
      #   bindings = client.list_topic_bindings(queue_name: "orders")
      def list_topic_bindings(queue_name: nil)
        result = with_connection do |conn|
          if queue_name
            validate_queue_name!(queue_name)
            conn.exec_params(
              "SELECT pattern, queue_name, bound_at FROM pgmq.list_topic_bindings($1::text)",
              [queue_name]
            )
          else
            conn.exec("SELECT pattern, queue_name, bound_at FROM pgmq.list_topic_bindings()")
          end
        end

        result.map do |row|
          {
            pattern: row["pattern"],
            queue_name: row["queue_name"],
            bound_at: row["bound_at"]
          }
        end
      end

      # Tests which queues a routing key would match
      #
      # Useful for debugging topic routing configurations.
      #
      # @param routing_key [String] routing key to test
      # @return [Array<Hash>] array of matched bindings with pattern and queue_name
      #
      # @example Test routing
      #   matches = client.test_routing("orders.new.priority")
      #   # => [{ pattern: "orders.*", queue_name: "new_orders" }, ...]
      def test_routing(routing_key)
        result = with_connection do |conn|
          conn.exec_params(
            "SELECT pattern, queue_name FROM pgmq.test_routing($1::text)",
            [routing_key]
          )
        end

        result.map do |row|
          { pattern: row["pattern"], queue_name: row["queue_name"] }
        end
      end

      # Validates a routing key
      #
      # Routing keys are dot-separated words (no wildcards allowed). Returns false for invalid routing keys (PGMQ raises
      # an error for invalid keys).
      #
      # @param routing_key [String] routing key to validate
      # @return [Boolean] true if valid, false if invalid
      #
      # @example
      #   client.validate_routing_key("orders.new.priority")  # => true
      #   client.validate_routing_key("orders.*")             # => false (wildcards not allowed)
      def validate_routing_key(routing_key)
        result = with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.validate_routing_key($1::text)",
            [routing_key]
          )
        end

        result[0]["validate_routing_key"] == "t"
      rescue PGMQ::Errors::ConnectionError => e
        # PGMQ raises an error for invalid routing keys
        return false if e.message.include?("invalid characters")

        raise
      end

      # Validates a topic pattern
      #
      # Topic patterns can include wildcards: * (single word) or # (zero or more words).
      #
      # @param pattern [String] topic pattern to validate
      # @return [Boolean] true if valid
      #
      # @example
      #   client.validate_topic_pattern("orders.*")    # => true
      #   client.validate_topic_pattern("orders.#")    # => true
      #   client.validate_topic_pattern("orders.new")  # => true
      def validate_topic_pattern(pattern)
        result = with_connection do |conn|
          conn.exec_params(
            "SELECT pgmq.validate_topic_pattern($1::text)",
            [pattern]
          )
        end

        result[0]["validate_topic_pattern"] == "t"
      end
    end
  end
end
