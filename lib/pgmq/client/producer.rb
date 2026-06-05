# frozen_string_literal: true

module PGMQ
  class Client
    # Message producing operations
    #
    # This module handles producing messages to queues, both individual messages
    # and batches. Users must serialize messages to JSON strings themselves.
    module Producer
      # Produces a message to a queue
      #
      # @param queue_name [String] name of the queue
      # @param message [String] message as JSON string (for PostgreSQL JSONB)
      # @param headers [String, nil] optional headers as JSON string (for metadata, routing, tracing)
      # @param delay [Numeric, Time] delay in seconds before message becomes visible (integer or float),
      #   or an absolute Time at which the message becomes visible, including ActiveSupport::TimeWithZone
      #   (PGMQ v1.10.0+)
      # @return [String] message ID as string
      #
      # @example Basic produce
      #   msg_id = client.produce("orders", '{"order_id":123,"total":99.99}')
      #
      # @example With integer delay (seconds)
      #   msg_id = client.produce("orders", '{"data":"value"}', delay: 60)
      #
      # @example With absolute timestamp delay
      #   msg_id = client.produce("orders", '{"data":"value"}', delay: Time.now + 3600)
      #
      # @example With headers for routing/tracing
      #   msg_id = client.produce("orders", '{"order_id":123}',
      #     headers: '{"trace_id":"abc123","priority":"high"}')
      #
      # @example With headers and absolute timestamp delay
      #   msg_id = client.produce("orders", '{"order_id":123}',
      #     headers: '{"correlation_id":"req-456"}',
      #     delay: Time.now + 300)
      #
      # @note Users must serialize to JSON themselves. Higher-level frameworks
      #       should handle serialization.
      def produce(
        queue_name,
        message,
        headers: nil,
        delay: 0
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          if headers && !delay.is_a?(Numeric)
            conn.exec_params(
              "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::timestamptz)",
              [queue_name, message, headers, delay.to_time.utc.iso8601(6)]
            )
          elsif headers
            conn.exec_params(
              "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::integer)",
              [queue_name, message, headers, delay]
            )
          elsif !delay.is_a?(Numeric)
            conn.exec_params(
              "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::timestamptz)",
              [queue_name, message, delay.to_time.utc.iso8601(6)]
            )
          else
            conn.exec_params(
              "SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer)",
              [queue_name, message, delay]
            )
          end
        end

        result[0]["send"]
      end

      # Produces multiple messages to a queue in a batch
      #
      # @param queue_name [String] name of the queue
      # @param messages [Array<String>] array of message payloads as JSON strings
      # @param headers [Array<String>, nil] optional array of headers as JSON strings (must match messages length)
      # @param delay [Numeric, Time] delay in seconds before messages become visible (integer or float),
      #   or an absolute Time at which the messages become visible, including ActiveSupport::TimeWithZone
      #   (PGMQ v1.10.0+)
      # @return [Array<String>] array of message IDs
      # @raise [ArgumentError] if headers array length doesn't match messages length
      #
      # @example Basic batch produce
      #   ids = client.produce_batch("orders", [
      #     '{"order_id":1}',
      #     '{"order_id":2}',
      #     '{"order_id":3}'
      #   ])
      #
      # @example With headers (one per message)
      #   ids = client.produce_batch("orders",
      #     ['{"order_id":1}', '{"order_id":2}'],
      #     headers: ['{"priority":"high"}', '{"priority":"low"}'])
      #
      # @example With headers and delay
      #   ids = client.produce_batch("orders",
      #     ['{"order_id":1}', '{"order_id":2}'],
      #     headers: ['{"trace_id":"a"}', '{"trace_id":"b"}'],
      #     delay: 60)
      def produce_batch(
        queue_name,
        messages,
        headers: nil,
        delay: 0
      )
        validate_queue_name!(queue_name)
        return [] if messages.empty?

        if headers && headers.length != messages.length
          raise ArgumentError,
            "headers array length (#{headers.length}) must match messages array length (#{messages.length})"
        end

        result = with_connection do |conn|
          encoder = PG::TextEncoder::Array.new
          encoded_messages = encoder.encode(messages)

          if headers && !delay.is_a?(Numeric)
            encoded_headers = encoder.encode(headers)
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::timestamptz)",
              [queue_name, encoded_messages, encoded_headers, delay.to_time.utc.iso8601(6)]
            )
          elsif headers
            encoded_headers = encoder.encode(headers)
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::integer)",
              [queue_name, encoded_messages, encoded_headers, delay]
            )
          elsif !delay.is_a?(Numeric)
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::timestamptz)",
              [queue_name, encoded_messages, delay.to_time.utc.iso8601(6)]
            )
          else
            conn.exec_params(
              "SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::integer)",
              [queue_name, encoded_messages, delay]
            )
          end
        end

        result.map { |row| row["send_batch"] }
      end
    end
  end
end
