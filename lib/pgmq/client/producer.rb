# frozen_string_literal: true

module PGMQ
  class Client
    # Message sending operations
    #
    # This module handles sending messages to queues, both individual messages
    # and batches. Users must serialize messages to JSON strings themselves.
    module Producer
      # Sends a message to a queue
      #
      # @param queue_name [String] name of the queue
      # @param message [String] message as JSON string (for PostgreSQL JSONB)
      # @param headers [String, nil] optional headers as JSON string (for metadata, routing, tracing)
      # @param delay [Integer] delay in seconds before message becomes visible
      # @return [String] message ID as string
      #
      # @example Basic send
      #   msg_id = client.send("orders", '{"order_id":123,"total":99.99}')
      #
      # @example With delay
      #   msg_id = client.send("orders", '{"data":"value"}', delay: 60)
      #
      # @example With headers for routing/tracing
      #   msg_id = client.send("orders", '{"order_id":123}',
      #     headers: '{"trace_id":"abc123","priority":"high"}')
      #
      # @example With headers and delay
      #   msg_id = client.send("orders", '{"order_id":123}',
      #     headers: '{"correlation_id":"req-456"}',
      #     delay: 30)
      #
      # @note Users must serialize to JSON themselves. Higher-level frameworks
      #       should handle serialization.
      def send(
        queue_name,
        message,
        headers: nil,
        delay: 0
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          if headers
            conn.exec_params(
              'SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::integer)',
              [queue_name, message, headers, delay]
            )
          else
            conn.exec_params(
              'SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer)',
              [queue_name, message, delay]
            )
          end
        end

        result[0]['send']
      end

      # Sends multiple messages to a queue in a batch
      #
      # @param queue_name [String] name of the queue
      # @param messages [Array<String>] array of message payloads as JSON strings
      # @param headers [Array<String>, nil] optional array of headers as JSON strings (must match messages length)
      # @param delay [Integer] delay in seconds before messages become visible
      # @return [Array<String>] array of message IDs
      #
      # @example Basic batch send
      #   ids = client.send_batch("orders", [
      #     '{"order_id":1}',
      #     '{"order_id":2}',
      #     '{"order_id":3}'
      #   ])
      #
      # @example With headers (one per message)
      #   ids = client.send_batch("orders",
      #     ['{"order_id":1}', '{"order_id":2}'],
      #     headers: ['{"priority":"high"}', '{"priority":"low"}'])
      #
      # @example With headers and delay
      #   ids = client.send_batch("orders",
      #     ['{"order_id":1}', '{"order_id":2}'],
      #     headers: ['{"trace_id":"a"}', '{"trace_id":"b"}'],
      #     delay: 60)
      #
      # @raise [ArgumentError] if headers array length doesn't match messages length
      def send_batch(
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

        # Use PostgreSQL array parameter binding for security
        # PG gem will properly encode the array values
        result = with_connection do |conn|
          # Create array encoder for proper PostgreSQL array formatting
          encoder = PG::TextEncoder::Array.new
          encoded_messages = encoder.encode(messages)

          if headers
            encoded_headers = encoder.encode(headers)
            conn.exec_params(
              'SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::integer)',
              [queue_name, encoded_messages, encoded_headers, delay]
            )
          else
            conn.exec_params(
              'SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::integer)',
              [queue_name, encoded_messages, delay]
            )
          end
        end

        result.map { |row| row['send_batch'] }
      end
    end
  end
end
