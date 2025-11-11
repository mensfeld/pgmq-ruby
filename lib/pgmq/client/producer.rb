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
      # @param delay [Integer] delay in seconds before message becomes visible
      # @return [String] message ID as string
      # @note Users must serialize to JSON themselves. Higher-level frameworks
      #       should handle serialization.
      #
      # @example
      #   msg_id = client.send("orders", '{"order_id":123,"total":99.99}')
      #
      # @example With delay
      #   msg_id = client.send("orders", '{"data":"value"}', delay: 60)
      def send(
        queue_name,
        message,
        delay: 0
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            'SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer)',
            [queue_name, message, delay]
          )
        end

        result[0]['send']
      end

      # Sends multiple messages to a queue in a batch
      #
      # @param queue_name [String] name of the queue
      # @param messages [Array<Hash>] array of message payloads
      # @param delay [Integer] delay in seconds before messages become visible
      # @return [Array<Integer>] array of message IDs
      #
      # @example
      #   ids = client.send_batch("orders", [
      #     { order_id: 1 },
      #     { order_id: 2 },
      #     { order_id: 3 }
      #   ])
      def send_batch(
        queue_name,
        messages,
        delay: 0
      )
        validate_queue_name!(queue_name)
        return [] if messages.empty?

        # Use PostgreSQL array parameter binding for security
        # PG gem will properly encode the array values
        result = with_connection do |conn|
          # Create array encoder for proper PostgreSQL array formatting
          encoder = PG::TextEncoder::Array.new
          encoded_array = encoder.encode(messages)

          conn.exec_params(
            'SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::integer)',
            [queue_name, encoded_array, delay]
          )
        end

        result.map { |row| row['send_batch'] }
      end
    end
  end
end
