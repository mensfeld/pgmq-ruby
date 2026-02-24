# frozen_string_literal: true

module PGMQ
  class Client
    # Single-queue message reading operations
    #
    # This module handles reading messages from a single queue, including basic reads,
    # batch reads, and long-polling for efficient message consumption.
    module Consumer
      # Reads a message from the queue
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param conditional [Hash] optional JSONB filter for message payload
      # @return [PGMQ::Message, nil] message object or nil if queue is empty
      #
      # @example
      #   msg = client.read("orders", vt: 30)
      #   if msg
      #     process(msg.payload)
      #     client.delete("orders", msg.msg_id)
      #   end
      #
      # @example With conditional filtering
      #   msg = client.read("orders", vt: 30, conditional: { type: "priority", status: "pending" })
      def read(
        queue_name,
        vt: DEFAULT_VT,
        conditional: {}
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          if conditional.empty?
            conn.exec_params(
              "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer)",
              [queue_name, vt, 1]
            )
          else
            conn.exec_params(
              "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer, $4::jsonb)",
              [queue_name, vt, 1, conditional.to_json]
            )
          end
        end

        return nil if result.ntuples.zero?

        Message.new(result[0])
      end

      # Reads multiple messages from the queue
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @param conditional [Hash] optional JSONB filter for message payload
      # @return [Array<PGMQ::Message>] array of messages
      #
      # @example
      #   messages = client.read_batch("orders", vt: 30, qty: 10)
      #   messages.each do |msg|
      #     process(msg.payload)
      #     client.delete("orders", msg.msg_id)
      #   end
      #
      # @example With conditional filtering
      #   messages = client.read_batch(
      #     "orders",
      #     vt: 30,
      #     qty: 10,
      #     conditional: { priority: "high" }
      #   )
      def read_batch(
        queue_name,
        vt: DEFAULT_VT,
        qty: 1,
        conditional: {}
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          if conditional.empty?
            conn.exec_params(
              "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer)",
              [queue_name, vt, qty]
            )
          else
            conn.exec_params(
              "SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer, $4::jsonb)",
              [queue_name, vt, qty, conditional.to_json]
            )
          end
        end

        result.map { |row| Message.new(row) }
      end

      # Reads messages with long-polling support
      #
      # Polls the queue for messages, waiting up to max_poll_seconds if queue is empty
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @param max_poll_seconds [Integer] maximum time to poll in seconds
      # @param poll_interval_ms [Integer] interval between polls in milliseconds
      # @param conditional [Hash] optional JSONB filter for message payload
      # @return [Array<PGMQ::Message>] array of messages
      #
      # @example
      #   messages = client.read_with_poll("orders",
      #     vt: 30,
      #     qty: 5,
      #     max_poll_seconds: 10,
      #     poll_interval_ms: 250
      #   )
      #
      # @example With conditional filtering
      #   messages = client.read_with_poll("orders",
      #     vt: 30,
      #     qty: 5,
      #     conditional: { status: "pending" }
      #   )
      def read_with_poll(
        queue_name,
        vt: DEFAULT_VT,
        qty: 1,
        max_poll_seconds: 5,
        poll_interval_ms: 100,
        conditional: {}
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          if conditional.empty?
            conn.exec_params(
              "SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)",
              [queue_name, vt, qty, max_poll_seconds, poll_interval_ms]
            )
          else
            sql = "SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, " \
                  "$4::integer, $5::integer, $6::jsonb)"
            conn.exec_params(
              sql,
              [queue_name, vt, qty, max_poll_seconds, poll_interval_ms, conditional.to_json]
            )
          end
        end

        result.map { |row| Message.new(row) }
      end
    end
  end
end
