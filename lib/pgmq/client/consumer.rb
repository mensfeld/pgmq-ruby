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

      # Reads messages using SQS-style grouped ordering (throughput-optimised)
      #
      # Messages are grouped by the first key in their JSON payload. Unlike
      # round-robin, this strategy fills the requested batch from the oldest
      # group first, then moves on to the next group only when the first is
      # exhausted. Maximises throughput for bursty workloads at the cost of
      # fairness across groups.
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @return [Array<PGMQ::Message>] array of messages, oldest group first
      #
      # @example Throughput-first batch processing
      #   # Queue contains: user1_msg1, user1_msg2, user2_msg1
      #   messages = client.read_grouped("tasks", vt: 30, qty: 3)
      #   # Returns: user1_msg1, user1_msg2, user2_msg1  (drains user1 first)
      #
      # @example High-volume processing where fairness is not required
      #   loop do
      #     messages = client.read_grouped("jobs", vt: 30, qty: 20)
      #     break if messages.empty?
      #     messages.each { |msg| process(msg) }
      #   end
      def read_grouped(queue_name, vt: DEFAULT_VT, qty: 1)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT * FROM pgmq.read_grouped($1::text, $2::integer, $3::integer)",
            [queue_name, vt, qty]
          )
        end

        result.map { |row| Message.new(row) }
      end

      # Reads messages using SQS-style grouped ordering with long-polling support
      #
      # Combines SQS-style throughput-first grouped ordering with long-polling.
      # Blocks up to max_poll_seconds if the queue is empty, returning as soon
      # as any message arrives.
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @param max_poll_seconds [Integer] maximum time to poll in seconds
      # @param poll_interval_ms [Integer] interval between polls in milliseconds
      # @return [Array<PGMQ::Message>] array of messages
      #
      # @example Poll with throughput-first grouped ordering
      #   messages = client.read_grouped_with_poll("jobs",
      #     vt: 30,
      #     qty: 10,
      #     max_poll_seconds: 5,
      #     poll_interval_ms: 100
      #   )
      def read_grouped_with_poll(
        queue_name,
        vt: DEFAULT_VT,
        qty: 1,
        max_poll_seconds: 5,
        poll_interval_ms: 100
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT * FROM pgmq.read_grouped_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)",
            [queue_name, vt, qty, max_poll_seconds, poll_interval_ms]
          )
        end

        result.map { |row| Message.new(row) }
      end

      # Reads one message per FIFO group from the head of each group
      #
      # Returns exactly one message - the oldest visible message - from each
      # distinct FIFO group, up to qty groups. Groups are determined by the
      # `x-pgmq-group` key in the message headers (set via the `headers:` param
      # on `produce`). Messages without that header key all land in a single
      # implicit default group, so only one of them is returned per call.
      #
      # Unlike `read_grouped` (which groups by the first payload key and drains
      # one group fully before moving to the next), `read_grouped_head` surfaces
      # the leading edge of every group in one call - useful for detecting
      # head-of-line stalls or building per-group progress dashboards.
      #
      # @note Requires PGMQ v1.11.1+.
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] maximum number of groups to sample
      # @return [Array<PGMQ::Message>] one message per group, up to qty
      #
      # @example Sample the head of each group
      #   # Produce with x-pgmq-group headers so each tenant is a separate group
      #   client.produce("tasks", '{"job":"build"}', headers: '{"x-pgmq-group":"tenant_a"}')
      #   client.produce("tasks", '{"job":"test"}',  headers: '{"x-pgmq-group":"tenant_b"}')
      #   messages = client.read_grouped_head("tasks", vt: 30, qty: 10)
      #   # Returns: one message from tenant_a and one from tenant_b
      #
      # @example Monitor for stuck groups
      #   heads = client.read_grouped_head("jobs", vt: 30, qty: 100)
      #   heads.each do |msg|
      #     alert_if_stuck(msg) if msg.enqueued_at < Time.now - 3600
      #   end
      def read_grouped_head(queue_name, vt: DEFAULT_VT, qty: 1)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT * FROM pgmq.read_grouped_head($1::text, $2::integer, $3::integer)",
            [queue_name, vt, qty]
          )
        end

        result.map { |row| Message.new(row) }
      end

      # Reads messages using grouped round-robin ordering
      #
      # Messages are grouped by the first key in their JSON payload and returned
      # in round-robin order across groups. This ensures fair processing when
      # messages from different entities (users, orders, etc.) are in the queue.
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @return [Array<PGMQ::Message>] array of messages in round-robin order
      #
      # @example Fair processing across users
      #   # Queue contains: user1_msg1, user1_msg2, user2_msg1, user3_msg1
      #   messages = client.read_grouped_rr("tasks", vt: 30, qty: 4)
      #   # Returns in round-robin: user1_msg1, user2_msg1, user3_msg1, user1_msg2
      #
      # @example Prevent single entity from monopolizing worker
      #   loop do
      #     messages = client.read_grouped_rr("orders", vt: 30, qty: 10)
      #     break if messages.empty?
      #     messages.each { |msg| process(msg) }
      #   end
      def read_grouped_rr(queue_name, vt: DEFAULT_VT, qty: 1)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT * FROM pgmq.read_grouped_rr($1::text, $2::integer, $3::integer)",
            [queue_name, vt, qty]
          )
        end

        result.map { |row| Message.new(row) }
      end

      # Reads messages using grouped round-robin with long-polling support
      #
      # Combines grouped round-robin ordering with long-polling for efficient
      # and fair message consumption.
      #
      # @param queue_name [String] name of the queue
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] number of messages to read
      # @param max_poll_seconds [Integer] maximum time to poll in seconds
      # @param poll_interval_ms [Integer] interval between polls in milliseconds
      # @return [Array<PGMQ::Message>] array of messages in round-robin order
      #
      # @example Long-polling with fair ordering
      #   messages = client.read_grouped_rr_with_poll("tasks",
      #     vt: 30,
      #     qty: 10,
      #     max_poll_seconds: 5,
      #     poll_interval_ms: 100
      #   )
      def read_grouped_rr_with_poll(
        queue_name,
        vt: DEFAULT_VT,
        qty: 1,
        max_poll_seconds: 5,
        poll_interval_ms: 100
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            "SELECT * FROM pgmq.read_grouped_rr_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)",
            [queue_name, vt, qty, max_poll_seconds, poll_interval_ms]
          )
        end

        result.map { |row| Message.new(row) }
      end
    end
  end
end
