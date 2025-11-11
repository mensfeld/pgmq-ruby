# frozen_string_literal: true

module PGMQ
  class Client
    # Multi-queue operations
    #
    # This module handles efficient operations across multiple queues using single
    # database queries with UNION ALL for optimal performance.
    module MultiQueue
      # Reads from multiple queues in a single query
      #
      # This is the most efficient way to monitor multiple queues with a single
      # database connection. Uses UNION ALL to read from all queues in one query.
      #
      # @param queue_names [Array<String>] array of queue names to read from
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] max messages to read per queue
      # @param limit [Integer, nil] max total messages across all queues (nil = all)
      # @return [Array<PGMQ::Message>] array of messages with queue_name attribute
      #
      # @example Read from multiple queues (gets first available from any queue)
      #   msg = client.read_multi(['orders', 'notifications', 'emails'], vt: 30, limit: 1).first
      #   puts "Got message from: #{msg.queue_name}"
      #
      # @example Read up to 5 messages from any of the queues
      #   messages = client.read_multi(['queue1', 'queue2', 'queue3'], vt: 30, qty: 5, limit: 5)
      #   messages.each do |msg|
      #     puts "Processing #{msg.queue_name}: #{msg.payload}"
      #     client.delete(msg.queue_name, msg.msg_id)
      #   end
      #
      # @example Poll all queues efficiently (single connection, single query)
      #   loop do
      #     messages = client.read_multi(['orders', 'emails', 'webhooks'], vt: 30, qty: 10, limit: 10)
      #     break if messages.empty?
      #     messages.each { |msg| process(msg) }
      #   end
      def read_multi(
        queue_names,
        vt: DEFAULT_VT,
        qty: 1,
        limit: nil
      )
        raise ArgumentError, 'queue_names must be an array' unless queue_names.is_a?(Array)
        raise ArgumentError, 'queue_names cannot be empty' if queue_names.empty?
        raise ArgumentError, 'queue_names cannot exceed 50 queues' if queue_names.size > 50

        # Validate all queue names (prevents SQL injection)
        queue_names.each { |qn| validate_queue_name!(qn) }

        # Build UNION ALL query for all queues
        # Note: Queue names are validated, so this is safe from SQL injection
        union_queries = queue_names.map do |queue_name|
          # Escape single quotes in queue name (though validation should prevent this)
          escaped_name = queue_name.gsub("'", "''")
          "SELECT '#{escaped_name}'::text as queue_name, * " \
            "FROM pgmq.read('#{escaped_name}'::text, #{vt.to_i}, #{qty.to_i})"
        end

        sql = union_queries.join("\nUNION ALL\n")
        sql += "\nLIMIT #{limit.to_i}" if limit

        result = with_connection do |conn|
          conn.exec(sql)
        end

        result.map do |row|
          Message.new(row)
        end
      end

      # Reads from multiple queues with long-polling (waits for messages)
      #
      # Efficiently polls multiple queues waiting for the first available message.
      # This uses a single connection with periodic polling until a message arrives
      # or the timeout is reached.
      #
      # @param queue_names [Array<String>] array of queue names to poll
      # @param vt [Integer] visibility timeout in seconds
      # @param qty [Integer] max messages to read per queue
      # @param limit [Integer, nil] max total messages across all queues
      # @param max_poll_seconds [Integer] maximum seconds to wait for messages
      # @param poll_interval_ms [Integer] milliseconds between polls
      # @return [Array<PGMQ::Message>] array of messages (empty if timeout)
      #
      # @example Wait for first available message from any queue
      #   msg = client.read_multi_with_poll(
      #     ['orders', 'notifications', 'emails'],
      #     vt: 30,
      #     max_poll_seconds: 5
      #   ).first
      #
      # @example Worker loop with efficient multi-queue polling
      #   loop do
      #     messages = client.read_multi_with_poll(
      #       ['queue1', 'queue2', 'queue3'],
      #       vt: 30,
      #       qty: 10,
      #       limit: 10,
      #       max_poll_seconds: 5
      #     )
      #
      #     messages.each do |msg|
      #       process(msg.queue_name, msg.payload)
      #       client.delete(msg.queue_name, msg.msg_id)
      #     end
      #   end
      def read_multi_with_poll(
        queue_names,
        vt: DEFAULT_VT,
        qty: 1,
        limit: nil,
        max_poll_seconds: 5,
        poll_interval_ms: 100
      )
        raise ArgumentError, 'queue_names must be an array' unless queue_names.is_a?(Array)
        raise ArgumentError, 'queue_names cannot be empty' if queue_names.empty?
        raise ArgumentError, 'queue_names cannot exceed 50 queues' if queue_names.size > 50

        start_time = Time.now
        poll_interval_seconds = poll_interval_ms / 1000.0

        loop do
          # Try to read from any queue
          messages = read_multi(queue_names, vt: vt, qty: qty, limit: limit)
          return messages if messages.any?

          # Check timeout
          elapsed = Time.now - start_time
          break if elapsed >= max_poll_seconds

          # Sleep for poll interval (or remaining time, whichever is less)
          remaining = max_poll_seconds - elapsed
          sleep [poll_interval_seconds, remaining].min
        end

        [] # Return empty array on timeout
      end

      # Pops a message from multiple queues (atomic read + delete)
      #
      # Efficiently gets and immediately deletes the first available message from
      # any of the specified queues. Uses a single query with UNION ALL.
      #
      # @param queue_names [Array<String>] array of queue names
      # @return [PGMQ::Message, nil] message with queue_name attribute, or nil if no messages
      #
      # @example Get first available message from any queue and auto-delete
      #   msg = client.pop_multi(['orders', 'notifications', 'emails'])
      #   if msg
      #     puts "Got message from #{msg.queue_name}"
      #     process(msg.payload)
      #     # No need to delete - already deleted!
      #   end
      #
      # @example Worker loop with atomic pop from multiple queues
      #   loop do
      #     msg = client.pop_multi(['queue1', 'queue2', 'queue3'])
      #     break unless msg
      #     process(msg.queue_name, msg.payload)
      #   end
      def pop_multi(queue_names)
        raise ArgumentError, 'queue_names must be an array' unless queue_names.is_a?(Array)
        raise ArgumentError, 'queue_names cannot be empty' if queue_names.empty?
        raise ArgumentError, 'queue_names cannot exceed 50 queues' if queue_names.size > 50

        # Validate all queue names
        queue_names.each { |qn| validate_queue_name!(qn) }

        # Build UNION ALL query for all queues
        union_queries = queue_names.map do |queue_name|
          escaped_name = queue_name.gsub("'", "''")
          "SELECT '#{escaped_name}'::text as queue_name, * FROM pgmq.pop('#{escaped_name}'::text)"
        end

        sql = "#{union_queries.join("\nUNION ALL\n")}\nLIMIT 1"

        result = with_connection do |conn|
          conn.exec(sql)
        end

        return nil if result.ntuples.zero?

        Message.new(result[0])
      end
    end
  end
end
