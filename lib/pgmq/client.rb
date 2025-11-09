# frozen_string_literal: true

require_relative 'connection'
require_relative 'message'
require_relative 'metrics'
require_relative 'queue_metadata'

module PGMQ
  # Low-level client for interacting with PGMQ (Postgres Message Queue)
  #
  # This is a thin wrapper around PGMQ SQL functions. For higher-level
  # abstractions (job processing, retries, Rails integration), use pgmq-framework.
  #
  # @example Basic usage
  #   client = PGMQ::Client.new(
  #     host: 'localhost',
  #     dbname: 'mydb',
  #     user: 'postgres',
  #     password: 'postgres'
  #   )
  #   client.create('my_queue')
  #   msg_id = client.send('my_queue', { data: 'value' })
  #   msg = client.read('my_queue', vt: 30)
  #   client.delete('my_queue', msg.msg_id)
  #
  # @example With Rails/ActiveRecord (reuses Rails connection pool)
  #   client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })
  #
  # @example With connection string
  #   client = PGMQ::Client.new('postgres://localhost/mydb')
  class Client
    include Transaction

    # Default visibility timeout in seconds
    DEFAULT_VT = 30

    # @return [PGMQ::Connection] the connection manager
    attr_reader :connection

    # @return [PGMQ::Serializers::Base] the message serializer
    attr_reader :serializer

    # Creates a new PGMQ client
    #
    # @param conn_params [String, Hash, Proc, PGMQ::Connection, nil] connection parameters
    # @param pool_size [Integer] connection pool size
    # @param pool_timeout [Integer] connection pool timeout in seconds
    # @param auto_reconnect [Boolean] automatically reconnect on connection errors (default: true)
    # @param serializer [PGMQ::Serializers::Base, nil] message serializer (default: JSON)
    #
    # @example Connection string
    #   client = PGMQ::Client.new('postgres://user:pass@localhost/db')
    #
    # @example Connection hash
    #   client = PGMQ::Client.new(host: 'localhost', dbname: 'mydb', user: 'postgres')
    #
    # @example With custom serializer
    #   client = PGMQ::Client.new(serializer: PGMQ::Serializers::MessagePack.new)
    #
    # @example Inject existing connection (for Rails)
    #   client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })
    #
    # @example Disable auto-reconnect
    #   client = PGMQ::Client.new(auto_reconnect: false)
    def initialize(conn_params = nil, pool_size: Connection::DEFAULT_POOL_SIZE,
                   pool_timeout: Connection::DEFAULT_POOL_TIMEOUT,
                   auto_reconnect: true,
                   serializer: nil)
      @connection = if conn_params.is_a?(Connection)
                      conn_params
                    else
                      Connection.new(conn_params, pool_size: pool_size, pool_timeout: pool_timeout,
                                                  auto_reconnect: auto_reconnect)
                    end
      @serializer = serializer || Serializers::JSON.new
    end

    # Creates a new queue
    #
    # @param queue_name [String] name of the queue to create
    # @return [void]
    # @raise [PGMQ::InvalidQueueNameError] if queue name is invalid
    # @raise [PGMQ::ConnectionError] if database operation fails
    #
    # @example
    #   client.create("orders")
    def create(queue_name)
      validate_queue_name!(queue_name)

      with_connection do |conn|
        conn.exec_params('SELECT pgmq.create($1::text)', [queue_name])
      end

      nil
    end

    # Creates a partitioned queue
    #
    # Requires pg_partman extension to be installed
    #
    # @param queue_name [String] name of the queue
    # @param partition_interval [String] partition interval (e.g., "daily", "10000")
    # @param retention_interval [String] retention interval (e.g., "7 days", "100000")
    # @return [void]
    #
    # @example
    #   client.create_partitioned("big_queue",
    #     partition_interval: "daily",
    #     retention_interval: "7 days"
    #   )
    def create_partitioned(queue_name, partition_interval: '10000', retention_interval: '100000')
      validate_queue_name!(queue_name)

      with_connection do |conn|
        conn.exec_params(
          'SELECT pgmq.create_partitioned($1::text, $2::text, $3::text)',
          [queue_name, partition_interval, retention_interval]
        )
      end

      nil
    end

    # Creates an unlogged queue for higher throughput (no crash recovery)
    #
    # @param queue_name [String] name of the queue
    # @return [void]
    #
    # @example
    #   client.create_unlogged("fast_queue")
    def create_unlogged(queue_name)
      validate_queue_name!(queue_name)

      with_connection do |conn|
        conn.exec_params('SELECT pgmq.create_unlogged($1::text)', [queue_name])
      end

      nil
    end

    # Drops a queue and its archive table
    #
    # @param queue_name [String] name of the queue to drop
    # @return [Boolean] true if queue was dropped
    #
    # @example
    #   client.drop_queue("old_queue")
    def drop_queue(queue_name)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params('SELECT pgmq.drop_queue($1::text)', [queue_name])
      end

      return false if result.ntuples.zero?

      result[0]['drop_queue'] == true
    end

    # Lists all queues
    #
    # @return [Array<PGMQ::QueueMetadata>] array of queue metadata objects
    #
    # @example
    #   queues = client.list_queues
    #   queues.each { |q| puts q.queue_name }
    def list_queues
      result = with_connection do |conn|
        conn.exec('SELECT * FROM pgmq.list_queues()')
      end

      result.map { |row| QueueMetadata.new(row) }
    end

    # Sends a message to a queue
    #
    # @param queue_name [String] name of the queue
    # @param message [Hash, Object] message payload
    # @param delay [Integer] delay in seconds before message becomes visible
    # @return [Integer] message ID
    #
    # @example
    #   msg_id = client.send("orders", { order_id: 123, total: 99.99 })
    #
    # @example With delay
    #   msg_id = client.send("orders", { data: "value" }, delay: 60)
    def send(queue_name, message, delay: 0)
      validate_queue_name!(queue_name)

      serialized = @serializer.serialize(message)

      result = with_connection do |conn|
        conn.exec_params(
          'SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer)',
          [queue_name, serialized, delay]
        )
      end

      result[0]['send'].to_i
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
    def send_batch(queue_name, messages, delay: 0)
      validate_queue_name!(queue_name)
      return [] if messages.empty?

      serialized_messages = messages.map { |msg| @serializer.serialize(msg) }

      # Use PostgreSQL array parameter binding for security
      # PG gem will properly encode the array values
      result = with_connection do |conn|
        # Create array encoder for proper PostgreSQL array formatting
        encoder = PG::TextEncoder::Array.new
        encoded_array = encoder.encode(serialized_messages)

        conn.exec_params(
          'SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::integer)',
          [queue_name, encoded_array, delay]
        )
      end

      result.map { |row| row['send_batch'].to_i }
    end

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
    def read(queue_name, vt: DEFAULT_VT, conditional: {})
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        if conditional.empty?
          conn.exec_params(
            'SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer)',
            [queue_name, vt, 1]
          )
        else
          conn.exec_params(
            'SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer, $4::jsonb)',
            [queue_name, vt, 1, conditional.to_json]
          )
        end
      end

      return nil if result.ntuples.zero?

      Message.new(result[0], serializer: @serializer)
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
    #   messages = client.read_batch("orders", vt: 30, qty: 10, conditional: { priority: "high" })
    def read_batch(queue_name, vt: DEFAULT_VT, qty: 1, conditional: {})
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        if conditional.empty?
          conn.exec_params(
            'SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer)',
            [queue_name, vt, qty]
          )
        else
          conn.exec_params(
            'SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer, $4::jsonb)',
            [queue_name, vt, qty, conditional.to_json]
          )
        end
      end

      result.map { |row| Message.new(row, serializer: @serializer) }
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
    def read_with_poll(queue_name, vt: DEFAULT_VT, qty: 1,
                       max_poll_seconds: 5, poll_interval_ms: 100, conditional: {})
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        if conditional.empty?
          conn.exec_params(
            'SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)',
            [queue_name, vt, qty, max_poll_seconds, poll_interval_ms]
          )
        else
          sql = 'SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, ' \
                '$4::integer, $5::integer, $6::jsonb)'
          conn.exec_params(
            sql,
            [queue_name, vt, qty, max_poll_seconds, poll_interval_ms, conditional.to_json]
          )
        end
      end

      result.map { |row| Message.new(row, serializer: @serializer) }
    end

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
    def read_multi(queue_names, vt: DEFAULT_VT, qty: 1, limit: nil)
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
        msg = Message.new(row, serializer: @serializer)
        # Add queue_name as an attribute
        msg.instance_variable_set(:@queue_name, row['queue_name'])
        def msg.queue_name
          @queue_name
        end
        msg
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
    def read_multi_with_poll(queue_names, vt: DEFAULT_VT, qty: 1, limit: nil,
                             max_poll_seconds: 5, poll_interval_ms: 100)
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

    # Pops a message (atomic read + delete)
    #
    # @param queue_name [String] name of the queue
    # @return [PGMQ::Message, nil] message object or nil if queue is empty
    #
    # @example
    #   msg = client.pop("orders")
    #   process(msg.payload) if msg
    def pop(queue_name)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params('SELECT * FROM pgmq.pop($1::text)', [queue_name])
      end

      return nil if result.ntuples.zero?

      Message.new(result[0], serializer: @serializer)
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

      msg = Message.new(result[0], serializer: @serializer)
      msg.instance_variable_set(:@queue_name, result[0]['queue_name'])
      def msg.queue_name
        @queue_name
      end
      msg
    end

    # Deletes a message from the queue
    #
    # @param queue_name [String] name of the queue
    # @param msg_id [Integer] message ID to delete
    # @return [Boolean] true if message was deleted
    #
    # @example
    #   client.delete("orders", 123)
    def delete(queue_name, msg_id)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params(
          'SELECT pgmq.delete($1::text, $2::bigint)',
          [queue_name, msg_id]
        )
      end

      return false if result.ntuples.zero?

      result[0]['delete'] == true
    end

    # Deletes multiple messages from the queue
    #
    # @param queue_name [String] name of the queue
    # @param msg_ids [Array<Integer>] array of message IDs to delete
    # @return [Array<Integer>] array of successfully deleted message IDs
    #
    # @example
    #   deleted = client.delete_batch("orders", [101, 102, 103])
    def delete_batch(queue_name, msg_ids)
      validate_queue_name!(queue_name)
      return [] if msg_ids.empty?

      # Use PostgreSQL array parameter binding
      result = with_connection do |conn|
        encoder = PG::TextEncoder::Array.new
        encoded_array = encoder.encode(msg_ids)

        conn.exec_params(
          'SELECT * FROM pgmq.delete($1::text, $2::bigint[])',
          [queue_name, encoded_array]
        )
      end

      result.map { |row| row['delete'].to_i }
    end

    # Archives a message
    #
    # @param queue_name [String] name of the queue
    # @param msg_id [Integer] message ID to archive
    # @return [Boolean] true if message was archived
    #
    # @example
    #   client.archive("orders", 123)
    def archive(queue_name, msg_id)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params(
          'SELECT pgmq.archive($1::text, $2::bigint)',
          [queue_name, msg_id]
        )
      end

      return false if result.ntuples.zero?

      result[0]['archive'] == true
    end

    # Archives multiple messages
    #
    # @param queue_name [String] name of the queue
    # @param msg_ids [Array<Integer>] array of message IDs to archive
    # @return [Array<Integer>] array of successfully archived message IDs
    #
    # @example
    #   archived = client.archive_batch("orders", [101, 102, 103])
    def archive_batch(queue_name, msg_ids)
      validate_queue_name!(queue_name)
      return [] if msg_ids.empty?

      # Use PostgreSQL array parameter binding
      result = with_connection do |conn|
        encoder = PG::TextEncoder::Array.new
        encoded_array = encoder.encode(msg_ids)

        conn.exec_params(
          'SELECT * FROM pgmq.archive($1::text, $2::bigint[])',
          [queue_name, encoded_array]
        )
      end

      result.map { |row| row['archive'].to_i }
    end

    # Updates the visibility timeout for a message
    #
    # @param queue_name [String] name of the queue
    # @param msg_id [Integer] message ID
    # @param vt_offset [Integer] visibility timeout offset in seconds
    # @return [PGMQ::Message] updated message
    #
    # @example
    #   # Extend processing time by 60 more seconds
    #   msg = client.set_vt("orders", 123, vt_offset: 60)
    def set_vt(queue_name, msg_id, vt_offset:)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params(
          'SELECT * FROM pgmq.set_vt($1::text, $2::bigint, $3::integer)',
          [queue_name, msg_id, vt_offset]
        )
      end

      return nil if result.ntuples.zero?

      Message.new(result[0], serializer: @serializer)
    end

    # Deletes specific messages from multiple queues in a single transaction
    #
    # Efficiently deletes messages across different queues atomically.
    # Useful when processing related messages from different queues.
    #
    # @param deletions [Hash] hash of queue_name => array of msg_ids
    # @return [Hash] hash of queue_name => array of deleted msg_ids
    #
    # @example Delete messages from multiple queues
    #   client.delete_multi({
    #     'orders' => [1, 2, 3],
    #     'notifications' => [5, 6],
    #     'emails' => [10]
    #   })
    #   # => { 'orders' => [1, 2, 3], 'notifications' => [5, 6], 'emails' => [10] }
    #
    # @example Clean up after batch processing across queues
    #   messages = client.read_multi(['q1', 'q2', 'q3'], qty: 10)
    #   deletions = messages.group_by(&:queue_name).transform_values { |msgs| msgs.map(&:msg_id) }
    #   client.delete_multi(deletions)
    def delete_multi(deletions)
      raise ArgumentError, 'deletions must be a hash' unless deletions.is_a?(Hash)
      return {} if deletions.empty?

      # Validate all queue names
      deletions.each_key { |qn| validate_queue_name!(qn) }

      transaction do |txn|
        result = {}
        deletions.each do |queue_name, msg_ids|
          next if msg_ids.empty?

          deleted_ids = txn.delete_batch(queue_name, msg_ids)
          result[queue_name] = deleted_ids
        end
        result
      end
    end

    # Archives specific messages from multiple queues in a single transaction
    #
    # Efficiently archives messages across different queues atomically.
    #
    # @param archives [Hash] hash of queue_name => array of msg_ids
    # @return [Hash] hash of queue_name => array of archived msg_ids
    #
    # @example Archive messages from multiple queues
    #   client.archive_multi({
    #     'orders' => [1, 2],
    #     'notifications' => [5]
    #   })
    def archive_multi(archives)
      raise ArgumentError, 'archives must be a hash' unless archives.is_a?(Hash)
      return {} if archives.empty?

      # Validate all queue names
      archives.each_key { |qn| validate_queue_name!(qn) }

      transaction do |txn|
        result = {}
        archives.each do |queue_name, msg_ids|
          next if msg_ids.empty?

          archived_ids = txn.archive_batch(queue_name, msg_ids)
          result[queue_name] = archived_ids
        end
        result
      end
    end

    # Purges all messages from a queue
    #
    # @param queue_name [String] name of the queue
    # @return [Integer] number of messages purged
    #
    # @example
    #   count = client.purge_queue("old_queue")
    #   puts "Purged #{count} messages"
    def purge_queue(queue_name)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params('SELECT pgmq.purge_queue($1::text)', [queue_name])
      end

      result[0]['purge_queue'].to_i
    end

    # Gets metrics for a specific queue
    #
    # @param queue_name [String] name of the queue
    # @return [PGMQ::Metrics] metrics object
    #
    # @example
    #   metrics = client.metrics("orders")
    #   puts "Queue length: #{metrics.queue_length}"
    #   puts "Oldest message: #{metrics.oldest_msg_age_sec}s"
    def metrics(queue_name)
      validate_queue_name!(queue_name)

      result = with_connection do |conn|
        conn.exec_params('SELECT * FROM pgmq.metrics($1::text)', [queue_name])
      end

      return nil if result.ntuples.zero?

      Metrics.new(result[0])
    end

    # Gets metrics for all queues
    #
    # @return [Array<PGMQ::Metrics>] array of metrics objects
    #
    # @example
    #   all_metrics = client.metrics_all
    #   all_metrics.each do |m|
    #     puts "#{m.queue_name}: #{m.queue_length} messages"
    #   end
    def metrics_all
      result = with_connection do |conn|
        conn.exec('SELECT * FROM pgmq.metrics_all()')
      end

      result.map { |row| Metrics.new(row) }
    end

    # Detaches the archive table from PGMQ management
    #
    # @param queue_name [String] name of the queue
    # @return [void]
    #
    # @example
    #   client.detach_archive("orders")
    def detach_archive(queue_name)
      validate_queue_name!(queue_name)

      with_connection do |conn|
        conn.exec_params('SELECT pgmq.detach_archive($1::text)', [queue_name])
      end

      nil
    end

    # Closes all connections in the pool
    # @return [void]
    def close
      @connection.close
    end

    # Returns connection pool statistics
    #
    # @return [Hash] statistics about the connection pool
    # @example
    #   stats = client.stats
    #   # => { size: 5, available: 3 }
    def stats
      @connection.stats
    end

    private

    # Executes a block with a database connection
    # @yield [PG::Connection] database connection
    # @return [Object] result of the block
    def with_connection(&)
      @connection.with_connection(&)
    end

    # Validates a queue name
    # @param queue_name [String] queue name to validate
    # @raise [PGMQ::InvalidQueueNameError] if name is invalid
    # @return [void]
    def validate_queue_name!(queue_name)
      raise InvalidQueueNameError, 'Queue name cannot be empty' if queue_name.nil? || queue_name.to_s.strip.empty?

      # PGMQ creates tables with prefixes (pgmq.q_<name>, pgmq.a_<name>)
      # PostgreSQL has a 63-character limit for identifiers, but PGMQ enforces 48
      # to account for prefixes and potential suffixes
      if queue_name.to_s.length >= 48
        raise InvalidQueueNameError,
              "Queue name '#{queue_name}' exceeds maximum length of 48 characters " \
              "(current length: #{queue_name.to_s.length})"
      end

      # PostgreSQL identifier rules: start with letter or underscore,
      # contain only letters, digits, underscores
      return if queue_name.to_s.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)

      raise InvalidQueueNameError,
            "Invalid queue name '#{queue_name}': must start with a letter or underscore " \
            'and contain only letters, digits, and underscores'
    end
  end
end
