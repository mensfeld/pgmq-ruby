# frozen_string_literal: true

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
    # Include functional modules (order matters for discoverability)
    include Transaction          # Transaction support (already existed)
    include QueueManagement      # Queue lifecycle (create, drop, list)
    include Producer             # Message sending operations
    include Consumer             # Single-queue reading operations
    include MultiQueue           # Multi-queue operations
    include MessageLifecycle     # Message state transitions (pop, delete, archive)
    include Maintenance          # Queue maintenance (purge, detach_archive)
    include Metrics              # Monitoring and metrics

    # Default visibility timeout in seconds
    DEFAULT_VT = 30

    # @return [PGMQ::Connection] the connection manager
    attr_reader :connection

    # Creates a new PGMQ client
    #
    # @param conn_params [String, Hash, Proc, PGMQ::Connection, nil] connection parameters
    # @param pool_size [Integer] connection pool size
    # @param pool_timeout [Integer] connection pool timeout in seconds
    # @param auto_reconnect [Boolean] automatically reconnect on connection errors (default: true)
    #
    # @example Connection string
    #   client = PGMQ::Client.new('postgres://user:pass@localhost/db')
    #
    # @example Connection hash
    #   client = PGMQ::Client.new(host: 'localhost', dbname: 'mydb', user: 'postgres')
    #
    # @example Inject existing connection (for Rails)
    #   client = PGMQ::Client.new(-> { ActiveRecord::Base.connection.raw_connection })
    #
    # @example Disable auto-reconnect
    #   client = PGMQ::Client.new(auto_reconnect: false)
    def initialize(
      conn_params = nil,
      pool_size: Connection::DEFAULT_POOL_SIZE,
      pool_timeout: Connection::DEFAULT_POOL_TIMEOUT,
      auto_reconnect: true
    )
      @connection = if conn_params.is_a?(Connection)
                      conn_params
                    else
                      Connection.new(
                        conn_params,
                        pool_size: pool_size,
                        pool_timeout: pool_timeout,
                        auto_reconnect: auto_reconnect
                      )
                    end
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
    # @return [void]
    # @raise [PGMQ::Errors::InvalidQueueNameError] if name is invalid
    def validate_queue_name!(queue_name)
      raise Errors::InvalidQueueNameError, 'Queue name cannot be empty' if queue_name.nil? || queue_name.to_s.strip.empty?

      # PGMQ creates tables with prefixes (pgmq.q_<name>, pgmq.a_<name>)
      # PostgreSQL has a 63-character limit for identifiers, but PGMQ enforces 48
      # to account for prefixes and potential suffixes
      if queue_name.to_s.length >= 48
        raise Errors::InvalidQueueNameError,
              "Queue name '#{queue_name}' exceeds maximum length of 48 characters " \
              "(current length: #{queue_name.to_s.length})"
      end

      # PostgreSQL identifier rules: start with letter or underscore,
      # contain only letters, digits, underscores
      return if queue_name.to_s.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)

      raise Errors::InvalidQueueNameError,
            "Invalid queue name '#{queue_name}': must start with a letter or underscore " \
            'and contain only letters, digits, and underscores'
    end
  end
end
