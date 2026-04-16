# frozen_string_literal: true

require "pg"
require "connection_pool"

module PGMQ
  # Manages database connections for PGMQ
  #
  # Supports multiple connection strategies:
  # - Connection strings
  # - Hash of connection parameters
  # - Callable objects (for Rails ActiveRecord integration)
  #
  # @example With connection string
  #   conn = PGMQ::Connection.new("postgres://localhost/mydb")
  #
  # @example With connection hash
  #   conn = PGMQ::Connection.new(host: 'localhost', dbname: 'mydb')
  #
  # @example With Rails ActiveRecord (reuses Rails connection pool)
  #   conn = PGMQ::Connection.new(-> { ActiveRecord::Base.connection.raw_connection })
  class Connection
    # Default connection pool size
    DEFAULT_POOL_SIZE = 5

    # Default connection pool timeout in seconds
    DEFAULT_POOL_TIMEOUT = 5

    # @return [ConnectionPool] the connection pool
    attr_reader :pool

    # Creates a new connection manager
    #
    # @param conn_params [String, Hash, Proc] connection parameters or callable
    # @param pool_size [Integer] size of the connection pool
    # @param pool_timeout [Integer] connection pool timeout in seconds
    # @param auto_reconnect [Boolean] automatically reconnect on connection errors
    # @raise [PGMQ::Errors::ConfigurationError] if conn_params is nil or invalid
    def initialize(
      conn_params,
      pool_size: DEFAULT_POOL_SIZE,
      pool_timeout: DEFAULT_POOL_TIMEOUT,
      auto_reconnect: true
    )
      if conn_params.nil?
        raise(
          PGMQ::Errors::ConfigurationError,
          "Connection parameters are required"
        )
      end

      @conn_params = normalize_connection_params(conn_params)
      @pool_size = pool_size
      @pool_timeout = pool_timeout
      @auto_reconnect = auto_reconnect
      @pool = create_pool
    end

    # Executes a block with a connection from the pool
    #
    # @yield [PG::Connection] database connection
    # @return [Object] result of the block
    # @raise [PGMQ::Errors::ConnectionError] if connection fails
    def with_connection
      retries = @auto_reconnect ? 1 : 0
      attempts = 0

      begin
        @pool.with do |conn|
          # Health check: verify connection is alive
          verify_connection!(conn) if @auto_reconnect

          yield conn
        end
      rescue PG::Error => e
        attempts += 1

        # If connection error and auto_reconnect enabled, try once more
        retry if attempts <= retries && connection_lost_error?(e)

        raise PGMQ::Errors::ConnectionError, "Database connection error: #{e.message}"
      rescue ConnectionPool::TimeoutError => e
        raise PGMQ::Errors::ConnectionError, "Connection pool timeout: #{e.message}"
      rescue ConnectionPool::PoolShuttingDownError => e
        raise PGMQ::Errors::ConnectionError, "Connection pool is closed: #{e.message}"
      end
    end

    # Closes all connections in the pool
    # @return [void]
    def close
      @pool.shutdown { |conn| conn.close unless conn.finished? }
    end

    # Returns connection pool statistics
    #
    # @return [Hash] statistics about the connection pool
    # @example
    #   stats = connection.stats
    #   # => { size: 5, available: 3 }
    def stats
      {
        size: @pool_size,
        available: @pool.available
      }
    end

    private

    # Checks if the error indicates a lost connection
    # @param error [PG::Error] the error to check
    # @return [Boolean] true if connection was lost
    def connection_lost_error?(error)
      # Common connection lost errors. Include the pg-gem C-extension message
      # ("PQsocket() can't get socket descriptor") that is raised when the
      # cached libpq socket descriptor is gone — e.g. after a server-side
      # close by a connection pooler such as PgBouncer.
      lost_connection_messages = [
        "server closed the connection",
        "connection not open",
        "connection is closed",
        "connection has been closed",
        "no connection to the server",
        "terminating connection",
        "connection to server was lost",
        "could not receive data from server",
        "pqsocket() can't get socket descriptor"
      ]

      message = error.message.to_s.downcase
      lost_connection_messages.any? { |pattern| message.include?(pattern) }
    end

    # Verifies a connection is alive and working.
    #
    # Also resets when the connection reports `PG::CONNECTION_BAD`, which
    # happens when the server (or an intermediate pooler such as PgBouncer)
    # has closed the socket while the client-side `PG::Connection` object
    # still exists. `#finished?` alone only catches connections closed
    # explicitly from the client side.
    #
    # @param conn [PG::Connection] connection to verify
    # @raise [PG::Error] if the reset itself fails
    def verify_connection!(conn)
      return conn.reset if conn.finished?
      return conn.reset if conn.status == PG::CONNECTION_BAD

      nil
    end

    # Normalizes various connection parameter formats
    # @param params [String, Hash, Proc]
    # @return [Hash, Proc]
    # @raise [PGMQ::Errors::ConfigurationError] if params format is invalid
    def normalize_connection_params(params)
      return params if params.respond_to?(:call) # Callable (e.g., proc for Rails)
      return parse_connection_string(params) if params.is_a?(String)
      return params if params.is_a?(Hash) && !params.empty?

      raise PGMQ::Errors::ConfigurationError, "Invalid connection parameters format"
    end

    # Parses a PostgreSQL connection string
    # @param conn_string [String] connection string (e.g., "postgres://user:pass@host/db")
    # @return [Hash] connection parameters
    def parse_connection_string(conn_string)
      # PG::Connection.conninfo_parse is available in pg >= 0.20
      if PG::Connection.respond_to?(:conninfo_parse)
        PG::Connection.conninfo_parse(conn_string).each_with_object({}) do |info, hash|
          hash[info[:keyword].to_sym] = info[:val] if info[:val]
        end
      else
        # Fallback: pass the string directly and let PG handle it
        { conninfo: conn_string }
      end
    rescue PG::Error => e
      raise PGMQ::Errors::ConfigurationError, "Invalid connection string: #{e.message}"
    end

    # Creates the connection pool
    # @return [ConnectionPool]
    def create_pool
      params = @conn_params
      seen_connections = ObjectSpace::WeakKeyMap.new
      seen_mutex = Mutex.new

      ConnectionPool.new(size: @pool_size, timeout: @pool_timeout) do
        conn = create_connection(params)

        # Detect shared connections: if a callable returns the same PG::Connection
        # object to multiple pool slots, concurrent use will corrupt libpq state
        # (nil results, segfaults, wrong data). Fail fast with a clear message.
        if conn.is_a?(PG::Connection)
          seen_mutex.synchronize do
            if seen_connections.key?(conn)
              raise PGMQ::Errors::ConfigurationError,
                "Connection callable returned the same PG::Connection object " \
                "(object_id: #{conn.object_id}) to multiple pool slots. " \
                "PG::Connection is NOT thread-safe — concurrent use causes nil results, " \
                "segfaults, and data corruption. Ensure your callable returns a unique " \
                "PG::Connection instance on each invocation (for example, by calling " \
                "PG.connect inside the callable)."
            end

            seen_connections[conn] = true
          end
        end

        conn
      end
    rescue => e
      raise PGMQ::Errors::ConnectionError, "Failed to create connection pool: #{e.message}"
    end

    # Creates a single database connection
    # @param params [Hash, Proc] connection parameters or callable
    # @return [PG::Connection]
    def create_connection(params)
      # If we have a callable (e.g., for Rails), call it to get the connection
      return params.call if params.respond_to?(:call)

      # Create new connection from parameters
      # Low-level library: return all values as strings from PostgreSQL
      # No automatic type conversion - let higher-level frameworks handle parsing
      # conn.type_map_for_results intentionally NOT set
      PG.connect(params[:conninfo] || params)
    rescue PG::Error => e
      raise PGMQ::Errors::ConnectionError, "Failed to connect to database: #{e.message}"
    end
  end
end
