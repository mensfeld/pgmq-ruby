# frozen_string_literal: true

require 'pg'
require 'connection_pool'

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
    # Default environment variables for connection parameters
    ENV_VARS = {
      host: 'PG_HOST',
      port: 'PG_PORT',
      dbname: 'PG_DATABASE',
      user: 'PG_USER',
      password: 'PG_PASSWORD'
    }.freeze

    # Default connection pool size
    DEFAULT_POOL_SIZE = 5

    # Default connection pool timeout in seconds
    DEFAULT_POOL_TIMEOUT = 5

    # @return [ConnectionPool] the connection pool
    attr_reader :pool

    # Creates a new connection manager
    #
    # @param conn_params [String, Hash, Proc, nil] connection parameters or callable
    # @param pool_size [Integer] size of the connection pool
    # @param pool_timeout [Integer] connection pool timeout in seconds
    # @param auto_reconnect [Boolean] automatically reconnect on connection errors
    def initialize(conn_params = nil, pool_size: DEFAULT_POOL_SIZE, pool_timeout: DEFAULT_POOL_TIMEOUT,
                   auto_reconnect: true)
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
    # @raise [PGMQ::ConnectionError] if connection fails
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

        raise PGMQ::ConnectionError, "Database connection error: #{e.message}"
      rescue ConnectionPool::TimeoutError => e
        raise PGMQ::ConnectionError, "Connection pool timeout: #{e.message}"
      rescue ConnectionPool::PoolShuttingDownError => e
        raise PGMQ::ConnectionError, "Connection pool is closed: #{e.message}"
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
      # Common connection lost errors
      lost_connection_messages = [
        'server closed the connection',
        'connection not open',
        'no connection to the server',
        'terminating connection',
        'connection to server was lost',
        'could not receive data from server'
      ]

      message = error.message.downcase
      lost_connection_messages.any? { |pattern| message.include?(pattern) }
    end

    # Verifies a connection is alive and working
    # @param conn [PG::Connection] connection to verify
    # @raise [PG::Error] if connection is not working
    def verify_connection!(conn)
      # Quick check - is connection object in bad state?
      return unless conn.finished?

      # Connection is finished/closed, try to reset it
      conn.reset
    end

    # Normalizes various connection parameter formats
    # @param params [String, Hash, Proc, nil]
    # @return [Hash, Proc]
    def normalize_connection_params(params)
      return params if params.respond_to?(:call) # Callable (e.g., proc for Rails)
      return parse_connection_string(params) if params.is_a?(String)
      return params if params.is_a?(Hash) && !params.empty?

      # No params provided - use ENV vars
      connection_params_from_env
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
      raise PGMQ::ConfigurationError, "Invalid connection string: #{e.message}"
    end

    # Builds connection parameters from environment variables
    # @return [Hash] connection parameters
    def connection_params_from_env
      ENV_VARS.each_with_object({}) do |(key, env_var), params|
        value = ENV.fetch(env_var, nil)
        params[key] = value if value
      end
    end

    # Creates the connection pool
    # @return [ConnectionPool]
    def create_pool
      params = @conn_params

      ConnectionPool.new(size: @pool_size, timeout: @pool_timeout) do
        create_connection(params)
      end
    rescue StandardError => e
      raise PGMQ::ConnectionError, "Failed to create connection pool: #{e.message}"
    end

    # Creates a single database connection
    # @param params [Hash, Proc] connection parameters or callable
    # @return [PG::Connection]
    def create_connection(params)
      # If we have a callable (e.g., for Rails), call it to get the connection
      return params.call if params.respond_to?(:call)

      # Create new connection from parameters
      conn = PG.connect(params[:conninfo] || params)

      # Set some sensible defaults for type mapping
      conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)

      conn
    rescue PG::Error => e
      raise PGMQ::ConnectionError, "Failed to connect to database: #{e.message}"
    end
  end
end
