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
  #   conn = PGMQ::Connection.new({ host: 'localhost', dbname: 'mydb' })
  #
  # @example With Rails ActiveRecord (reuses Rails connection pool)
  #   conn = PGMQ::Connection.new(-> { ActiveRecord::Base.connection.raw_connection })
  class Connection
    # Default connection pool size
    DEFAULT_POOL_SIZE = 5

    # Default connection pool timeout in seconds
    DEFAULT_POOL_TIMEOUT = 5

    # Validates the connection configuration
    CONTRACT = Contracts::Connection.new

    private_constant :CONTRACT

    class << self
      # Additional error message patterns (String or Regexp) that mean the connection is dead and a retry on a fresh
      # socket is safe. Strings are matched as case-insensitive substrings; Regexps match the original message. The
      # built-in LOST_CONNECTION_MESSAGES are always checked first - this list is appended to them.
      #
      # Thread-safe: reads are lock-free (frozen array swap); writes should be done at boot time before forking workers.
      #
      # @return [Array<String, Regexp>]
      # @example
      #   PGMQ::Connection.reconnectable_error_patterns = [
      #     "connection reset by peer",
      #     /\Abroken pipe\b/i
      #   ]
      attr_reader :reconnectable_error_patterns

      # Additional exception classes that mean the connection is dead. `PG::ConnectionBad` and `PG::UnableToSend` are
      # always matched - this list is appended to them. Subclasses also match.
      #
      # Thread-safe: reads are lock-free; writes should be done at boot time.
      #
      # @return [Array<Class>]
      # @example
      #   PGMQ::Connection.reconnectable_error_classes = [PG::ConnectionFailure]
      attr_reader :reconnectable_error_classes

      # Replaces the extra reconnectable error patterns.
      #
      # @param patterns [Array<String, Regexp>]
      # @raise [PGMQ::Errors::ConfigurationError] if any element is invalid
      def reconnectable_error_patterns=(patterns)
        CONTRACT.validate!({ reconnectable_error_patterns: patterns }, Errors::ConfigurationError)

        @reconnectable_error_patterns = normalize_patterns(patterns)
      end

      # Replaces the extra reconnectable error classes.
      #
      # @param classes [Array<Class>]
      # @raise [PGMQ::Errors::ConfigurationError] if any element is invalid
      def reconnectable_error_classes=(classes)
        CONTRACT.validate!({ reconnectable_error_classes: classes }, Errors::ConfigurationError)

        @reconnectable_error_classes = Array(classes).dup.freeze
      end

      private

      # Normalizes user-supplied reconnectable error patterns.
      #
      # Strings are downcased once at configuration time so the hot path (`connection_lost_error?`) only does substring
      # checks. Regexps are passed through unchanged.
      #
      # @param patterns [Array<String, Regexp>, String, Regexp, nil]
      # @return [Array<String, Regexp>]
      def normalize_patterns(patterns)
        Array(patterns).map { |pattern| pattern.is_a?(String) ? pattern.downcase : pattern }.freeze
      end
    end

    # Initialize class-level defaults (empty frozen arrays, users replace them at boot via the setters)
    @reconnectable_error_patterns = [].freeze
    @reconnectable_error_classes = [].freeze

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
      CONTRACT.validate!({ conn_params: conn_params }, Errors::ConfigurationError)

      @conn_params = normalize_connection_params(conn_params)
      @pool_size = pool_size
      @pool_timeout = pool_timeout
      @auto_reconnect = auto_reconnect
      # A callable hands us connections it may share with its owner (e.g. ActiveRecord's raw connection), so we never
      # reset or close them after a failure; only connections we open ourselves are reset.
      @owns_connections = !conn_params.respond_to?(:call)
      @pool = create_pool
    end

    # Executes a block with a connection from the pool
    #
    # When the block raises `PG::ConnectionBad` or `PG::UnableToSend`, the connection is reset before it goes back to
    # the pool, so the next checkout gets a working one. Connections from a callable are left as they are.
    #
    # @yield [PG::Connection] database connection
    # @return [Object] result of the block
    # @raise [PGMQ::Errors::ConnectionError] if the block raises any `PG::Error` (the original is in `#cause`), or a
    #   connection cannot be obtained
    def with_connection
      retries = @auto_reconnect ? 1 : 0
      attempts = 0

      begin
        @pool.with do |conn|
          # Health check: verify connection is alive
          verify_connection!(conn) if @auto_reconnect

          begin
            yield conn
          rescue *LOST_CONNECTION_ERRORS
            reset_lost_connection(conn) if @owns_connections
            raise
          end
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

    # Drops every connection currently in the pool and lets the pool build fresh ones lazily on the next checkout.
    # Unlike {#close} (which shuts the pool down permanently), the pool stays usable afterwards.
    #
    # Use this to recover from a connection that libpq still reports as +CONNECTION_OK+ but which is in fact wedged -
    # e.g. after a wall-clock timeout (+Timeout.timeout+) interrupted a query mid-flight, leaving the socket poisoned so
    # it re-hangs on reuse. {#verify_connection!} cannot catch that case (the connection still reports
    # +CONNECTION_OK+), so the caller must discard it explicitly; +reload+ is the safe, pool-wide way to do so.
    #
    # @return [void]
    # @example Discard the pool after a query had to be force-interrupted
    #   begin
    #     Timeout.timeout(5) { connection.with_connection { |conn| conn.exec("SELECT ...") } }
    #   rescue Timeout::Error
    #     connection.reload # drop the possibly-poisoned pooled connections
    #     raise
    #   end
    def reload
      @pool.reload { |conn| conn.close unless conn.finished? }
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

    # libpq's dedicated connection-failure classes. A pooled connection that raises one of them mid-operation is reset.
    LOST_CONNECTION_ERRORS = [PG::ConnectionBad, PG::UnableToSend].freeze
    private_constant :LOST_CONNECTION_ERRORS

    # Messages libpq raises when the server/pooler has already torn down the socket. The list has grown organically with
    # each pooler/TLS variant we see in the wild; the class check below catches future variants that libpq raises as
    # `PG::ConnectionBad` or `PG::UnableToSend` without waiting for a new message to hit production.
    LOST_CONNECTION_MESSAGES = [
      "server closed the connection",
      "connection not open",
      "connection is closed",
      "connection has been closed",
      "no connection to the server",
      "terminating connection",
      "connection to server was lost",
      "could not receive data from server",
      "pqsocket() can't get socket descriptor",
      "ssl error: unexpected eof",
      "ssl syscall error"
    ].freeze
    private_constant :LOST_CONNECTION_MESSAGES

    # Checks if the error indicates a lost connection.
    #
    # Matches in three steps: first by class (`PG::ConnectionBad` / `PG::UnableToSend` are dedicated connection-failure
    # classes libpq raises regardless of message, plus any user-supplied classes), then by built-in message substrings
    # for the bare `PG::Error` cases where libpq doesn't reach for the specific subclass, and finally by user-supplied
    # patterns (strings matched as case-insensitive substrings, Regexps matched against the original message).
    #
    # @param error [PG::Error] the error to check
    # @return [Boolean] true if the connection was lost and a retry on a
    #   fresh connection is appropriate
    def connection_lost_error?(error)
      return true if LOST_CONNECTION_ERRORS.any? { |klass| error.is_a?(klass) }

      extra_classes = self.class.reconnectable_error_classes
      return true if extra_classes.any? { |klass| error.is_a?(klass) }

      original_message = error.message.to_s
      downcased = original_message.downcase

      return true if LOST_CONNECTION_MESSAGES.any? { |pattern| downcased.include?(pattern) }

      self.class.reconnectable_error_patterns.any? do |pattern|
        case pattern
        when Regexp then pattern.match?(original_message)
        else downcased.include?(pattern)
        end
      end
    end

    # Verifies a connection is alive and working.
    #
    # Also resets when the connection reports any status other than `PG::CONNECTION_OK`: `CONNECTION_BAD` when the
    # server (or an intermediate pooler such as PgBouncer) closed the socket while the client-side `PG::Connection`
    # object still exists, and an intermediate state (`CONNECTION_STARTED`, `CONNECTION_MADE`, ...) when a previous
    # reset or connect was interrupted before it completed. libpq refuses to send on either. `#finished?` alone only
    # catches connections closed explicitly from the client side.
    #
    # @param conn [PG::Connection] connection to verify
    # @raise [PG::Error] if the reset itself fails
    def verify_connection!(conn)
      return conn.reset if conn.finished?
      return conn.reset unless conn.status == PG::CONNECTION_OK

      nil
    end

    # Resets a connection that failed mid-operation so it does not go back to the pool broken. A connection can pass
    # {#verify_connection!} at checkout (`CONNECTION_OK`) and still fail on the next send.
    #
    # A failing reset must not replace the error the caller sees, so it is swallowed and the connection is closed
    # instead. With `auto_reconnect`, the next checkout sees it as `finished?` and resets it. Without it, the next
    # operation on it fails with `PG::ConnectionBad` ("connection is closed"), and that failure resets it.
    #
    # @param conn [PG::Connection] connection the failed operation used
    # @return [void]
    def reset_lost_connection(conn)
      conn.reset
    rescue
      conn.close unless conn.finished?
    end

    # Normalizes various connection parameter formats
    # @param params [String, Hash, Proc]
    # @return [Hash, Proc]
    # @raise [PGMQ::Errors::ConfigurationError] if the connection string is invalid
    def normalize_connection_params(params)
      return parse_connection_string(params) if params.is_a?(String)

      params
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

        # Detect shared connections: if a callable returns the same PG::Connection object to multiple pool slots,
        # concurrent use will corrupt libpq state (nil results, segfaults, wrong data). Fail fast with a clear message.
        if conn.is_a?(PG::Connection)
          seen_mutex.synchronize do
            if seen_connections.key?(conn)
              raise PGMQ::Errors::ConfigurationError,
                "Connection callable returned the same PG::Connection object " \
                "(object_id: #{conn.object_id}) to multiple pool slots. " \
                "PG::Connection is NOT thread-safe - concurrent use causes nil results, " \
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

      # Create new connection from parameters.
      # Low-level library: return all values as strings from PostgreSQL.
      # No automatic type conversion - let higher-level frameworks handle parsing.
      # conn.type_map_for_results intentionally NOT set.
      PG.connect(params[:conninfo] || params)
    rescue PG::Error => e
      raise PGMQ::Errors::ConnectionError, "Failed to connect to database: #{e.message}"
    end
  end
end
