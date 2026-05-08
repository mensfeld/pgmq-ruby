# frozen_string_literal: true

describe PGMQ::Connection do
  before { @conn_params = TEST_DB_PARAMS }

  describe "pool statistics" do
    it "provides pool size and available connections" do
      connection = PGMQ::Connection.new(@conn_params, pool_size: 3)

      stats = connection.stats

      assert_equal 3, stats[:size]
      assert_equal 3, stats[:available]

      connection.close
    end

    it "tracks available connections when in use" do
      connection = PGMQ::Connection.new(@conn_params, pool_size: 2)

      stats_before = connection.stats

      assert_equal 2, stats_before[:available]

      # Hold one connection
      connection.with_connection do |_conn|
        stats_during = connection.stats

        assert_equal 1, stats_during[:available]
      end

      # Connection should be returned
      stats_after = connection.stats

      assert_equal 2, stats_after[:available]

      connection.close
    end

    it "is accessible from client" do
      client = PGMQ::Client.new(@conn_params, pool_size: 4)

      stats = client.stats

      assert_equal 4, stats[:size]
      assert_equal 4, stats[:available]

      client.close
    end
  end

  describe "auto-reconnect" do
    it "is enabled by default" do
      client = PGMQ::Client.new(@conn_params)

      # Should not raise when enabled (default)
      client.list_queues

      client.close
    end

    it "can be disabled" do
      client = PGMQ::Client.new(@conn_params, auto_reconnect: false)

      # Should still work normally
      client.list_queues

      client.close
    end
  end

  describe "connection verification" do
    it "verifies connections before use when auto_reconnect enabled" do
      connection = PGMQ::Connection.new(@conn_params, auto_reconnect: true)

      verified = false

      connection.with_connection do |conn|
        # Connection should be verified (not finished)
        verified = !conn.finished?
      end

      assert verified
      connection.close
    end

    it "skips verification when auto_reconnect disabled" do
      connection = PGMQ::Connection.new(@conn_params, auto_reconnect: false)

      # Should work normally without verification
      connection.with_connection { |conn| conn.exec("SELECT 1") }

      connection.close
    end
  end

  describe "connection pool timeout" do
    it "raises error when pool is exhausted" do
      client = PGMQ::Client.new(@conn_params, pool_size: 1, pool_timeout: 0.5)

      # Hold the only connection
      thread = Thread.new do
        client.instance_variable_get(:@connection).with_connection do |_conn|
          sleep 2 # Hold for longer than timeout
        end
      end

      sleep 0.1 # Let thread acquire connection

      # Try to acquire while exhausted
      assert_raises(PGMQ::Errors::ConnectionError) do
        client.list_queues
      end

      thread.join
      client.close
    end
  end

  describe "concurrent access" do
    it "handles multiple threads accessing pool" do
      client = PGMQ::Client.new(@conn_params, pool_size: 5)
      queue = unique_queue_name("concurrent")
      client.create(queue)

      threads = Array.new(10) do |i|
        Thread.new do
          client.produce(queue, to_json_msg({ thread: i }))
        end
      end

      threads.each(&:join)

      # All messages should have been sent
      messages = []
      10.times do
        msg = client.read(queue, vt: 1)
        messages << msg if msg
      end

      assert_equal 10, messages.size

      client.drop_queue(queue)
      client.close
    end

    it "properly returns connections to pool" do
      client = PGMQ::Client.new(@conn_params, pool_size: 2)

      # Use all connections multiple times
      10.times do
        threads = Array.new(2) do
          Thread.new do
            client.list_queues
          end
        end

        threads.each(&:join)
      end

      # Pool should still be healthy
      stats = client.stats

      assert_equal 2, stats[:available]

      client.close
    end
  end

  describe "fiber scheduler compatibility" do
    # ConnectionPool gem is Fiber-aware, so basic fiber usage works
    it "supports fiber-based concurrency" do
      client = PGMQ::Client.new(@conn_params, pool_size: 3)
      queue = unique_queue_name("fiber")
      client.create(queue)

      results = []
      fibers = []

      5.times do |i|
        fibers << Fiber.new do
          client.produce(queue, to_json_msg({ fiber: i }))
          results << i
          Fiber.yield
        end
      end

      # Resume all fibers
      fibers.each(&:resume)

      assert_equal 5, results.size

      client.drop_queue(queue)
      client.close
    end
  end

  describe "shared connection detection" do
    it "raises ConfigurationError when callable returns same connection to multiple slots" do
      shared_conn = PG.connect(TEST_DB_PARAMS)
      connection = PGMQ::Connection.new(-> { shared_conn }, pool_size: 2)

      # Deterministically force pool to create two slots using the same PG::Connection
      errors = []
      ready_queue = Queue.new

      # Hold one connection in a background thread to occupy the first slot
      holder_thread = Thread.new do
        connection.with_connection do |_c|
          ready_queue << :acquired
          sleep 0.5
        end
      rescue PGMQ::Errors::ConfigurationError => e
        errors << e
        ready_queue << :error
      rescue
        # Connection errors from corrupted state are also possible
        ready_queue << :error
      end

      # Wait until the first slot is definitely acquired or an error occurs
      ready_queue.pop

      # Now, from the main thread, force creation of the second slot
      begin
        connection.with_connection { |c| c.exec("SELECT 1") }
      rescue PGMQ::Errors::ConfigurationError => e
        errors << e
      rescue
        # Connection errors from corrupted state are also possible
      end

      holder_thread.join

      assert_predicate errors, :any?, "Expected ConfigurationError for shared connection"
      assert_match(/same PG::Connection object/, errors.first.message)
    ensure
      begin
        connection&.close
      rescue
        nil
      end

      begin
        shared_conn&.close
      rescue
        nil
      end
    end
  end

  describe "connection_lost_error? (private)" do
    let(:connection) { PGMQ::Connection.new(TEST_DB_PARAMS, pool_size: 1) }

    after { connection.close }

    it "matches the pg-gem 'PQsocket' error raised when the cached socket is gone" do
      # pg gem raises this (see pg_connection.c) when the libpq socket
      # descriptor has been closed out from under an open PG::Connection,
      # which happens when a connection pooler such as PgBouncer closes
      # the server link on idle/lifetime timeout.
      error = PG::ConnectionBad.new("PQsocket() can't get socket descriptor")

      assert connection.send(:connection_lost_error?, error)
    end

    it "still matches the pre-existing server-initiated disconnect messages" do
      %w[
        server_closed_the_connection_unexpectedly
        connection_to_server_was_lost
        no_connection_to_the_server
      ].each do |key|
        message = key.tr("_", " ")
        error = PG::ConnectionBad.new(message)

        assert connection.send(:connection_lost_error?, error), "expected match for #{message.inspect}"
      end
    end

    it "does not match unrelated PG errors" do
      error = PG::Error.new("duplicate key value violates unique constraint")

      refute connection.send(:connection_lost_error?, error)
    end

    it "matches SSL-layer EOF raised when libpq's read returns 0 on a dead socket" do
      # Observed in production behind a managed Postgres pooler: an idle
      # connection is torn down at the SSL layer and the next I/O raises
      # this exact message. Semantically identical to "could not receive
      # data from server" (which is already covered), just one layer down.
      error = PG::ConnectionBad.new(
        "PQconsumeInput() SSL error: unexpected eof while reading"
      )

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches SSL SYSCALL teardown errors" do
      error = PG::ConnectionBad.new(
        "SSL SYSCALL error: EOF detected"
      )

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches PG::ConnectionBad by class, even when the message is opaque" do
      # libpq doesn't promise a stable message for every pooler/OS/TLS
      # combination, but it does raise a dedicated class for connection
      # failures. Class-matching catches future message variants without
      # growing the string list.
      error = PG::ConnectionBad.new("")

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches PG::UnableToSend by class" do
      # Raised by libpq when the write side of the connection is gone —
      # another dedicated connection-failure class.
      error = PG::UnableToSend.new("")

      assert connection.send(:connection_lost_error?, error)
    end
  end

  describe "class-level reconnectable error configuration" do
    let(:connection) { PGMQ::Connection.new(TEST_DB_PARAMS, pool_size: 1) }

    before do
      # Save original class-level settings
      @original_patterns = PGMQ::Connection.reconnectable_error_patterns
      @original_classes = PGMQ::Connection.reconnectable_error_classes
    end

    after do
      # Restore class-level settings
      PGMQ::Connection.reconnectable_error_patterns = @original_patterns
      PGMQ::Connection.reconnectable_error_classes = @original_classes
      connection.close
    end

    it "keeps the built-in defaults when no extras are configured" do
      error = PG::ConnectionBad.new("server closed the connection unexpectedly")

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches class-level string patterns as case-insensitive substrings" do
      PGMQ::Connection.reconnectable_error_patterns = ["Connection Reset By Peer"]

      error = PG::Error.new("recv() failed: connection reset by peer")

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches class-level Regexp patterns against the original message" do
      PGMQ::Connection.reconnectable_error_patterns = [/\Abroken pipe\b/i]

      error = PG::Error.new("Broken pipe while writing to server")

      assert connection.send(:connection_lost_error?, error)
    end

    it "still rejects unrelated errors when extra patterns are configured" do
      PGMQ::Connection.reconnectable_error_patterns = ["broken pipe"]

      error = PG::Error.new("duplicate key value violates unique constraint")

      refute connection.send(:connection_lost_error?, error)
    end

    it "matches class-level error classes" do
      stub_class = Class.new(PG::Error)
      PGMQ::Connection.reconnectable_error_classes = [stub_class]

      error = stub_class.new("anything")

      assert connection.send(:connection_lost_error?, error)
    end

    it "matches subclasses of class-level error classes" do
      stub_class = Class.new(PG::Error)
      child_class = Class.new(stub_class)
      PGMQ::Connection.reconnectable_error_classes = [stub_class]

      error = child_class.new("anything")

      assert connection.send(:connection_lost_error?, error)
    end

    it "accepts a single pattern instead of an array" do
      PGMQ::Connection.reconnectable_error_patterns = "broken pipe"

      error = PG::Error.new("broken pipe")

      assert connection.send(:connection_lost_error?, error)
    end

    it "raises ConfigurationError for non-String, non-Regexp patterns" do
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Connection.reconnectable_error_patterns = [123]
      end
    end

    it "raises ConfigurationError for non-Exception classes" do
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Connection.reconnectable_error_classes = [String]
      end
    end

    it "raises ConfigurationError when classes contains a non-Class value" do
      assert_raises(PGMQ::Errors::ConfigurationError) do
        PGMQ::Connection.reconnectable_error_classes = ["PG::Error"]
      end
    end

    it "drives the auto_reconnect retry path for class-level patterns" do
      PGMQ::Connection.reconnectable_error_patterns = ["pretend-pooler-eof"]

      attempts = 0
      result = connection.with_connection do |conn|
        attempts += 1
        raise PG::Error, "pretend-pooler-eof on read" if attempts == 1

        conn.exec("SELECT 1").getvalue(0, 0)
      end

      assert_equal "1", result
      assert_equal 2, attempts, "expected one retry after the class-level pattern match"
    end

    it "reflects runtime changes to class-level settings" do
      error = PG::Error.new("novel-disconnection-error")

      refute connection.send(:connection_lost_error?, error), "should not match before config"

      PGMQ::Connection.reconnectable_error_patterns = ["novel-disconnection-error"]

      assert connection.send(:connection_lost_error?, error), "should match after config"
    end
  end

  describe "verify_connection! (private)" do
    let(:connection) { PGMQ::Connection.new(TEST_DB_PARAMS, pool_size: 1) }

    after { connection.close }

    it "resets connections that are closed client-side (finished? is true)" do
      pg_conn = PG.connect(TEST_DB_PARAMS)
      pg_conn.close

      assert_predicate pg_conn, :finished?

      connection.send(:verify_connection!, pg_conn)

      refute_predicate pg_conn, :finished?, "expected verify_connection! to reset a finished connection"
      assert_equal "1", pg_conn.exec("SELECT 1").getvalue(0, 0)
    ensure
      pg_conn.close if pg_conn && !pg_conn.finished?
    end

    it "resets connections whose status is CONNECTION_BAD without finished? flipping true" do
      pg_conn = PG.connect(TEST_DB_PARAMS)

      # Simulate a server-initiated disconnect (PgBouncer idle timeout, admin
      # kill, TCP RST) where the cached PG::Connection is still alive enough
      # that finished? returns false but status reports CONNECTION_BAD.
      # Return CONNECTION_BAD for the first query only so the subsequent
      # reset (which re-checks status) can succeed.
      pg_conn.stubs(:status).returns(PG::CONNECTION_BAD, PG::CONNECTION_OK)
      pg_conn.expects(:reset).once.returns(pg_conn)

      connection.send(:verify_connection!, pg_conn)
    ensure
      pg_conn.close if pg_conn && !pg_conn.finished?
    end

    it "does not touch healthy connections" do
      pg_conn = PG.connect(TEST_DB_PARAMS)
      pg_conn.expects(:reset).never

      connection.send(:verify_connection!, pg_conn)
    ensure
      pg_conn&.close
    end
  end

  describe "connection lifecycle" do
    it "closes all connections properly" do
      client = PGMQ::Client.new(@conn_params, pool_size: 3)

      # Use connections
      3.times { client.list_queues }

      # Close should work without errors
      client.close

      # Further operations should fail
      assert_raises(PGMQ::Errors::ConnectionError) do
        client.list_queues
      end
    end

    it "handles closing with connections in use" do
      client = PGMQ::Client.new(@conn_params, pool_size: 2)

      thread = Thread.new do
        client.instance_variable_get(:@connection).with_connection do |_conn|
          sleep 0.5
        end
      end

      sleep 0.1 # Let thread acquire connection

      # Close while connection is in use
      client.close

      thread.join
    end
  end
end
