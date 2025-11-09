# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Connection pooling features' do
  let(:conn_params) { TEST_DB_PARAMS }

  describe 'pool statistics' do
    it 'provides pool size and available connections' do
      connection = PGMQ::Connection.new(conn_params, pool_size: 3)

      stats = connection.stats

      expect(stats[:size]).to eq(3)
      expect(stats[:available]).to eq(3)

      connection.close
    end

    it 'tracks available connections when in use' do
      connection = PGMQ::Connection.new(conn_params, pool_size: 2)

      stats_before = connection.stats
      expect(stats_before[:available]).to eq(2)

      # Hold one connection
      connection.with_connection do |_conn|
        stats_during = connection.stats
        expect(stats_during[:available]).to eq(1)
      end

      # Connection should be returned
      stats_after = connection.stats
      expect(stats_after[:available]).to eq(2)

      connection.close
    end

    it 'is accessible from client' do
      client = PGMQ::Client.new(conn_params, pool_size: 4)

      stats = client.stats

      expect(stats[:size]).to eq(4)
      expect(stats[:available]).to eq(4)

      client.close
    end
  end

  describe 'auto-reconnect' do
    it 'is enabled by default' do
      client = PGMQ::Client.new(conn_params)

      # Should not raise when enabled (default)
      expect { client.list_queues }.not_to raise_error

      client.close
    end

    it 'can be disabled' do
      client = PGMQ::Client.new(conn_params, auto_reconnect: false)

      # Should still work normally
      expect { client.list_queues }.not_to raise_error

      client.close
    end
  end

  describe 'connection verification' do
    it 'verifies connections before use when auto_reconnect enabled' do
      connection = PGMQ::Connection.new(conn_params, auto_reconnect: true)

      verified = false

      connection.with_connection do |conn|
        # Connection should be verified (not finished)
        verified = !conn.finished?
      end

      expect(verified).to be true
      connection.close
    end

    it 'skips verification when auto_reconnect disabled' do
      connection = PGMQ::Connection.new(conn_params, auto_reconnect: false)

      # Should work normally without verification
      expect do
        connection.with_connection { |conn| conn.exec('SELECT 1') }
      end.not_to raise_error

      connection.close
    end
  end

  describe 'connection pool timeout' do
    it 'raises error when pool is exhausted' do
      client = PGMQ::Client.new(conn_params, pool_size: 1, pool_timeout: 0.5)

      # Hold the only connection
      Thread.new do
        client.instance_variable_get(:@connection).with_connection do |_conn|
          sleep 2 # Hold for longer than timeout
        end
      end

      sleep 0.1 # Let thread acquire connection

      # Try to acquire while exhausted
      expect do
        client.list_queues
      end.to raise_error(PGMQ::ConnectionError, /pool timeout/)

      client.close
    end
  end

  describe 'concurrent access' do
    it 'handles multiple threads accessing pool' do
      client = PGMQ::Client.new(conn_params, pool_size: 5)
      queue = test_queue_name('concurrent')
      client.create(queue)

      threads = Array.new(10) do |i|
        Thread.new do
          client.send(queue, { thread: i })
        end
      end

      threads.each(&:join)

      # All messages should have been sent
      messages = []
      10.times do
        msg = client.read(queue, vt: 1)
        messages << msg if msg
      end

      expect(messages.size).to eq(10)

      client.drop_queue(queue)
      client.close
    end

    it 'properly returns connections to pool' do
      client = PGMQ::Client.new(conn_params, pool_size: 2)

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
      expect(stats[:available]).to eq(2)

      client.close
    end
  end

  describe 'fiber scheduler compatibility' do
    # ConnectionPool gem is Fiber-aware, so basic fiber usage works
    it 'supports fiber-based concurrency' do
      client = PGMQ::Client.new(conn_params, pool_size: 3)
      queue = test_queue_name('fiber')
      client.create(queue)

      results = []
      fibers = []

      5.times do |i|
        fibers << Fiber.new do
          client.send(queue, { fiber: i })
          results << i
          Fiber.yield
        end
      end

      # Resume all fibers
      fibers.each(&:resume)

      expect(results.size).to eq(5)

      client.drop_queue(queue)
      client.close
    end
  end

  describe 'connection lifecycle' do
    it 'closes all connections properly' do
      client = PGMQ::Client.new(conn_params, pool_size: 3)

      # Use connections
      3.times { client.list_queues }

      # Close should work without errors
      expect { client.close }.not_to raise_error

      # Further operations should fail
      expect { client.list_queues }.to raise_error(PGMQ::ConnectionError)
    end

    it 'handles closing with connections in use' do
      client = PGMQ::Client.new(conn_params, pool_size: 2)

      thread = Thread.new do
        client.instance_variable_get(:@connection).with_connection do |_conn|
          sleep 0.5
        end
      end

      sleep 0.1 # Let thread acquire connection

      # Close while connection is in use
      expect { client.close }.not_to raise_error

      thread.join
    end
  end
end
