# frozen_string_literal: true

require 'securerandom'

module DatabaseHelpers
  # Creates a test client with test database connection
  def create_test_client(**options)
    # JRuby needs smaller pool sizes to avoid exhausting connections
    if RUBY_PLATFORM == 'java' && !options.key?(:pool_size)
      options[:pool_size] = 2
    end
    PGMQ::Client.new(TEST_DB_PARAMS.merge(options))
  end

  # Generates a unique test queue name
  def test_queue_name(suffix = nil)
    name = "test_queue_#{SecureRandom.hex(4)}"
    name += "_#{suffix}" if suffix
    name
  end

  # Ensures a queue exists for testing
  def ensure_test_queue(client, queue_name)
    client.create(queue_name)
  rescue PG::DuplicateTable
    # Queue already exists, that's fine
  end

  # Waits for a condition to be true
  def wait_for(timeout: 5, &block)
    start_time = Time.now
    loop do
      return true if block.call

      raise 'Timeout waiting for condition' if Time.now - start_time > timeout

      sleep 0.1
    end
  end

  # Checks if PostgreSQL with PGMQ extension is available
  def pgmq_available?
    client = create_test_client
    result = client.connection.with_connection do |conn|
      conn.exec("SELECT 1 FROM pg_extension WHERE extname = 'pgmq'")
    end
    result.ntuples.positive?
  rescue StandardError => e
    warn "\n⚠️  PGMQ extension not available: #{e.message}"
    warn "   Run 'docker compose up -d' to start PostgreSQL with PGMQ extension"
    false
  ensure
    client&.close
  end

  # Ensures test database and extension exist
  def setup_test_database
    # Try to create extension if database exists

    client = create_test_client
    client.connection.with_connection do |conn|
      conn.exec('CREATE EXTENSION IF NOT EXISTS pgmq CASCADE')
    end
    true
  rescue PG::UndefinedFile
    warn "\n⚠️  PGMQ extension not installed in PostgreSQL"
    warn '   Use Docker: docker compose up -d'
    warn '   Or install PGMQ extension manually'
    false
  rescue StandardError => e
    warn "\n⚠️  Could not setup test database: #{e.message}"
    false
  ensure
    client&.close
  end
end

RSpec.configure do |config|
  config.include DatabaseHelpers

  # Skip integration tests if PGMQ is not available
  config.before(:each, :integration) do
    unless pgmq_available?
      skip "PGMQ extension not available. Run 'docker compose up -d' to start PostgreSQL with PGMQ."
    end
  end
end
