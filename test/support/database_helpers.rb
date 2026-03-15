# frozen_string_literal: true

require "securerandom"

module DatabaseHelpers
  # Creates a test client with test database connection
  def create_test_client(**options)
    PGMQ::Client.new(TEST_DB_PARAMS.merge(options))
  end

  # Generates a unique test queue name
  def unique_queue_name(suffix = nil)
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

      raise "Timeout waiting for condition" if Time.now - start_time > timeout

      sleep 0.1
    end
  end

  # Returns the PGMQ extension version as an array [major, minor, patch]
  def pgmq_version
    client = create_test_client
    result = client.connection.with_connection do |conn|
      conn.exec("SELECT extversion FROM pg_extension WHERE extname = 'pgmq'")
    end
    return nil if result.ntuples.zero?

    version_str = result[0]["extversion"]
    # Parse version string like "v1.9.0" or "1.9.0"
    version_str.gsub(/^v/, "").split(".").map(&:to_i)
  rescue
    nil
  ensure
    client&.close
  end

  # Checks if PGMQ version supports v1.11.0+ features (set_vt timestamp, topic routing)
  def pgmq_supports_v1_11_features?
    version = pgmq_version
    return false unless version

    # v1.11.0 or higher
    version[0] > 1 || (version[0] == 1 && version[1] >= 11)
  end

  # Alias for readability in different contexts
  alias_method :pgmq_supports_set_vt_timestamp?, :pgmq_supports_v1_11_features?
  alias_method :pgmq_supports_topic_routing?, :pgmq_supports_v1_11_features?

  # Standard before block: creates a test client with a single queue
  def setup_client_and_queue(suffix = nil)
    @client = create_test_client
    @queue_name = unique_queue_name(suffix)
    ensure_test_queue(@client, @queue_name)
  end

  # Standard after block: drops the queue and closes the client
  def teardown_client_and_queue
    @client.drop_queue(@queue_name)
  rescue
    nil
  ensure
    @client.close
  end
end
