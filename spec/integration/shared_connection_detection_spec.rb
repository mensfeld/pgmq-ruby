# frozen_string_literal: true

# Example: Shared Connection Detection
#
# Verifies that PGMQ raises ConfigurationError when a callable returns the
# same PG::Connection to multiple pool slots. This is a deterministic test:
# hold one connection, then force a second slot creation from another thread.
#

ExampleHelper.run_example("Shared Connection Detection") do |_client, _queues, _interrupted|
  shared_conn = PG.connect(ExampleHelper::DB_PARAMS)
  unsafe_client = PGMQ::Client.new(-> { shared_conn }, pool_size: 2, pool_timeout: 5)

  puts "Created client with callable returning same PG::Connection (pool_size: 2)"

  error = nil

  # Hold the first pool slot, forcing the pool to create a second one
  unsafe_client.connection.with_connection do |_held|
    thread = Thread.new do
      unsafe_client.list_queues
    rescue PGMQ::Errors::ConfigurationError => e
      error = e
    end
    thread.join
  end

  raise "Expected ConfigurationError but none was raised" unless error
  raise "Unexpected message: #{error.message}" unless error.message.include?("same PG::Connection object")

  puts "ConfigurationError raised: #{error.message[0..80]}..."

  # Verify safe pattern does not raise
  safe_client = ExampleHelper.create_client(pool_size: 2)
  results = []

  safe_client.connection.with_connection do |_held|
    thread = Thread.new do
      results << safe_client.list_queues
    end
    thread.join
  end

  raise "Safe pattern should not raise" if results.empty?

  puts "Safe pattern works: no error with separate connections"

  safe_client.close
ensure
  begin
    unsafe_client&.close
  rescue
    nil
  end

  begin
    shared_conn&.close
  rescue
    nil
  end
end
