# frozen_string_literal: true

# SimpleCov must be loaded before application code
require "simplecov"

SimpleCov.start do
  add_filter "/spec/"
  add_filter "/examples/"
  add_filter "/vendor/"

  minimum_coverage 96.5
end

require "pgmq"
require "json" # Tests need JSON for serialization (user responsibility)

# Database connection parameters for testing
# Uses port 5433 by default to avoid conflicts with existing PostgreSQL installations
TEST_DB_PARAMS = {
  host: ENV.fetch("PG_HOST", "localhost"),
  port: ENV.fetch("PG_PORT", 5433).to_i,
  dbname: ENV.fetch("PG_DATABASE", "pgmq_test"),
  user: ENV.fetch("PG_USER", "postgres"),
  password: ENV.fetch("PG_PASSWORD", "postgres")
}.freeze

# Helper to convert Ruby objects to JSON strings (user responsibility in real apps)
module JSONHelpers
  def to_json_msg(obj)
    obj.is_a?(String) ? obj : JSON.generate(obj)
  end
end

# Support files
Dir[File.join(__dir__, "support", "**", "*.rb")].each { |f| require f }

RSpec.configure do |config|
  config.disable_monkey_patching!

  # Include JSON helper in all tests
  config.include JSONHelpers

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # Run specs in random order to surface order dependencies
  config.order = :random
  Kernel.srand config.seed

  # Clean up any test queues after each test
  config.after do
    cleanup_test_queues
  end
end

# Helper to clean up test queues
def cleanup_test_queues
  return unless defined?(@test_client)

  @test_client&.list_queues&.each do |queue|
    @test_client.drop_queue(queue.queue_name) if queue.queue_name.start_with?("test_")
  rescue
    # Ignore errors during cleanup
  end
rescue
  # Ignore errors if connection is already closed
end
