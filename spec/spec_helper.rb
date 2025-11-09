# frozen_string_literal: true

# SimpleCov must be loaded before application code
require 'simplecov'

SimpleCov.start do
  add_filter '/spec/'
  add_filter '/examples/'
  add_filter '/vendor/'

  add_group 'Core', 'lib/pgmq/client.rb'
  add_group 'Models', ['lib/pgmq/message.rb', 'lib/pgmq/metrics.rb', 'lib/pgmq/queue_metadata.rb']
  add_group 'Infrastructure', 'lib/pgmq/connection.rb'
  add_group 'Serializers', 'lib/pgmq/serializers'

  minimum_coverage 80
  minimum_coverage_by_file 70
end

require 'pgmq'
require 'pry'

# Database connection parameters for testing
# Uses port 5433 by default to avoid conflicts with existing PostgreSQL installations
TEST_DB_PARAMS = {
  host: ENV.fetch('PG_HOST', 'localhost'),
  port: ENV.fetch('PG_PORT', 5433).to_i,
  dbname: ENV.fetch('PG_DATABASE', 'pgmq_test'),
  user: ENV.fetch('PG_USER', 'postgres'),
  password: ENV.fetch('PG_PASSWORD', 'postgres')
}.freeze

# Support files
Dir[File.join(__dir__, 'support', '**', '*.rb')].each { |f| require f }

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

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
    @test_client.drop_queue(queue.queue_name) if queue.queue_name.start_with?('test_')
  rescue StandardError
    # Ignore errors during cleanup
  end
rescue StandardError
  # Ignore errors if connection is already closed
end
