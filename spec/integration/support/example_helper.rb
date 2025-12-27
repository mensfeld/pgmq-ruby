# frozen_string_literal: true

# Shared helper module for all PGMQ examples.
#
# Provides common functionality for database connection, queue management,
# and graceful shutdown handling.

require 'bundler/setup'
require 'pgmq'
require 'json'
require 'securerandom'

module ExampleHelper
  # Database connection parameters matching test configuration
  DB_PARAMS = {
    host: ENV.fetch('PG_HOST', 'localhost'),
    port: ENV.fetch('PG_PORT', 5433).to_i,
    dbname: ENV.fetch('PG_DATABASE', 'pgmq_test'),
    user: ENV.fetch('PG_USER', 'postgres'),
    password: ENV.fetch('PG_PASSWORD', 'postgres')
  }.freeze

  class << self
    # Creates a PGMQ client with test database parameters
    #
    # @param options [Hash] Additional options to merge with DB_PARAMS
    # @return [PGMQ::Client] Configured client instance
    def create_client(**options)
      PGMQ::Client.new(DB_PARAMS.merge(options))
    end

    # Generates a unique queue name to avoid conflicts between runs
    #
    # @param prefix [String] Prefix for the queue name
    # @return [String] Unique queue name
    def unique_queue_name(prefix = 'example')
      "#{prefix}_#{SecureRandom.hex(4)}"
    end

    # Runs an example with proper setup, teardown, and interrupt handling
    #
    # @param name [String] Name of the example for display
    # @yield [client, queues, interrupted] Block containing example code
    # @yieldparam client [PGMQ::Client] Connected client instance
    # @yieldparam queues [Array<String>] Array to track created queues for cleanup
    # @yieldparam interrupted [Proc] Proc that returns true if SIGINT received
    def run_example(name)
      puts '=' * 60
      puts "Example: #{name}"
      puts '=' * 60
      puts

      client = create_client
      queues = []
      interrupted = false

      # Handle graceful shutdown on SIGINT (Ctrl+C)
      original_handler = Signal.trap('INT') do
        puts "\nInterrupted. Cleaning up..."
        interrupted = true
      end

      begin
        yield(client, queues, -> { interrupted })
        puts "\nExample completed successfully."
      rescue StandardError => e
        puts "\nError: #{e.class}: #{e.message}"
        puts e.backtrace.first(5).join("\n") if ENV['DEBUG']
        exit(1)
      ensure
        # Restore original signal handler
        case original_handler
        when String then Signal.trap('INT', original_handler)
        when Proc then Signal.trap('INT', &original_handler)
        else Signal.trap('INT', 'DEFAULT')
        end
        cleanup(client, queues)
        client.close
      end
    end

    # Cleans up queues created during an example
    #
    # @param client [PGMQ::Client] Client to use for cleanup
    # @param queues [Array<String>] Queue names to drop
    def cleanup(client, queues)
      return if queues.empty?

      puts "\nCleaning up #{queues.size} queue(s)..."
      queues.each do |queue_name|
        client.drop_queue(queue_name)
        puts "  Dropped: #{queue_name}"
      rescue StandardError => e
        puts "  Failed to drop #{queue_name}: #{e.message}"
      end
    end

    # Converts a Ruby object to JSON string
    #
    # @param obj [Object] Object to convert (or String to pass through)
    # @return [String] JSON string
    def to_json(obj)
      obj.is_a?(String) ? obj : JSON.generate(obj)
    end

    # Parses message payload from JSON
    #
    # @param msg [PGMQ::Message] Message to parse
    # @return [Hash, Array] Parsed JSON payload
    def parse_message(msg)
      JSON.parse(msg.message)
    end
  end
end
