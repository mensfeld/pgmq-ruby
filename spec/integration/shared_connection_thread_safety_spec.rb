# frozen_string_literal: true

# Example: Shared Connection Thread Safety
#
# Demonstrates that PGMQ detects when a callable returns the same
# PG::Connection object to multiple pool slots and raises ConfigurationError,
# preventing thread-safety corruption (nil PG::Result, segfaults, wrong data).
#
# Run: bundle exec ruby spec/integration/shared_connection_thread_safety_spec.rb

require_relative "support/example_helper"

THREAD_COUNT = 5
ITERATIONS = 30
THREAD_TIMEOUT = 10

ExampleHelper.run_example("Shared Connection Thread Safety") do |_client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("threadsafe")
  queues << queue

  # --- Unsafe pattern: callable returning same PG::Connection ---
  raw_conn = PG.connect(ExampleHelper::DB_PARAMS)
  unsafe_client = PGMQ::Client.new(-> { raw_conn }, pool_size: THREAD_COUNT)

  config_errors = 0
  ok_results = 0
  mutex = Mutex.new

  threads = THREAD_COUNT.times.map do
    break [] if interrupted.call

    Thread.new do
      ITERATIONS.times do
        break if interrupted.call

        begin
          unsafe_client.list_queues
          mutex.synchronize { ok_results += 1 }
        rescue PGMQ::Errors::ConfigurationError
          mutex.synchronize { config_errors += 1 }
        rescue
          # Connection errors from corrupted state
        end
      end
    end
  end
  threads.each { |t| t.join(THREAD_TIMEOUT) || t.kill }

  puts "Unsafe pattern: config_errors=#{config_errors}, ok=#{ok_results}"
  puts config_errors.positive? ? "  Shared connection detected" : "  Race not triggered this run"

  raw_conn.close rescue nil

  break if interrupted.call

  # --- Safe pattern: each pool slot gets its own connection ---
  safe_client = ExampleHelper.create_client(pool_size: THREAD_COUNT)
  safe_client.create(queue)

  safe_errors = 0
  safe_ok = 0

  threads = THREAD_COUNT.times.map do
    break [] if interrupted.call

    Thread.new do
      ITERATIONS.times do
        break if interrupted.call

        begin
          safe_client.archive(queue, 99_999)
          mutex.synchronize { safe_ok += 1 }
        rescue
          mutex.synchronize { safe_errors += 1 }
        end
      end
    end
  end
  threads.each { |t| t.join(THREAD_TIMEOUT) || t.kill }

  puts "Safe pattern: ok=#{safe_ok}, errors=#{safe_errors}"

  safe_client.close
end
