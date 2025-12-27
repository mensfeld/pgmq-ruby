#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Error Handling
#
# Demonstrates proper error handling patterns with PGMQ:
# - Connection errors and recovery
# - Queue not found errors
# - Invalid queue name validation
# - Dead letter queue pattern
# - Retry with exponential backoff
#
# Proper error handling ensures robust message processing.
#
# Run: bundle exec ruby examples/14_error_handling.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Error Handling') do |client, queues, interrupted|
  main_queue = ExampleHelper.unique_queue_name('main')
  dlq_queue = ExampleHelper.unique_queue_name('dlq')
  queues << main_queue
  queues << dlq_queue

  # Create queues
  puts '1. Creating queues...'
  client.create(main_queue)
  client.create(dlq_queue)
  puts "   Main queue: #{main_queue}"
  puts "   Dead letter queue: #{dlq_queue}"

  break if interrupted.call

  # Step 2: Demonstrate error types
  puts "\n2. PGMQ Error Types:"
  puts '   PGMQ::Errors::BaseError - Base class for all PGMQ errors'
  puts '   PGMQ::Errors::ConnectionError - Database connection issues'
  puts '   PGMQ::Errors::QueueNotFoundError - Queue does not exist'
  puts '   PGMQ::Errors::InvalidQueueNameError - Invalid queue name format'
  puts '   PGMQ::Errors::SerializationError - Message serialization failed'
  puts '   PGMQ::Errors::ConfigurationError - Invalid configuration'

  break if interrupted.call

  # Step 3: Invalid queue name error
  puts "\n3. Handling InvalidQueueNameError..."
  invalid_names = [
    '123_starts_with_number',
    'name-with-dashes',
    'a' * 50, # Too long (max 48 chars)
    ''
  ]

  invalid_names.each do |name|
    client.create(name)
    puts "   '#{name[0..20]}...' - Unexpectedly succeeded"
  rescue PGMQ::Errors::InvalidQueueNameError => e
    puts "   '#{name[0..20]}...' - Caught: #{e.message[0..50]}..."
  end

  break if interrupted.call

  # Step 4: Queue not found handling
  puts "\n4. Handling operations on non-existent queue..."
  begin
    client.read('nonexistent_queue_xyz', vt: 30)
    puts '   Read succeeded (queue might exist)'
  rescue PGMQ::Errors::QueueNotFoundError => e
    puts "   Caught QueueNotFoundError: #{e.message[0..60]}..."
  rescue PGMQ::Errors::ConnectionError => e
    # ConnectionError wraps PG errors like missing tables
    msg = e.message.split("\n").first[0..50]
    puts "   Caught ConnectionError (queue does not exist): #{msg}..."
  rescue PG::UndefinedTable => e
    puts "   Caught PG error (queue does not exist): #{e.message.split("\n").first[0..50]}..."
  end

  break if interrupted.call

  # Step 5: Dead Letter Queue pattern
  puts "\n5. Demonstrating Dead Letter Queue (DLQ) pattern..."

  # Add messages that will "fail" processing
  3.times do |i|
    client.produce(main_queue, ExampleHelper.to_json({
      task_id: i + 1,
      will_fail: i.even? # Even tasks will fail
    }))
  end
  puts '   Produced 3 messages (some will fail processing)'

  max_retries = 3
  processed = 0
  failed = 0

  loop do
    break if interrupted.call

    msg = client.read(main_queue, vt: 5)
    break unless msg

    data = ExampleHelper.parse_message(msg)
    retry_count = data['retry_count'] || 0

    begin
      # Simulate processing (fails for even task_ids)
      raise StandardError, "Simulated failure for task #{data['task_id']}" if data['will_fail']

      puts "   Processed task #{data['task_id']} successfully"
      client.delete(main_queue, msg.msg_id)
      processed += 1
    rescue StandardError => e
      if retry_count < max_retries
        # Re-queue with incremented retry count
        data['retry_count'] = retry_count + 1
        data['last_error'] = e.message
        client.produce(main_queue, ExampleHelper.to_json(data))
        client.delete(main_queue, msg.msg_id)
        puts "   Task #{data['task_id']} failed, retry #{data['retry_count']}/#{max_retries}"
      else
        # Move to DLQ after max retries
        data['final_error'] = e.message
        data['failed_at'] = Time.now.to_s
        client.produce(dlq_queue, ExampleHelper.to_json(data))
        client.delete(main_queue, msg.msg_id)
        puts "   Task #{data['task_id']} moved to DLQ after #{max_retries} retries"
        failed += 1
      end
    end
  end

  puts "   Result: #{processed} processed, #{failed} moved to DLQ"

  break if interrupted.call

  # Step 6: Check DLQ
  puts "\n6. Checking Dead Letter Queue..."
  dlq_messages = client.read_batch(dlq_queue, vt: 30, qty: 10)
  puts "   DLQ contains #{dlq_messages.size} failed messages:"
  dlq_messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Task #{data['task_id']}: #{data['final_error']}"
    client.delete(dlq_queue, msg.msg_id)
  end

  # Step 7: Connection error handling pattern
  puts "\n7. Connection error handling pattern:"
  puts '   begin'
  puts '     client.produce(queue, message)'
  puts '   rescue PGMQ::Errors::ConnectionError => e'
  puts '     logger.error("Connection failed: #{e.message}")'
  puts '     # Option 1: Retry with backoff'
  puts '     # Option 2: Use circuit breaker'
  puts '     # Option 3: Queue locally and retry later'
  puts '   end'

  puts "\n8. Best practices:"
  puts '   - Always handle specific errors before generic ones'
  puts '   - Use DLQ for messages that fail repeatedly'
  puts '   - Implement exponential backoff for retries'
  puts '   - Log errors with context for debugging'
  puts '   - Monitor DLQ size for alerting'
  puts '   - Consider circuit breaker for connection issues'
end
