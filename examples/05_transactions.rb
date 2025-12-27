#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Transactions
#
# Demonstrates atomic operations across queues using PostgreSQL transactions:
# - All operations within a transaction block succeed or fail together
# - Automatic rollback on errors
# - Useful for read-process-forward patterns
# - Ensures exactly-once processing semantics
#
# Transactions are essential for workflows where you need to move messages
# between queues atomically (e.g., from inbox to processed queue).
#
# Run: bundle exec ruby examples/05_transactions.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Transactions') do |client, queues, interrupted|
  # Create queues
  inbox_queue = ExampleHelper.unique_queue_name('inbox')
  processed_queue = ExampleHelper.unique_queue_name('processed')
  failed_queue = ExampleHelper.unique_queue_name('failed')
  all_queues = [inbox_queue, processed_queue, failed_queue]
  queues.concat(all_queues)

  puts '1. Creating queues for transaction demo...'
  all_queues.each do |q|
    client.create(q)
    puts "   Created: #{q}"
  end

  # Add messages to inbox
  puts "\n2. Producing messages to inbox..."
  5.times do |i|
    message = { job_id: i + 1, data: "Job #{i + 1}" }
    client.produce(inbox_queue, ExampleHelper.to_json(message))
  end
  puts '   Produced 5 messages to inbox.'

  break if interrupted.call

  # Step 3: Demonstrate successful transaction
  puts "\n3. Processing message with successful transaction..."
  client.transaction do |txn|
    # Read from inbox
    msg = txn.read(inbox_queue, vt: 30)
    if msg
      data = ExampleHelper.parse_message(msg)
      puts "   Read job #{data['job_id']} from inbox"

      # Process (simulated)
      result = { job_id: data['job_id'], status: 'completed', processed_at: Time.now.to_s }

      # Forward to processed queue
      txn.produce(processed_queue, ExampleHelper.to_json(result))
      puts '   Forwarded to processed queue'

      # Delete from inbox
      txn.delete(inbox_queue, msg.msg_id)
      puts '   Deleted from inbox'
      puts '   Transaction committed - all operations atomic'
    end
  end

  break if interrupted.call

  # Step 4: Demonstrate transaction rollback on error
  puts "\n4. Demonstrating transaction rollback on error..."
  begin
    client.transaction do |txn|
      msg = txn.read(inbox_queue, vt: 30)
      if msg
        data = ExampleHelper.parse_message(msg)
        puts "   Read job #{data['job_id']} from inbox"

        # Forward to processed queue
        result = { job_id: data['job_id'], status: 'processing' }
        txn.produce(processed_queue, ExampleHelper.to_json(result))
        puts '   Produced to processed queue'

        # Simulate an error during processing
        puts '   Simulating error...'
        raise StandardError, 'Simulated processing error!'
      end
    end
  rescue StandardError => e
    puts "   Error caught: #{e.message}"
    puts '   Transaction rolled back - message still in inbox'
  end

  # Verify message is still in inbox (rollback worked)
  msg = client.read(inbox_queue, vt: 1)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Verified: Job #{data['job_id']} still in inbox (rollback success)"
    # Reset visibility for next step
    client.set_vt(inbox_queue, msg.msg_id, vt_offset: -30)
  end

  break if interrupted.call

  # Step 5: Demonstrate multi-queue batch transaction
  puts "\n5. Processing multiple messages in single transaction..."
  client.transaction do |txn|
    messages = txn.read_batch(inbox_queue, vt: 30, qty: 3)
    puts "   Read #{messages.size} messages from inbox"

    messages.each do |msg|
      data = ExampleHelper.parse_message(msg)
      result = { job_id: data['job_id'], status: 'batch_processed' }
      txn.produce(processed_queue, ExampleHelper.to_json(result))
    end
    puts "   Produced #{messages.size} results to processed queue"

    txn.delete_batch(inbox_queue, messages.map(&:msg_id))
    puts "   Deleted #{messages.size} messages from inbox"
    puts '   Batch transaction committed'
  end

  # Step 6: Show final queue states
  puts "\n6. Final queue states:"
  [inbox_queue, processed_queue, failed_queue].each do |q|
    metrics = client.metrics(q)
    puts "   #{q.split('_').first}: #{metrics&.queue_length || 0} messages"
  end

  # Clean up processed queue
  loop do
    msg = client.pop(processed_queue)
    break unless msg
  end
end
