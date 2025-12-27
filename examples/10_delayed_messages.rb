#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Delayed Messages
#
# Demonstrates scheduled/delayed message delivery:
# - produce with delay parameter: Schedule message for future delivery
# - produce_batch with delay: Batch scheduling
#
# Delayed messages are invisible until their scheduled time.
# Use cases: scheduled tasks, retry with backoff, rate limiting.
#
# Run: bundle exec ruby examples/10_delayed_messages.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Delayed Messages') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('delayed')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Produce immediate and delayed messages
  puts "\n2. Producing messages with different delays..."

  # Immediate message
  msg_id1 = client.produce(queue, ExampleHelper.to_json({ type: 'immediate', task: 'Task A' }))
  puts "   Immediate message: ID #{msg_id1}"

  # Delayed message (2 seconds)
  msg_id2 = client.produce(
    queue,
    ExampleHelper.to_json({ type: 'delayed_2s', task: 'Task B' }),
    delay: 2
  )
  puts "   2-second delay: ID #{msg_id2}"

  # Delayed message (4 seconds)
  msg_id3 = client.produce(
    queue,
    ExampleHelper.to_json({ type: 'delayed_4s', task: 'Task C' }),
    delay: 4
  )
  puts "   4-second delay: ID #{msg_id3}"

  break if interrupted.call

  # Step 3: Try to read immediately
  puts "\n3. Reading immediately after producing..."
  messages = client.read_batch(queue, vt: 30, qty: 10)
  puts "   Available messages: #{messages.size}"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - #{data['type']}: #{data['task']}"
    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Step 4: Wait and read again
  puts "\n4. Waiting 2 seconds for delayed message..."
  sleep 2

  messages = client.read_batch(queue, vt: 30, qty: 10)
  puts "   Available after 2s: #{messages.size}"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - #{data['type']}: #{data['task']}"
    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Step 5: Wait for final message
  puts "\n5. Waiting 2 more seconds for final message..."
  sleep 2

  messages = client.read_batch(queue, vt: 30, qty: 10)
  puts "   Available after 4s: #{messages.size}"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - #{data['type']}: #{data['task']}"
    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Step 6: Demonstrate batch with delay
  puts "\n6. Producing batch with delay..."
  batch_messages = [
    ExampleHelper.to_json({ batch_item: 1, scheduled: 'batch_delayed' }),
    ExampleHelper.to_json({ batch_item: 2, scheduled: 'batch_delayed' }),
    ExampleHelper.to_json({ batch_item: 3, scheduled: 'batch_delayed' })
  ]

  msg_ids = client.produce_batch(queue, batch_messages, delay: 1)
  puts "   Produced #{msg_ids.size} messages with 1s delay"

  # Verify not visible yet
  messages = client.read_batch(queue, vt: 1, qty: 10)
  puts "   Immediately visible: #{messages.size}"

  # Wait and verify
  sleep 1.1
  messages = client.read_batch(queue, vt: 30, qty: 10)
  puts "   After 1s delay: #{messages.size} messages now visible"

  # Clean up
  client.delete_batch(queue, messages.map(&:msg_id)) if messages.any?

  puts "\n   Delayed messages use cases:"
  puts '     - Scheduled tasks (run at specific time)'
  puts '     - Retry with exponential backoff'
  puts '     - Rate limiting (spread load over time)'
  puts '     - Reminder/notification scheduling'
end
