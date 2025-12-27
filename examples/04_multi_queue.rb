#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Multi-Queue Operations
#
# Demonstrates reading from multiple queues efficiently:
# - read_multi: Read from multiple queues in a single database query (UNION ALL)
# - read_multi_with_poll: Long-poll across multiple queues
# - pop_multi: Atomic read+delete from the first available queue
#
# Multi-queue operations are useful for workers that handle messages
# from different sources with a single polling loop.
#
# Run: bundle exec ruby examples/04_multi_queue.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Multi-Queue Operations') do |client, queues, interrupted|
  # Create multiple queues
  orders_queue = ExampleHelper.unique_queue_name('orders')
  notifications_queue = ExampleHelper.unique_queue_name('notifications')
  emails_queue = ExampleHelper.unique_queue_name('emails')
  all_queues = [orders_queue, notifications_queue, emails_queue]
  queues.concat(all_queues)

  puts '1. Creating multiple queues...'
  all_queues.each do |q|
    client.create(q)
    puts "   Created: #{q}"
  end

  # Step 2: Produce messages to different queues
  puts "\n2. Producing messages to different queues..."
  client.produce(orders_queue, ExampleHelper.to_json({ type: 'order', id: 1001 }))
  client.produce(orders_queue, ExampleHelper.to_json({ type: 'order', id: 1002 }))
  client.produce(notifications_queue, ExampleHelper.to_json({ type: 'notification', alert: 'System update' }))
  client.produce(emails_queue, ExampleHelper.to_json({ type: 'email', to: 'user@example.com' }))
  puts '   Produced: 2 orders, 1 notification, 1 email'

  break if interrupted.call

  # Step 3: Read from multiple queues at once
  puts "\n3. Reading from all queues with read_multi..."
  messages = client.read_multi(all_queues, vt: 30, qty: 2)
  puts "   Retrieved #{messages.size} messages:"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Queue: #{msg.queue_name}, Type: #{data['type']}, ID: #{msg.msg_id}"
  end

  # Delete processed messages
  messages.each { |msg| client.delete(msg.queue_name, msg.msg_id) }

  break if interrupted.call

  # Step 4: Use read_multi_with_poll for long-polling across queues
  puts "\n4. Long-polling across multiple queues..."
  start_time = Time.now
  messages = client.read_multi_with_poll(
    all_queues,
    vt: 30,
    qty: 5,
    max_poll_seconds: 2,
    poll_interval_ms: 100
  )
  elapsed = (Time.now - start_time).round(2)
  puts "   Retrieved #{messages.size} messages in #{elapsed}s"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Queue: #{msg.queue_name}, Type: #{data['type']}"
    client.delete(msg.queue_name, msg.msg_id)
  end

  break if interrupted.call

  # Step 5: Demonstrate pop_multi (atomic read+delete from first available)
  puts "\n5. Using pop_multi for atomic read+delete..."
  # Add fresh messages
  client.produce(orders_queue, ExampleHelper.to_json({ type: 'order', id: 1003 }))
  client.produce(notifications_queue, ExampleHelper.to_json({ type: 'notification', alert: 'New alert' }))

  2.times do |i|
    msg = client.pop_multi(all_queues)
    if msg
      data = ExampleHelper.parse_message(msg)
      puts "   Pop #{i + 1}: Queue=#{msg.queue_name}, Type=#{data['type']} (already deleted)"
    else
      puts "   Pop #{i + 1}: No message available"
    end
  end

  # Step 6: Demonstrate limit parameter
  puts "\n6. Using limit parameter to cap total messages..."
  # Add more messages
  3.times { |i| client.produce(orders_queue, ExampleHelper.to_json({ type: 'order', id: 2000 + i })) }
  3.times { |i| client.produce(emails_queue, ExampleHelper.to_json({ type: 'email', id: 3000 + i })) }

  # Read with limit (max 4 total across all queues)
  messages = client.read_multi(all_queues, vt: 30, qty: 10, limit: 4)
  puts "   Requested qty=10 with limit=4, got #{messages.size} messages"

  # Clean up
  messages.each { |msg| client.delete(msg.queue_name, msg.msg_id) }

  # Clean up any remaining
  remaining = client.read_multi(all_queues, vt: 1, qty: 100)
  remaining.each { |msg| client.delete(msg.queue_name, msg.msg_id) }
end
