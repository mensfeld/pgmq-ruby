#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Conditional Filtering
#
# Demonstrates server-side JSONB filtering for selective message reading:
# - Filter messages by payload field values
# - Multiple conditions (AND logic)
# - Works with read, read_batch, and read_with_poll
#
# Conditional filtering pushes filtering logic to the database,
# reducing network traffic and application-side filtering overhead.
#
# Run: bundle exec ruby examples/07_conditional_filtering.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Conditional Filtering') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('filter')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Produce messages with different attributes
  puts "\n2. Producing messages with various attributes..."
  messages = [
    { type: 'order', priority: 'high', region: 'us-east', amount: 500 },
    { type: 'order', priority: 'low', region: 'us-west', amount: 50 },
    { type: 'notification', priority: 'high', region: 'us-east', alert: 'critical' },
    { type: 'order', priority: 'high', region: 'eu-west', amount: 1000 },
    { type: 'notification', priority: 'low', region: 'us-east', alert: 'info' },
    { type: 'order', priority: 'medium', region: 'us-east', amount: 200 }
  ]

  messages.each do |msg|
    client.produce(queue, ExampleHelper.to_json(msg))
    puts "   Produced: type=#{msg[:type]}, priority=#{msg[:priority]}, region=#{msg[:region]}"
  end

  break if interrupted.call

  # Step 3: Filter by single condition
  puts "\n3. Reading only 'high' priority messages..."
  high_priority = client.read_batch(
    queue,
    vt: 30,
    qty: 10,
    conditional: { priority: 'high' }
  )
  puts "   Found #{high_priority.size} high priority messages:"
  high_priority.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Type: #{data['type']}, Region: #{data['region']}"
  end
  client.delete_batch(queue, high_priority.map(&:msg_id))

  break if interrupted.call

  # Step 4: Filter by multiple conditions (AND)
  puts "\n4. Reading 'order' type from 'us-east' region..."
  filtered = client.read_batch(
    queue,
    vt: 30,
    qty: 10,
    conditional: { type: 'order', region: 'us-east' }
  )
  puts "   Found #{filtered.size} matching messages:"
  filtered.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Priority: #{data['priority']}, Amount: $#{data['amount']}"
  end
  client.delete_batch(queue, filtered.map(&:msg_id))

  break if interrupted.call

  # Step 5: Filter with read_with_poll
  puts "\n5. Long-polling with filter (notifications only)..."
  start_time = Time.now
  notifications = client.read_with_poll(
    queue,
    vt: 30,
    qty: 10,
    max_poll_seconds: 2,
    poll_interval_ms: 100,
    conditional: { type: 'notification' }
  )
  elapsed = (Time.now - start_time).round(2)
  puts "   Found #{notifications.size} notifications in #{elapsed}s:"
  notifications.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Alert: #{data['alert']}, Priority: #{data['priority']}"
  end
  client.delete_batch(queue, notifications.map(&:msg_id)) if notifications.any?

  # Step 6: Show remaining messages (should be orders not matching filters)
  puts "\n6. Remaining messages in queue..."
  remaining = client.read_batch(queue, vt: 1, qty: 10)
  puts "   #{remaining.size} messages remaining:"
  remaining.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Type: #{data['type']}, Priority: #{data['priority']}, Region: #{data['region']}"
    client.delete(queue, msg.msg_id)
  end

  puts "\n   Note: Conditional filtering uses PostgreSQL JSONB operators"
  puts '   for efficient server-side filtering.'
end
