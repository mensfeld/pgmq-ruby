#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Queue Metrics and Monitoring
#
# Demonstrates PGMQ's monitoring capabilities:
# - metrics: Get metrics for a single queue
# - metrics_all: Get metrics for all queues
# - list_queues: List all queues with metadata
#
# Metrics include queue length, message age, and total message count.
# Useful for monitoring, alerting, and capacity planning.
#
# Run: bundle exec ruby examples/09_queue_metrics.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Queue Metrics and Monitoring') do |client, queues, interrupted|
  # Create multiple queues with different characteristics
  orders_queue = ExampleHelper.unique_queue_name('orders')
  notifications_queue = ExampleHelper.unique_queue_name('notifications')
  empty_queue = ExampleHelper.unique_queue_name('empty')
  all_queues = [orders_queue, notifications_queue, empty_queue]
  queues.concat(all_queues)

  puts '1. Creating test queues...'
  all_queues.each do |q|
    client.create(q)
    puts "   Created: #{q}"
  end

  # Add messages to simulate different queue states
  puts "\n2. Adding messages to queues..."
  10.times { |i| client.produce(orders_queue, ExampleHelper.to_json({ order_id: i + 1 })) }
  puts '   Added 10 messages to orders queue'

  3.times { |i| client.produce(notifications_queue, ExampleHelper.to_json({ notification_id: i + 1 })) }
  puts '   Added 3 messages to notifications queue'
  puts '   Empty queue has 0 messages'

  # Small delay to establish message age
  sleep 0.1

  break if interrupted.call

  # Step 3: Get metrics for single queue
  puts "\n3. Getting metrics for orders queue..."
  metrics = client.metrics(orders_queue)
  if metrics
    puts '   Metrics:'
    puts "     queue_name: #{metrics.queue_name}"
    puts "     queue_length: #{metrics.queue_length}"
    puts "     oldest_msg_age_sec: #{metrics.oldest_msg_age_sec}"
    puts "     newest_msg_age_sec: #{metrics.newest_msg_age_sec}"
    puts "     total_messages: #{metrics.total_messages}"
    puts "     scrape_time: #{metrics.scrape_time}"
  end

  break if interrupted.call

  # Step 4: Get metrics for all queues
  puts "\n4. Getting metrics for all queues..."
  all_metrics = client.metrics_all
  puts "   Found #{all_metrics.size} queues with metrics:"

  # Filter to our test queues
  test_metrics = all_metrics.select { |m| all_queues.include?(m.queue_name) }
  test_metrics.each do |m|
    puts "\n   Queue: #{m.queue_name}"
    puts "     Length: #{m.queue_length}"
    puts "     Oldest message: #{m.oldest_msg_age_sec}s ago"
    puts "     Total ever: #{m.total_messages}"
  end

  break if interrupted.call

  # Step 5: List all queues with metadata
  puts "\n5. Listing all queues with metadata..."
  queue_list = client.list_queues
  puts "   Total queues in database: #{queue_list.size}"

  # Filter to our test queues
  test_queues = queue_list.select { |q| all_queues.include?(q.queue_name) }
  puts "\n   Our test queues:"
  test_queues.each do |q|
    puts "     #{q.queue_name}:"
    puts "       created_at: #{q.created_at}"
    puts "       partitioned: #{q.is_partitioned}"
    puts "       unlogged: #{q.is_unlogged}"
  end

  break if interrupted.call

  # Step 6: Demonstrate monitoring loop
  puts "\n6. Simulating monitoring loop (3 iterations)..."
  3.times do |i|
    break if interrupted.call

    puts "\n   Iteration #{i + 1}:"

    # Process some messages from orders
    msgs = client.read_batch(orders_queue, vt: 1, qty: 2)
    client.delete_batch(orders_queue, msgs.map(&:msg_id)) if msgs.any?

    # Get fresh metrics
    metrics = client.metrics(orders_queue)
    if metrics
      puts "     Orders queue: #{metrics.queue_length} pending, #{metrics.oldest_msg_age_sec}s oldest"
    end

    sleep 0.2
  end

  puts "\n   Monitoring can be used for:"
  puts '     - Alerting on queue depth thresholds'
  puts '     - Tracking message age (processing lag)'
  puts '     - Capacity planning based on total_messages trends'

  # Clean up
  loop do
    msg = client.pop(orders_queue)
    break unless msg
  end
  loop do
    msg = client.pop(notifications_queue)
    break unless msg
  end
end
