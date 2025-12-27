#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Queue Metrics and Monitoring
#
# Demonstrates PGMQ monitoring capabilities:
# - metrics: Get metrics for a single queue
# - metrics_all: Get metrics for all queues
# - list_queues: List all queues with metadata
#
# Run: bundle exec ruby examples/09_queue_metrics_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Queue Metrics') do |client, queues, interrupted|
  q1 = ExampleHelper.unique_queue_name('orders')
  q2 = ExampleHelper.unique_queue_name('empty')
  queues << q1 << q2

  client.create(q1)
  client.create(q2)

  10.times { |i| client.produce(q1, ExampleHelper.to_json({ id: i })) }
  puts 'Created 2 queues, added 10 messages to first'

  break if interrupted.call

  # Single queue metrics
  m = client.metrics(q1)
  puts "Orders queue: length=#{m.queue_length}, oldest_age=#{m.oldest_msg_age_sec}s"

  # All queues metrics
  all = client.metrics_all.select { |x| queues.include?(x.queue_name) }
  puts "All queues: #{all.map { |x| "#{x.queue_name.split('_').first}=#{x.queue_length}" }.join(', ')}"

  break if interrupted.call

  # Queue metadata
  list = client.list_queues.select { |x| queues.include?(x.queue_name) }
  list.each { |q| puts "#{q.queue_name}: partitioned=#{q.is_partitioned}, unlogged=#{q.is_unlogged}" }

  # Clean up
  loop { break unless client.pop(q1) }
end
