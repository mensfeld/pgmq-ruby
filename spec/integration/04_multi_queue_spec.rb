#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Multi-Queue Operations
#
# Demonstrates reading from multiple queues efficiently:
# - read_multi: Single query across multiple queues (UNION ALL)
# - read_multi_with_poll: Long-poll across multiple queues
# - pop_multi: Atomic read+delete from first available queue
#
# Run: bundle exec ruby examples/04_multi_queue_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Multi-Queue Operations') do |client, queues, interrupted|
  q1 = ExampleHelper.unique_queue_name('orders')
  q2 = ExampleHelper.unique_queue_name('notifications')
  q3 = ExampleHelper.unique_queue_name('emails')
  all_queues = [q1, q2, q3]
  queues.concat(all_queues)

  all_queues.each { |q| client.create(q) }

  # Produce to different queues
  client.produce(q1, ExampleHelper.to_json({ type: 'order', id: 1 }))
  client.produce(q1, ExampleHelper.to_json({ type: 'order', id: 2 }))
  client.produce(q2, ExampleHelper.to_json({ type: 'notification' }))
  client.produce(q3, ExampleHelper.to_json({ type: 'email' }))
  puts 'Produced: 2 orders, 1 notification, 1 email'

  break if interrupted.call

  # Read from all queues at once
  messages = client.read_multi(all_queues, vt: 30, qty: 2)
  puts "read_multi: #{messages.size} messages from #{messages.map(&:queue_name).uniq.size} queues"
  messages.each { |m| client.delete(m.queue_name, m.msg_id) }

  break if interrupted.call

  # Long-poll across queues
  messages = client.read_multi_with_poll(all_queues, vt: 30, qty: 5, max_poll_seconds: 1)
  puts "read_multi_with_poll: #{messages.size} messages"
  messages.each { |m| client.delete(m.queue_name, m.msg_id) }

  break if interrupted.call

  # pop_multi (atomic read+delete)
  client.produce(q1, ExampleHelper.to_json({ type: 'order', id: 3 }))
  msg = client.pop_multi(all_queues)
  puts "pop_multi: got message from #{msg&.queue_name || 'none'}"
end
