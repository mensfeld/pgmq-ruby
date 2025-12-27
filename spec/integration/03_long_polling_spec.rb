#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Long Polling
#
# Demonstrates efficient message consumption using long-polling:
# - read_with_poll blocks until a message arrives or timeout expires
# - More efficient than busy-waiting with repeated read calls
#
# Run: bundle exec ruby examples/03_long_polling_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Long Polling') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('polling')
  queues << queue

  client.create(queue)

  # Poll empty queue (will timeout)
  start = Time.now
  messages = client.read_with_poll(queue, vt: 30, qty: 1, max_poll_seconds: 2, poll_interval_ms: 100)
  puts "Empty queue poll: #{messages.size} messages in #{(Time.now - start).round(1)}s"

  break if interrupted.call

  # Produce messages
  5.times { |i| client.produce(queue, ExampleHelper.to_json({ task: i + 1 })) }
  puts 'Produced 5 messages'

  # Poll with messages (returns immediately)
  start = Time.now
  messages = client.read_with_poll(queue, vt: 30, qty: 3, max_poll_seconds: 5, poll_interval_ms: 100)
  puts "With messages: #{messages.size} messages in #{(Time.now - start).round(2)}s"
  client.delete_batch(queue, messages.map(&:msg_id))

  break if interrupted.call

  # Worker loop simulation
  processed = 0
  loop do
    break if interrupted.call

    msgs = client.read_with_poll(queue, vt: 30, qty: 1, max_poll_seconds: 1, poll_interval_ms: 50)
    break if msgs.empty?

    msgs.each { |m| client.delete(queue, m.msg_id) }
    processed += msgs.size
  end
  puts "Worker processed #{processed} remaining messages"
end
