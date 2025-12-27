# frozen_string_literal: true

# Example: Delayed Messages
#
# Demonstrates scheduled message delivery:
# - produce with delay: Message invisible until delay expires
# - Use cases: scheduled tasks, retry backoff, rate limiting
#
# Run: bundle exec ruby spec/integration/delayed_messages_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Delayed Messages') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('delayed')
  queues << queue

  client.create(queue)

  # Produce with different delays
  client.produce(queue, ExampleHelper.to_json({ type: 'immediate' }))
  client.produce(queue, ExampleHelper.to_json({ type: 'delayed_2s' }), delay: 2)
  puts 'Produced: 1 immediate, 1 delayed (2s)'

  break if interrupted.call

  # Read immediately
  msgs = client.read_batch(queue, vt: 30, qty: 10)
  puts "Immediate read: #{msgs.size} available"
  client.delete_batch(queue, msgs.map(&:msg_id)) if msgs.any?

  break if interrupted.call

  # Wait and read again
  puts 'Waiting 2 seconds...'
  sleep 2

  msgs = client.read_batch(queue, vt: 30, qty: 10)
  puts "After delay: #{msgs.size} available"
  client.delete_batch(queue, msgs.map(&:msg_id)) if msgs.any?
end
