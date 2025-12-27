# frozen_string_literal: true

# Example: Pop Pattern (Atomic Read + Delete)
#
# Demonstrates pop for fire-and-forget processing:
# - pop: Atomically read and delete a message
# - pop_batch: Atomic batch read+delete
# - No VT needed - message is immediately deleted
#
# Run: bundle exec ruby spec/integration/pop_pattern_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Pop Pattern') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('pop')
  queues << queue

  client.create(queue)

  5.times { |i| client.produce(queue, ExampleHelper.to_json({ task: i + 1 })) }
  puts 'Produced 5 messages'

  # Pop single (atomic read+delete)
  msg = client.pop(queue)
  puts "pop: got task #{ExampleHelper.parse_message(msg)['task']} (already deleted)"

  # Pop batch
  msgs = client.pop_batch(queue, 3)
  puts "pop_batch: got #{msgs.size} messages (already deleted)"

  metrics = client.metrics(queue)
  puts "Remaining in queue: #{metrics&.queue_length}"

  # Clean up
  client.pop(queue)
end
