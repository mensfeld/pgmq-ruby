#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Pop Pattern (Atomic Read + Delete)
#
# Demonstrates the pop operation for fire-and-forget processing:
# - pop: Atomically read and delete a single message
# - pop_batch: Atomically read and delete multiple messages
# - No visibility timeout needed - message is immediately deleted
#
# Use pop when you don't need retry capability and want simpler code.
# Trade-off: If processing fails, the message is already gone.
#
# Run: bundle exec ruby examples/13_pop_pattern.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Pop Pattern (Atomic Read + Delete)') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('pop')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Produce messages
  puts "\n2. Producing 5 messages..."
  5.times do |i|
    client.produce(queue, ExampleHelper.to_json({ task_id: i + 1, data: "Task #{i + 1}" }))
  end
  puts '   Produced 5 messages.'

  break if interrupted.call

  # Step 3: Pop single message
  puts "\n3. Using pop (atomic read + delete)..."
  msg = client.pop(queue)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Popped task #{data['task_id']}"
    puts "   Message ID: #{msg.msg_id}"
    puts '   Note: Message is already deleted from queue!'

    # Verify it's gone
    metrics = client.metrics(queue)
    puts "   Queue length after pop: #{metrics&.queue_length}"
  end

  break if interrupted.call

  # Step 4: Pop batch of messages
  puts "\n4. Using pop_batch (atomic batch read + delete)..."
  messages = client.pop_batch(queue, 3)
  puts "   Popped #{messages.size} messages:"
  messages.each do |m|
    data = ExampleHelper.parse_message(m)
    puts "     - Task #{data['task_id']} (ID: #{m.msg_id})"
  end
  puts '   All messages already deleted!'

  metrics = client.metrics(queue)
  puts "   Queue length after pop_batch: #{metrics&.queue_length}"

  break if interrupted.call

  # Step 5: Compare with read + delete pattern
  puts "\n5. Comparison: pop vs read+delete..."

  # Add a message
  client.produce(queue, ExampleHelper.to_json({ comparison: 'test' }))

  puts "\n   Pattern A: pop (atomic)"
  puts '     msg = client.pop(queue)'
  puts '     process(msg)'
  puts '     # Done! No delete needed'
  puts '     Pros: Simpler code, atomic operation'
  puts '     Cons: No retry if processing fails'

  puts "\n   Pattern B: read + delete (separate)"
  puts '     msg = client.read(queue, vt: 30)'
  puts '     process(msg)'
  puts '     client.delete(queue, msg.msg_id)'
  puts '     Pros: Retry on failure (message reappears after VT)'
  puts '     Cons: More code, need visibility timeout management'

  # Clean up remaining
  client.pop(queue)

  break if interrupted.call

  # Step 6: Pop from multiple queues
  puts "\n6. Using pop_multi (pop from first available queue)..."
  queue2 = ExampleHelper.unique_queue_name('pop2')
  queue3 = ExampleHelper.unique_queue_name('pop3')
  queues << queue2
  queues << queue3

  client.create(queue2)
  client.create(queue3)

  # Only add message to queue3
  client.produce(queue3, ExampleHelper.to_json({ from: 'queue3' }))

  msg = client.pop_multi([queue, queue2, queue3])
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Popped from: #{msg.queue_name}"
    puts "   Data: #{data.inspect}"
  else
    puts '   No messages in any queue'
  end

  puts "\n7. When to use pop vs read:"
  puts '   Use POP when:'
  puts '     - Processing is idempotent or best-effort'
  puts '     - You want simpler code'
  puts '     - Message loss on failure is acceptable'
  puts '     - High throughput is critical'

  puts "\n   Use READ + DELETE when:"
  puts '     - You need retry on failure'
  puts '     - Processing is not idempotent'
  puts '     - Message delivery guarantee is important'
  puts '     - You need to extend visibility (heartbeat)'
end
