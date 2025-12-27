#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Long Polling
#
# Demonstrates efficient message consumption using long-polling:
# - read_with_poll blocks until a message arrives or timeout expires
# - More efficient than busy-waiting with repeated read calls
# - Configurable poll interval and maximum wait time
#
# Long polling is ideal for worker processes that need to efficiently
# wait for new messages without wasting CPU cycles.
#
# Run: bundle exec ruby examples/03_long_polling.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Long Polling') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('polling')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Demonstrate polling on empty queue
  puts "\n2. Polling empty queue (max 2 seconds)..."
  puts '   This will wait up to 2 seconds for a message...'
  start_time = Time.now
  messages = client.read_with_poll(
    queue,
    vt: 30,
    qty: 1,
    max_poll_seconds: 2,
    poll_interval_ms: 100
  )
  elapsed = (Time.now - start_time).round(2)
  puts "   Result: #{messages.size} messages (waited #{elapsed}s)"

  break if interrupted.call

  # Step 3: Produce some messages
  puts "\n3. Producing 5 messages..."
  5.times do |i|
    client.produce(queue, ExampleHelper.to_json({ task_id: i + 1, data: "Task #{i + 1}" }))
  end
  puts '   Produced 5 messages.'

  # Step 4: Poll for messages (should return immediately)
  puts "\n4. Polling with messages available..."
  start_time = Time.now
  messages = client.read_with_poll(
    queue,
    vt: 30,
    qty: 3,
    max_poll_seconds: 5,
    poll_interval_ms: 100
  )
  elapsed = (Time.now - start_time).round(2)
  puts "   Retrieved #{messages.size} messages in #{elapsed}s"
  messages.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - Task #{data['task_id']}: #{data['data']}"
  end

  # Delete processed messages
  client.delete_batch(queue, messages.map(&:msg_id))

  break if interrupted.call

  # Step 5: Simulate worker loop with polling
  puts "\n5. Simulating worker loop (processing remaining messages)..."
  processed = 0

  loop do
    break if interrupted.call

    msgs = client.read_with_poll(
      queue,
      vt: 30,
      qty: 1,
      max_poll_seconds: 1,
      poll_interval_ms: 50
    )

    break if msgs.empty?

    msgs.each do |msg|
      data = ExampleHelper.parse_message(msg)
      puts "   Processing: #{data['data']}"
      client.delete(queue, msg.msg_id)
      processed += 1
    end
  end

  puts "   Worker loop complete. Processed #{processed} additional messages."
end
