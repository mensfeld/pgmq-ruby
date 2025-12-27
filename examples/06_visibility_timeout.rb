#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Visibility Timeout Management
#
# Demonstrates extending processing time for long-running tasks:
# - set_vt: Extend visibility timeout for a single message
# - set_vt_batch: Extend timeout for multiple messages
# - Heartbeat pattern for long-running processing
#
# Visibility timeout prevents other consumers from reading a message
# while it's being processed. Extending it prevents message redelivery
# for tasks that take longer than expected.
#
# Run: bundle exec ruby examples/06_visibility_timeout.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Visibility Timeout Management') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('vt')
  queues << queue

  # Create queue and add messages
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  puts "\n2. Producing messages..."
  3.times do |i|
    client.produce(queue, ExampleHelper.to_json({ task_id: i + 1, data: "Long task #{i + 1}" }))
  end
  puts '   Produced 3 messages.'

  break if interrupted.call

  # Step 3: Read with short visibility timeout
  puts "\n3. Reading message with 5 second visibility timeout..."
  msg = client.read(queue, vt: 5)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Read task #{data['task_id']}"
    puts "   Original VT: #{msg.vt}"

    # Step 4: Extend visibility timeout
    puts "\n4. Extending visibility timeout by 30 seconds..."
    updated_msg = client.set_vt(queue, msg.msg_id, vt_offset: 30)
    puts "   New VT: #{updated_msg.vt}"
    puts '   Message now invisible for 30 more seconds'

    # Delete when done
    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Step 5: Demonstrate batch visibility timeout update
  puts "\n5. Demonstrating batch visibility timeout update..."
  messages = client.read_batch(queue, vt: 5, qty: 2)
  puts "   Read #{messages.size} messages with 5s VT"

  if messages.any?
    msg_ids = messages.map(&:msg_id)
    puts '   Extending VT for all messages by 60 seconds...'
    updated = client.set_vt_batch(queue, msg_ids, vt_offset: 60)
    puts "   Updated #{updated.size} messages"
    updated.each do |m|
      puts "     - ID #{m.msg_id}: new VT = #{m.vt}"
    end

    # Clean up
    client.delete_batch(queue, msg_ids)
  end

  break if interrupted.call

  # Step 6: Demonstrate heartbeat pattern
  puts "\n6. Demonstrating heartbeat pattern for long-running task..."
  client.produce(queue, ExampleHelper.to_json({ task_id: 99, data: 'Very long task' }))

  msg = client.read(queue, vt: 2) # Short initial timeout
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Processing task #{data['task_id']} (simulating long work)..."

    # Simulate long processing with periodic heartbeats
    3.times do |i|
      break if interrupted.call

      # Simulate work
      sleep 0.5

      # Send heartbeat (extend VT)
      client.set_vt(queue, msg.msg_id, vt_offset: 2)
      puts "   Heartbeat #{i + 1}: Extended VT by 2 seconds"
    end

    puts '   Task complete, deleting message...'
    client.delete(queue, msg.msg_id)
  end

  puts "\n   Heartbeat pattern ensures messages are not redelivered"
  puts '   during long-running tasks by periodically extending VT.'
end
