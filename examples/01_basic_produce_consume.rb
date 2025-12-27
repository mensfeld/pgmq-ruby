#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Basic Produce and Consume
#
# Demonstrates the fundamental PGMQ workflow:
# 1. Create a queue
# 2. Produce (send) a message
# 3. Read the message with visibility timeout
# 4. Delete the message after processing
#
# This is the simplest possible PGMQ usage pattern.
#
# Run: bundle exec ruby examples/01_basic_produce_consume.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Basic Produce/Consume') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('basic')
  queues << queue

  # Step 1: Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created successfully.'

  # Step 2: Produce a message
  message = { order_id: 12_345, status: 'pending', amount: 99.99 }
  puts "\n2. Producing message:"
  puts "   Payload: #{message.inspect}"
  msg_id = client.produce(queue, ExampleHelper.to_json(message))
  puts "   Message ID: #{msg_id}"

  # Step 3: Read the message with visibility timeout
  # vt: 30 means the message is invisible to other consumers for 30 seconds
  puts "\n3. Reading message (visibility timeout: 30s)..."
  msg = client.read(queue, vt: 30)

  if msg
    puts '   Message received:'
    puts "     msg_id: #{msg.msg_id}"
    puts "     read_ct: #{msg.read_ct} (times this message has been read)"
    puts "     enqueued_at: #{msg.enqueued_at}"
    puts "     vt: #{msg.vt} (visible again after this time)"
    puts "     message: #{msg.message}"

    # Parse and process the message
    data = ExampleHelper.parse_message(msg)
    puts "\n   Processing order #{data['order_id']} for $#{data['amount']}..."

    # Step 4: Delete after successful processing
    puts "\n4. Deleting message after successful processing..."
    deleted = client.delete(queue, msg.msg_id)
    puts "   Deleted: #{deleted}"
  else
    puts '   No message available in queue.'
  end

  # Step 5: Verify queue is empty
  puts "\n5. Verifying queue is empty..."
  remaining = client.read(queue, vt: 1)
  puts "   Queue empty: #{remaining.nil?}"
end
