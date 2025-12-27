#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Batch Operations
#
# Demonstrates efficient batch operations for high-throughput scenarios:
# 1. produce_batch - Send multiple messages in one call
# 2. read_batch - Read multiple messages at once
# 3. delete_batch - Delete multiple messages efficiently
#
# Batch operations reduce network round-trips and improve performance
# when processing many messages.
#
# Run: bundle exec ruby examples/02_batch_operations.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Batch Operations') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('batch')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Produce batch of messages
  puts "\n2. Producing batch of 10 messages..."
  messages = (1..10).map do |i|
    ExampleHelper.to_json({ item_id: i, product: "Product #{i}", price: i * 10.0 })
  end

  msg_ids = client.produce_batch(queue, messages)
  puts "   Produced #{msg_ids.size} messages"
  puts "   Message IDs: #{msg_ids.first(3).join(', ')}... (showing first 3)"

  # Step 3: Read batch of messages
  puts "\n3. Reading batch of 5 messages (vt: 30s)..."
  batch = client.read_batch(queue, vt: 30, qty: 5)
  puts "   Read #{batch.size} messages:"
  batch.each do |msg|
    data = ExampleHelper.parse_message(msg)
    puts "     - ID #{msg.msg_id}: #{data['product']} ($#{data['price']})"
  end

  # Step 4: Process and delete batch
  puts "\n4. Processing and deleting batch..."
  ids_to_delete = batch.map(&:msg_id)
  deleted_ids = client.delete_batch(queue, ids_to_delete)
  puts "   Deleted #{deleted_ids.size} messages"

  # Step 5: Read remaining messages
  puts "\n5. Reading remaining messages..."
  remaining = client.read_batch(queue, vt: 30, qty: 10)
  puts "   Remaining messages: #{remaining.size}"

  # Clean up remaining
  if remaining.any?
    puts "\n6. Cleaning up remaining messages..."
    client.delete_batch(queue, remaining.map(&:msg_id))
    puts '   Deleted remaining messages.'
  end
end
