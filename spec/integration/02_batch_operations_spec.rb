#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Batch Operations
#
# Demonstrates efficient batch operations for high-throughput scenarios:
# - produce_batch: Send multiple messages in one call
# - read_batch: Read multiple messages at once
# - delete_batch: Delete multiple messages efficiently
#
# Run: bundle exec ruby examples/02_batch_operations_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Batch Operations') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('batch')
  queues << queue

  client.create(queue)

  # Produce batch
  messages = (1..10).map { |i| ExampleHelper.to_json({ item_id: i, price: i * 10.0 }) }
  msg_ids = client.produce_batch(queue, messages)
  puts "Produced #{msg_ids.size} messages"

  # Read batch
  batch = client.read_batch(queue, vt: 30, qty: 5)
  puts "Read #{batch.size} messages"

  # Delete batch
  deleted = client.delete_batch(queue, batch.map(&:msg_id))
  puts "Deleted #{deleted.size} messages"

  # Clean up remaining
  remaining = client.read_batch(queue, vt: 1, qty: 10)
  client.delete_batch(queue, remaining.map(&:msg_id)) if remaining.any?
end
