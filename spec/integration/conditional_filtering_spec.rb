# frozen_string_literal: true

# Example: Conditional Filtering
#
# Demonstrates server-side JSONB filtering:
# - Filter messages by payload fields
# - Multiple conditions use AND logic
# - Works with read, read_batch, and read_with_poll
#
# Run: bundle exec ruby spec/integration/conditional_filtering_spec.rb

require_relative "support/example_helper"

ExampleHelper.run_example("Conditional Filtering") do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("filter")
  queues << queue

  client.create(queue)

  # Produce messages with different attributes
  [
    { type: "order", priority: "high", region: "us-east" },
    { type: "order", priority: "low", region: "us-west" },
    { type: "notification", priority: "high", region: "us-east" },
    { type: "order", priority: "high", region: "eu-west" }
  ].each { |m| client.produce(queue, ExampleHelper.to_json(m)) }
  puts "Produced 4 messages with different attributes"

  break if interrupted.call

  # Filter by single condition
  high = client.read_batch(queue, vt: 30, qty: 10, conditional: { priority: "high" })
  puts "Filter priority=high: #{high.size} messages"
  client.delete_batch(queue, high.map(&:msg_id))

  break if interrupted.call

  # Filter by multiple conditions (AND)
  orders_west = client.read_batch(queue, vt: 30, qty: 10, conditional: { type: "order" })
  puts "Filter type=order: #{orders_west.size} messages"
  client.delete_batch(queue, orders_west.map(&:msg_id)) if orders_west.any?
end
