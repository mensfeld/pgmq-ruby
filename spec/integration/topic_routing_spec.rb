# frozen_string_literal: true

# Example: Topic Routing (AMQP-like patterns)
#
# Demonstrates PGMQ's topic routing feature for content-based message routing:
# - bind_topic: Subscribe a queue to a topic pattern
# - produce_topic: Send messages via routing key
# - Pattern wildcards: * (one word), # (zero or more words)
#
# Run: bundle exec ruby spec/integration/topic_routing_spec.rb

require_relative "support/example_helper"

ExampleHelper.run_example("Topic Routing (AMQP-like)") do |client, queues, interrupted|
  # Check if PGMQ supports topic routing
  version_result = client.connection.with_connection do |conn|
    conn.exec("SELECT extversion FROM pg_extension WHERE extname = 'pgmq'")
  end
  version_str = version_result[0]["extversion"].gsub(/^v/, "")
  version_parts = version_str.split(".").map(&:to_i)
  supports_topic_routing = version_parts[0] > 1 || (version_parts[0] == 1 && version_parts[1] >= 11)
  unless supports_topic_routing
    puts "⚠️  Skipping: PGMQ v1.11.0+ required for topic routing (current: #{version_str})"
    break
  end

  # Create queues for different order types
  order_new = ExampleHelper.unique_queue_name("orders_new")
  order_update = ExampleHelper.unique_queue_name("orders_update")
  order_all = ExampleHelper.unique_queue_name("orders_all")
  audit_log = ExampleHelper.unique_queue_name("audit")
  queues.concat([order_new, order_update, order_all, audit_log])

  [order_new, order_update, order_all, audit_log].each { |q| client.create(q) }

  # Bind topic patterns
  puts "Setting up topic bindings..."
  client.bind_topic("orders.new", order_new)
  client.bind_topic("orders.update", order_update)
  client.bind_topic("orders.*", order_all)        # Single-word wildcard
  client.bind_topic("#", audit_log)               # Catch-all for audit

  break if interrupted.call

  # List bindings
  puts "\nCurrent topic bindings:"
  client.list_topic_bindings.each do |binding|
    puts "  #{binding[:pattern]} -> #{binding[:queue_name]}"
  end

  break if interrupted.call

  # Send messages via routing keys
  puts "\nSending messages via topic routing..."

  count = client.produce_topic("orders.new", ExampleHelper.to_json({ order_id: 1, action: "created" }))
  puts "  orders.new -> delivered to #{count} queue(s)"

  count = client.produce_topic("orders.update", ExampleHelper.to_json({ order_id: 1, action: "updated" }))
  puts "  orders.update -> delivered to #{count} queue(s)"

  break if interrupted.call

  # Check each queue
  puts "\nReading messages from queues:"

  [
    [order_new, "orders_new"],
    [order_update, "orders_update"],
    [order_all, "orders_all"],
    [audit_log, "audit_log"]
  ].each do |queue, label|
    break if interrupted.call

    messages = client.read_batch(queue, vt: 30, qty: 10)
    puts "  #{label}: #{messages.size} message(s)"
    messages.each do |msg|
      puts "    - #{msg.message}"
      client.delete(queue, msg.msg_id)
    end
  end

  break if interrupted.call

  # Test routing (debugging tool)
  puts "\nTest routing for 'orders.new':"
  client.test_routing("orders.new").each do |match|
    puts "  Pattern: #{match[:pattern]} -> Queue: #{match[:queue_name]}"
  end

  # Cleanup bindings
  puts "\nCleaning up bindings..."
  client.unbind_topic("orders.new", order_new)
  client.unbind_topic("orders.update", order_update)
  client.unbind_topic("orders.*", order_all)
  client.unbind_topic("#", audit_log)

  puts "Topic routing example complete!"
end
