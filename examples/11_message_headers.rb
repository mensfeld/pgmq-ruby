#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Message Headers
#
# Demonstrates using headers for message metadata and routing:
# - Headers are stored separately from the message payload
# - Useful for tracing, routing, and metadata without modifying payload
# - Headers are JSONB and can contain any structured data
#
# Use cases: distributed tracing, message routing, priority handling,
# tenant isolation, and audit metadata.
#
# Run: bundle exec ruby examples/11_message_headers.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Message Headers') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('headers')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'

  # Step 2: Produce message with headers
  puts "\n2. Producing message with headers..."
  message = { order_id: 12_345, items: %w[item1 item2], total: 99.99 }
  headers = {
    trace_id: 'abc-123-xyz',
    span_id: 'span-456',
    source: 'web-checkout',
    priority: 'high',
    tenant_id: 'tenant-001',
    timestamp: Time.now.to_s
  }

  msg_id = client.produce(
    queue,
    ExampleHelper.to_json(message),
    headers: ExampleHelper.to_json(headers)
  )
  puts "   Message ID: #{msg_id}"
  puts "   Payload: #{message.inspect}"
  puts "   Headers: #{headers.inspect}"

  break if interrupted.call

  # Step 3: Read message and access headers
  puts "\n3. Reading message with headers..."
  msg = client.read(queue, vt: 30)
  if msg
    puts '   Message received:'
    puts "     msg_id: #{msg.msg_id}"
    puts "     message (payload): #{msg.message}"
    puts "     headers: #{msg.headers}"

    # Parse headers for processing
    if msg.headers
      parsed_headers = JSON.parse(msg.headers)
      puts "\n   Parsed headers:"
      puts "     trace_id: #{parsed_headers['trace_id']}"
      puts "     priority: #{parsed_headers['priority']}"
      puts "     tenant_id: #{parsed_headers['tenant_id']}"
    end

    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Step 4: Batch produce with headers
  puts "\n4. Batch producing with individual headers..."
  messages = [
    ExampleHelper.to_json({ event: 'user.created', user_id: 1 }),
    ExampleHelper.to_json({ event: 'user.updated', user_id: 2 }),
    ExampleHelper.to_json({ event: 'user.deleted', user_id: 3 })
  ]
  headers_batch = [
    ExampleHelper.to_json({ trace_id: 'trace-001', priority: 'normal' }),
    ExampleHelper.to_json({ trace_id: 'trace-002', priority: 'high' }),
    ExampleHelper.to_json({ trace_id: 'trace-003', priority: 'low' })
  ]

  msg_ids = client.produce_batch(queue, messages, headers: headers_batch)
  puts "   Produced #{msg_ids.size} messages with individual headers"

  break if interrupted.call

  # Step 5: Read batch and show headers
  puts "\n5. Reading batch to show different headers..."
  batch = client.read_batch(queue, vt: 30, qty: 10)
  puts "   Read #{batch.size} messages:"
  batch.each do |msg|
    payload = ExampleHelper.parse_message(msg)
    hdrs = msg.headers ? JSON.parse(msg.headers) : {}
    puts "     - Event: #{payload['event']}, Priority: #{hdrs['priority']}, Trace: #{hdrs['trace_id']}"
  end

  # Clean up
  client.delete_batch(queue, batch.map(&:msg_id))

  # Step 6: Demonstrate routing use case
  puts "\n6. Headers use cases:"
  puts '   - Distributed tracing: trace_id, span_id, parent_span_id'
  puts '   - Message routing: route_key, destination, version'
  puts '   - Priority handling: priority, deadline, sla'
  puts '   - Multi-tenancy: tenant_id, org_id, user_id'
  puts '   - Audit: source, timestamp, correlation_id'
  puts '   - Retry handling: retry_count, original_queue, error_info'
end
