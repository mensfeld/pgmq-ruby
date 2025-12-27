#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Message Headers
#
# Demonstrates using headers for metadata:
# - Headers are stored separately from payload
# - Useful for tracing, routing, priority handling
#
# Run: bundle exec ruby examples/11_message_headers_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Message Headers') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('headers')
  queues << queue

  client.create(queue)

  # Produce with headers
  message = { order_id: 123, total: 99.99 }
  headers = { trace_id: 'abc-123', priority: 'high', source: 'web' }

  client.produce(queue, ExampleHelper.to_json(message), headers: ExampleHelper.to_json(headers))
  puts 'Produced message with headers'

  # Read and access headers
  msg = client.read(queue, vt: 30)
  if msg
    hdrs = JSON.parse(msg.headers) if msg.headers
    puts "Headers: trace_id=#{hdrs['trace_id']}, priority=#{hdrs['priority']}"
    client.delete(queue, msg.msg_id)
  end

  # Batch with individual headers
  messages = %w[event1 event2 event3].map { |e| ExampleHelper.to_json({ event: e }) }
  headers_batch = (1..3).map { |i| ExampleHelper.to_json({ trace_id: "trace-#{i}" }) }

  client.produce_batch(queue, messages, headers: headers_batch)
  puts 'Produced batch with individual headers'

  # Clean up
  loop { break unless client.pop(queue) }
end
