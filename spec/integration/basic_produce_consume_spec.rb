# frozen_string_literal: true

# Example: Basic Produce and Consume
#
# Demonstrates the fundamental PGMQ workflow:
# 1. Create a queue
# 2. Produce (send) a message
# 3. Read the message with visibility timeout
# 4. Delete the message after processing
#
# Run: bundle exec ruby spec/integration/basic_produce_consume_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Basic Produce/Consume') do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name('basic')
  queues << queue

  client.create(queue)
  puts "Queue created: #{queue}"

  # Produce a message
  message = { order_id: 12_345, status: 'pending', amount: 99.99 }
  msg_id = client.produce(queue, ExampleHelper.to_json(message))
  puts "Produced message ID: #{msg_id}"

  # Read with 30s visibility timeout
  msg = client.read(queue, vt: 30)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "Read: order_id=#{data['order_id']}, amount=$#{data['amount']}"

    # Delete after processing
    client.delete(queue, msg.msg_id)
    puts 'Message deleted'
  end
end
