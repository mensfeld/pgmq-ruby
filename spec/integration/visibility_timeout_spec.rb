# frozen_string_literal: true

# Example: Visibility Timeout Management
#
# Demonstrates extending processing time for long-running tasks:
# - set_vt: Extend visibility timeout for a message
# - Heartbeat pattern: Periodically extend VT during long processing
#
# Run: bundle exec ruby spec/integration/visibility_timeout_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Visibility Timeout Management') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('vt')
  queues << queue

  client.create(queue)
  client.produce(queue, ExampleHelper.to_json({ task: 'long_running' }))

  # Read with short VT
  msg = client.read(queue, vt: 5)
  puts "Read message, VT expires: #{msg.vt}"

  # Extend VT
  updated = client.set_vt(queue, msg.msg_id, vt_offset: 30)
  puts "Extended VT to: #{updated.vt}"

  client.delete(queue, msg.msg_id)

  break if interrupted.call

  # Heartbeat pattern demo
  client.produce(queue, ExampleHelper.to_json({ task: 'heartbeat_demo' }))
  msg = client.read(queue, vt: 2)

  puts 'Simulating long task with heartbeats...'
  3.times do |i|
    break if interrupted.call

    sleep 0.5
    client.set_vt(queue, msg.msg_id, vt_offset: 2)
    puts "  Heartbeat #{i + 1}: VT extended"
  end

  client.delete(queue, msg.msg_id)
  puts 'Task complete'
end
