# frozen_string_literal: true

# Example: Grouped Round-Robin Reading
#
# Demonstrates fair message processing across different entities:
# - read_grouped_rr: Read messages in round-robin order across groups
# - Prevents a single entity from monopolizing workers
# - Messages are grouped by the first key in their JSON payload
#
# Run: bundle exec ruby spec/integration/grouped_round_robin_spec.rb

require_relative "support/example_helper"

ExampleHelper.run_example("Grouped Round-Robin Reading") do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("grouped_rr")
  queues << queue

  client.create(queue)

  # Simulate a scenario where one user has many tasks while others have few
  puts "Sending messages from different users..."
  puts "  User A: 5 messages"
  5.times do |i|
    client.produce(queue, ExampleHelper.to_json({ user_id: "user_a", task: "task_#{i + 1}" }))
  end

  puts "  User B: 2 messages"
  2.times do |i|
    client.produce(queue, ExampleHelper.to_json({ user_id: "user_b", task: "task_#{i + 1}" }))
  end

  puts "  User C: 1 message"
  client.produce(queue, ExampleHelper.to_json({ user_id: "user_c", task: "task_1" }))

  break if interrupted.call

  # Regular read would process user_a's tasks first
  puts "\nRegular read order (FIFO - unfair):"
  regular_queue = ExampleHelper.unique_queue_name("regular")
  queues << regular_queue
  client.create(regular_queue)

  # Copy messages to regular queue
  8.times do
    msg = client.pop(queue)
    break unless msg
    client.produce(regular_queue, msg.message)
    client.produce(queue, msg.message) # Put back
  end

  4.times do |i|
    break if interrupted.call
    msg = client.pop(regular_queue)
    break unless msg
    data = JSON.parse(msg.message)
    puts "  #{i + 1}. #{data["user_id"]}: #{data["task"]}"
  end

  break if interrupted.call

  # Grouped round-robin provides fair ordering
  puts "\nGrouped round-robin order (fair):"
  messages = client.read_grouped_rr(queue, vt: 30, qty: 8)

  messages.each_with_index do |msg, i|
    break if interrupted.call
    data = JSON.parse(msg.message)
    puts "  #{i + 1}. #{data["user_id"]}: #{data["task"]}"
    client.delete(queue, msg.msg_id)
  end

  break if interrupted.call

  # Long-polling version
  puts "\nTesting grouped round-robin with polling..."
  client.produce(queue, ExampleHelper.to_json({ user_id: "test_user", task: "delayed_task" }))

  messages = client.read_grouped_rr_with_poll(
    queue,
    vt: 30,
    qty: 1,
    max_poll_seconds: 2,
    poll_interval_ms: 100
  )

  if messages.any?
    data = JSON.parse(messages.first.message)
    puts "  Got message from #{data["user_id"]}: #{data["task"]}"
    client.delete(queue, messages.first.msg_id)
  end

  puts "\nGrouped round-robin example complete!"
end
