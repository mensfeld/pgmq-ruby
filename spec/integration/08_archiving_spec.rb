#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Message Archiving
#
# Demonstrates archive vs delete:
# - delete: Permanently removes message
# - archive: Moves to archive table for audit/analysis
#
# Run: bundle exec ruby examples/08_archiving_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Message Archiving') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('archive')
  queues << queue

  client.create(queue)

  3.times { |i| client.produce(queue, ExampleHelper.to_json({ order_id: 1000 + i })) }
  puts 'Produced 3 messages'

  break if interrupted.call

  # Archive a message (moves to archive table)
  msg = client.read(queue, vt: 30)
  if msg
    client.archive(queue, msg.msg_id)
    puts "Archived message #{msg.msg_id} (available in pgmq.a_#{queue})"
  end

  break if interrupted.call

  # Archive batch
  messages = client.read_batch(queue, vt: 30, qty: 2)
  if messages.any?
    archived = client.archive_batch(queue, messages.map(&:msg_id))
    puts "Archived #{archived.size} more messages"
  end

  metrics = client.metrics(queue)
  puts "Queue length: #{metrics&.queue_length}, Total ever: #{metrics&.total_messages}"
end
