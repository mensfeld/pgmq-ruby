#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Message Archiving
#
# Demonstrates the difference between delete and archive operations:
# - delete: Permanently removes message from queue
# - archive: Moves message to archive table for audit/analysis
# - archive_batch: Archive multiple messages efficiently
# - detach_archive: Disconnect archive table for separate management
#
# Archiving is useful for audit trails, debugging, and analytics
# while keeping the main queue lean.
#
# Run: bundle exec ruby examples/08_archiving.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Message Archiving') do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name('archive')
  queues << queue

  # Create queue
  puts "1. Creating queue: #{queue}"
  client.create(queue)
  puts '   Queue created.'
  puts '   Note: PGMQ automatically creates an archive table for each queue.'

  # Step 2: Produce messages
  puts "\n2. Producing messages..."
  5.times do |i|
    client.produce(queue, ExampleHelper.to_json({
      order_id: 1000 + i,
      customer: "Customer #{i + 1}",
      total: (i + 1) * 100.0
    }))
  end
  puts '   Produced 5 order messages.'

  break if interrupted.call

  # Step 3: Archive a single message
  puts "\n3. Archiving a single message..."
  msg = client.read(queue, vt: 30)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Read order #{data['order_id']}"

    archived = client.archive(queue, msg.msg_id)
    puts "   Archived: #{archived}"
    puts '   Message moved to archive table (not deleted permanently)'
  end

  break if interrupted.call

  # Step 4: Archive batch of messages
  puts "\n4. Archiving batch of messages..."
  messages = client.read_batch(queue, vt: 30, qty: 3)
  puts "   Read #{messages.size} messages"

  if messages.any?
    archived_ids = client.archive_batch(queue, messages.map(&:msg_id))
    puts "   Archived #{archived_ids.size} messages"
    archived_ids.each { |id| puts "     - Archived ID: #{id}" }
  end

  break if interrupted.call

  # Step 5: Delete remaining message (for comparison)
  puts "\n5. Deleting remaining message (for comparison)..."
  msg = client.read(queue, vt: 30)
  if msg
    data = ExampleHelper.parse_message(msg)
    puts "   Read order #{data['order_id']}"

    deleted = client.delete(queue, msg.msg_id)
    puts "   Deleted: #{deleted}"
    puts '   Message permanently removed (not in archive)'
  end

  # Step 6: Show queue metrics
  puts "\n6. Queue metrics after operations..."
  metrics = client.metrics(queue)
  if metrics
    puts "   Queue length: #{metrics.queue_length}"
    puts "   Total messages ever: #{metrics.total_messages}"
  end

  # Step 7: Explain archive table
  puts "\n7. Archive table information:"
  puts "   Archive table name: pgmq.a_#{queue}"
  puts '   Contains: All archived messages with original metadata'
  puts '   Use case: Audit trails, compliance, debugging, analytics'

  # Step 8: Demonstrate detach_archive
  puts "\n8. Demonstrating detach_archive..."
  puts '   detach_archive disconnects the archive table from the queue.'
  puts '   The archive table becomes a standalone table for separate management.'
  client.detach_archive(queue)
  puts '   Archive detached successfully.'
  puts '   Note: After detaching, new archives go to a fresh archive table.'
end
