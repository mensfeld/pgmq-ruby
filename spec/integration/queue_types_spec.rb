# frozen_string_literal: true

# Example: Queue Types
#
# Demonstrates different queue types:
# - Standard (create): Durable, crash-safe
# - Unlogged (create_unlogged): Fast, no WAL, not crash-safe
# - Partitioned (create_partitioned): For high-volume queues
#
# Run: bundle exec ruby spec/integration/queue_types_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Queue Types') do |client, queues, _interrupted|
  std = ExampleHelper.unique_queue_name('standard')
  unlog = ExampleHelper.unique_queue_name('unlogged')

  # Standard queue (default)
  client.create(std)
  queues << std
  puts "Created STANDARD queue: durable, WAL-logged"

  # Unlogged queue (faster, not crash-safe)
  client.create_unlogged(unlog)
  queues << unlog
  puts "Created UNLOGGED queue: fast, no WAL, ephemeral"

  # Partitioned queue (requires pg_partman extension)
  begin
    part = ExampleHelper.unique_queue_name('partitioned')
    client.create_partitioned(part)
    queues << part
    puts "Created PARTITIONED queue: auto-partitioned by msg_id"
  rescue StandardError
    puts 'PARTITIONED queue: requires pg_partman extension (skipped)'
  end

  # All queue types work identically
  [std, unlog].each do |q|
    client.produce(q, ExampleHelper.to_json({ test: true }))
    msg = client.read(q, vt: 30)
    client.delete(q, msg.msg_id) if msg
  end
  puts 'All queue types use identical API'
end
