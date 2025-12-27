#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Queue Types
#
# Demonstrates different queue types available in PGMQ:
# - Standard queue (default): Durable, crash-safe
# - Unlogged queue: High speed, no WAL, not crash-safe
# - Partitioned queue: For high-volume queues with automatic partitioning
#
# Choose queue type based on your durability vs performance requirements.
#
# Run: bundle exec ruby examples/12_queue_types.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Queue Types') do |client, queues, interrupted|
  standard_queue = ExampleHelper.unique_queue_name('standard')
  unlogged_queue = ExampleHelper.unique_queue_name('unlogged')
  partitioned_queue = ExampleHelper.unique_queue_name('partitioned')

  # Step 1: Create standard queue
  puts '1. Creating STANDARD queue (default)...'
  client.create(standard_queue)
  queues << standard_queue
  puts "   Created: #{standard_queue}"
  puts '   Characteristics:'
  puts '     - Durable: Messages survive server restart'
  puts '     - WAL-logged: Full crash safety'
  puts '     - Use for: Most production workloads'

  break if interrupted.call

  # Step 2: Create unlogged queue
  puts "\n2. Creating UNLOGGED queue..."
  client.create_unlogged(unlogged_queue)
  queues << unlogged_queue
  puts "   Created: #{unlogged_queue}"
  puts '   Characteristics:'
  puts '     - NOT durable: Messages lost on crash/restart'
  puts '     - No WAL: Faster writes (2-3x performance)'
  puts '     - Use for: Caching, ephemeral data, dev/test'

  break if interrupted.call

  # Step 3: Create partitioned queue (if pg_partman available)
  puts "\n3. Creating PARTITIONED queue..."
  begin
    # Partitioned queues require pg_partman extension
    # partition_interval: messages per partition (default 10000)
    # retention_interval: partitions to retain (default 100000)
    client.create_partitioned(
      partitioned_queue,
      partition_interval: '10000',
      retention_interval: '100000'
    )
    queues << partitioned_queue
    puts "   Created: #{partitioned_queue}"
    puts '   Characteristics:'
    puts '     - Automatic partitioning by msg_id'
    puts '     - Efficient for very high message volumes'
    puts '     - Automatic old partition cleanup'
    puts '     - Use for: High-volume production queues'
  rescue StandardError => e
    # pg_partman not installed - this is expected in many environments
    puts "   Skipped: #{e.message.split("\n").first[0..60]}..."
    puts '   Note: Partitioned queues require pg_partman extension'
  end

  break if interrupted.call

  # Step 4: List queues and show their types
  puts "\n4. Listing queues with metadata..."
  queue_list = client.list_queues
  our_queues = queue_list.select { |q| queues.include?(q.queue_name) }

  our_queues.each do |q|
    type = if q.is_partitioned == 't'
             'PARTITIONED'
           elsif q.is_unlogged == 't'
             'UNLOGGED'
           else
             'STANDARD'
           end
    puts "   #{q.queue_name}: #{type}"
    puts "     Created: #{q.created_at}"
  end

  break if interrupted.call

  # Step 5: Demonstrate they all work the same way
  puts "\n5. All queue types use the same API..."
  [standard_queue, unlogged_queue].each do |q|
    next unless queues.include?(q)

    # Produce
    client.produce(q, ExampleHelper.to_json({ test: 'data', queue: q }))

    # Read
    msg = client.read(q, vt: 30)

    # Delete
    client.delete(q, msg.msg_id) if msg

    short_name = q.split('_').first
    puts "   #{short_name}: produce -> read -> delete (success)"
  end

  puts "\n6. Queue type selection guide:"
  puts '   STANDARD (create):'
  puts '     - Default choice for production'
  puts '     - Message durability required'
  puts '     - Moderate throughput (thousands/sec)'

  puts "\n   UNLOGGED (create_unlogged):"
  puts '     - Ephemeral/cacheable messages'
  puts '     - Maximum throughput needed'
  puts '     - Data loss acceptable on crash'

  puts "\n   PARTITIONED (create_partitioned):"
  puts '     - Very high message volumes'
  puts '     - Millions of messages'
  puts '     - Automatic partition management'
end
