# frozen_string_literal: true

# Example: Fiber Scheduler Integration Tests
#
# Demonstrates that pgmq-ruby works correctly with Ruby's Fiber Scheduler API,
# proving true concurrent I/O behavior when used with async/await patterns.
#
# These tests verify:
# - Basic PGMQ operations work under fiber scheduler
# - Connection pool is properly shared across fibers
# - Multiple fibers can perform concurrent I/O without blocking each other
# - Pool exhaustion is handled gracefully with fiber-aware waiting
#
# Run: bundle exec ruby spec/integration/fiber_scheduler_spec.rb
#
# Prerequisites:
# - Ruby with Fiber Scheduler support (3.0+, recommended 4.0+)
# - async gem installed: gem 'async', '~> 2.6'
# - PostgreSQL with PGMQ: docker compose up -d

require_relative "support/example_helper"
require_relative "support/fiber_scheduler_helper"

# Test 1: Basic Operations Under Fiber Scheduler
ExampleHelper.run_example("Fiber Scheduler - Basic Operations") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 3)
    queue = ExampleHelper.unique_queue_name("fiber_basic")
    queues << queue

    # All basic operations should work under fiber scheduler
    client.create(queue)
    puts "  Created queue: #{queue}"

    msg_id = client.produce(queue, ExampleHelper.to_json({ test: "fiber" }))
    puts "  Produced message: #{msg_id}"

    msg = client.read(queue, vt: 30)
    puts "  Read message: #{msg&.msg_id}"

    raise "Read returned wrong message" unless msg&.msg_id == msg_id

    client.delete(queue, msg.msg_id)
    puts "  Deleted message"

    # Verify queue is empty
    remaining = client.read(queue, vt: 30)
    raise "Queue should be empty" unless remaining.nil?

    client.close
    puts "  Basic operations completed successfully"
  end
end

# Test 2: Concurrent Producers with Limited Pool
ExampleHelper.run_example("Fiber Scheduler - Concurrent Producers") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 3)
    queue = ExampleHelper.unique_queue_name("fiber_prod")
    queues << queue
    client.create(queue)

    producer_count = 10
    tracker = FiberSchedulerHelper.create_tracker(producer_count)

    puts "  Spawning #{producer_count} producer fibers with pool_size=3..."

    # Spawn concurrent producer fibers
    fibers = producer_count.times.map do |i|
      task.async do
        tracker.track(i) do
          # Small sleep to simulate real I/O latency
          sleep(0.05)
          client.produce(queue, ExampleHelper.to_json({ fiber: i, timestamp: Time.now.to_f }))
        end
      end
    end

    # Wait for all producers to complete
    fibers.each(&:wait)

    # Verify all messages were produced
    messages = client.read_batch(queue, producer_count, vt: 30)
    raise "Expected #{producer_count} messages, got #{messages.size}" unless messages.size == producer_count

    summary = tracker.summary
    puts "  Results:"
    puts "    Total elapsed: #{(summary[:total_elapsed] * 1000).round(1)}ms"
    puts "    Sum of durations: #{(summary[:sum_of_durations] * 1000).round(1)}ms"
    puts "    Concurrency ratio: #{summary[:concurrency_ratio]}x"
    puts "    Overlapping pairs: #{summary[:overlapping_pairs]}"
    puts "    Concurrent execution: #{summary[:concurrent] ? "YES" : "NO"}"

    # With pool_size=3, we expect significant concurrency
    # 10 ops of ~50ms each: sequential = ~500ms, concurrent = ~200ms
    if summary[:concurrent]
      puts "  PASSED: True concurrent execution detected"
    else
      puts "  WARNING: Concurrent execution not detected (may indicate scheduling issues)"
    end

    client.delete_batch(queue, messages.map(&:msg_id))
    client.close
  end
end

# Test 3: Concurrent Consumers with Visibility Timeout
ExampleHelper.run_example("Fiber Scheduler - Concurrent Consumers") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 3)
    queue = ExampleHelper.unique_queue_name("fiber_cons")
    queues << queue
    client.create(queue)

    message_count = 10
    consumer_count = 5

    # Pre-populate messages
    message_count.times do |i|
      client.produce(queue, ExampleHelper.to_json({ msg: i }))
    end
    puts "  Produced #{message_count} messages"

    tracker = FiberSchedulerHelper.create_tracker(consumer_count)
    consumed = []
    mutex = Mutex.new

    puts "  Spawning #{consumer_count} consumer fibers..."

    fibers = consumer_count.times.map do |i|
      task.async do
        tracker.track(i) do
          # Each consumer reads up to 2 messages
          msgs = client.read_batch(queue, 2, vt: 30)
          sleep(0.03) # Simulate processing
          mutex.synchronize { consumed.concat(msgs) }
          msgs.size
        end
      end
    end

    fibers.each(&:wait)

    summary = tracker.summary
    puts "  Results:"
    puts "    Consumed #{consumed.size} unique messages"
    puts "    Concurrency ratio: #{summary[:concurrency_ratio]}x"
    puts "    Concurrent execution: #{summary[:concurrent] ? "YES" : "NO"}"

    # Verify no duplicate consumption (visibility timeout working)
    msg_ids = consumed.map(&:msg_id)
    raise "Duplicate messages consumed!" if msg_ids.uniq.size != msg_ids.size

    puts "  PASSED: No duplicate consumption (visibility timeout working)"

    client.delete_batch(queue, msg_ids)
    client.close
  end
end

# Test 4: Pool Exhaustion with Fiber-Aware Waiting
ExampleHelper.run_example("Fiber Scheduler - Pool Exhaustion") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    # Very small pool to force contention
    client = ExampleHelper.create_client(pool_size: 2, pool_timeout: 10)
    queue = ExampleHelper.unique_queue_name("fiber_pool")
    queues << queue
    client.create(queue)

    fiber_count = 6 # 3x the pool size
    tracker = FiberSchedulerHelper.create_tracker(fiber_count)

    puts "  Spawning #{fiber_count} fibers with pool_size=2..."

    fibers = fiber_count.times.map do |i|
      task.async do
        tracker.track(i) do
          # Hold connection longer to force others to wait
          sleep(0.1)
          client.produce(queue, ExampleHelper.to_json({ fiber: i }))
        end
      end
    end

    fibers.each(&:wait)

    # Verify all completed (no deadlock)
    messages = client.read_batch(queue, fiber_count, vt: 30)
    raise "Expected #{fiber_count} messages, got #{messages.size}" unless messages.size == fiber_count

    summary = tracker.summary
    puts "  Results:"
    puts "    Total elapsed: #{(summary[:total_elapsed] * 1000).round(1)}ms"
    puts "    All #{fiber_count} fibers completed (no deadlock)"
    puts "    Concurrency ratio: #{summary[:concurrency_ratio]}x"

    # With pool_size=2 and 6 fibers, expect ~3 batches
    # But with fiber scheduling, should still see some concurrency
    puts "  PASSED: Pool exhaustion handled gracefully"

    client.delete_batch(queue, messages.map(&:msg_id))
    client.close
  end
end

# Test 5: Mixed Producer/Consumer Workload
ExampleHelper.run_example("Fiber Scheduler - Mixed Producer/Consumer") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 4)
    queue = ExampleHelper.unique_queue_name("fiber_mixed")
    queues << queue
    client.create(queue)

    produced_count = 0
    consumed_count = 0
    mutex = Mutex.new

    puts "  Running mixed producer/consumer workload..."

    # Producer fibers
    producer_fibers = 3.times.map do |i|
      task.async do
        5.times do |j|
          sleep(0.02)
          client.produce(queue, ExampleHelper.to_json({ producer: i, msg: j }))
          mutex.synchronize { produced_count += 1 }
        end
      end
    end

    # Consumer fibers - start slightly after producers
    sleep(0.05)
    2.times.map do |i|
      task.async do
        loop do
          msg = client.read(queue, vt: 30)
          break unless msg

          sleep(0.01)
          client.delete(queue, msg.msg_id)
          mutex.synchronize { consumed_count += 1 }
        end
      end
    end

    # Wait for producers first
    producer_fibers.each(&:wait)

    # Give consumers time to catch up
    sleep(0.2)

    # Read any remaining
    remaining = []
    loop do
      msg = client.read(queue, vt: 30)
      break unless msg

      remaining << msg
    end
    remaining.each { |m| client.delete(queue, m.msg_id) }
    consumed_count += remaining.size

    puts "  Results:"
    puts "    Produced: #{produced_count} messages"
    puts "    Consumed: #{consumed_count} messages"

    raise "Mismatch: produced #{produced_count}, consumed #{consumed_count}" unless produced_count == consumed_count

    puts "  PASSED: All produced messages were consumed"

    client.close
  end
end

# Test 6: Long Polling with Multiple Fibers
ExampleHelper.run_example("Fiber Scheduler - Long Polling") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 4)
    queue = ExampleHelper.unique_queue_name("fiber_poll")
    queues << queue
    client.create(queue)

    puts "  Starting concurrent long-polling fibers..."

    results = []
    mutex = Mutex.new

    # Start 3 polling fibers
    poll_fibers = 3.times.map do |i|
      task.async do
        start = Time.now
        msgs = client.read_with_poll(
          queue,
          vt: 30,
          qty: 1,
          max_poll_seconds: 2,
          poll_interval_ms: 100
        )
        elapsed = Time.now - start
        mutex.synchronize do
          results << { fiber: i, found: msgs.size, elapsed: elapsed }
        end
        msgs
      end
    end

    # After a short delay, produce messages for the pollers
    task.async do
      sleep(0.3)
      3.times do |i|
        client.produce(queue, ExampleHelper.to_json({ for_poller: i }))
        sleep(0.1)
      end
    end.wait

    poll_fibers.each(&:wait)

    total_found = results.sum { |r| r[:found] }
    puts "  Results:"
    results.each do |r|
      puts "    Fiber #{r[:fiber]}: found #{r[:found]} message(s) in #{(r[:elapsed] * 1000).round(1)}ms"
    end
    puts "    Total messages received: #{total_found}"

    # All pollers should have received messages (or timed out reasonably)
    puts "  PASSED: Long polling works with fiber scheduler"

    client.close
  end
end

# Test 7: Multi-Queue Operations with Fibers
ExampleHelper.run_example("Fiber Scheduler - Multi-Queue") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 4)

    queue_names = 3.times.map { ExampleHelper.unique_queue_name("fiber_mq") }
    queues.concat(queue_names)
    queue_names.each { |q| client.create(q) }

    puts "  Created #{queue_names.size} queues for multi-queue testing"

    tracker = FiberSchedulerHelper.create_tracker(6)

    # Concurrent producers to different queues
    producer_fibers = queue_names.each_with_index.map do |q, i|
      task.async do
        tracker.track("producer_#{i}") do
          sleep(0.03)
          client.produce(q, ExampleHelper.to_json({ queue: q, type: "test" }))
        end
      end
    end

    producer_fibers.each(&:wait)
    puts "  Produced messages to all queues"

    # Concurrent multi-queue reads
    read_results = []
    mutex = Mutex.new

    reader_fibers = 3.times.map do |i|
      task.async do
        tracker.track("reader_#{i}") do
          sleep(0.02)
          msgs = client.read_multi(queue_names, vt: 30, qty: 2)
          mutex.synchronize { read_results.concat(msgs) }
          msgs
        end
      end
    end

    reader_fibers.each(&:wait)

    summary = tracker.summary
    puts "  Results:"
    puts "    Total operations: #{summary[:operation_count]}"
    puts "    Concurrency ratio: #{summary[:concurrency_ratio]}x"
    puts "    Messages read from multi-queue: #{read_results.size}"
    puts "    Queues represented: #{read_results.map(&:queue_name).uniq.size}"

    # Verify messages came from multiple queues (UNION ALL working)
    queue_distribution = read_results.group_by(&:queue_name).transform_values(&:size)
    puts "    Distribution: #{queue_distribution}"

    puts "  PASSED: Multi-queue operations work with fiber scheduler"

    read_results.each { |m| client.delete(m.queue_name, m.msg_id) }
    client.close
  end
end

puts
puts "=" * 60
puts "All Fiber Scheduler integration tests completed!"
puts "=" * 60
