# frozen_string_literal: true

# Example: Concurrency Overlap Checks
#
# Proves concurrent execution by asserting that execution intervals overlap,
# not by relying on execution order or absolute timings:
# - Threads overlap on server-side long polls
# - Fibers overlap under the Fiber Scheduler without blocking the reactor
# - A pool of one serializes work inside the checkout
# - A transactional client refuses use from another thread or fiber
#

require_relative "support/fiber_scheduler_helper"

# Test 1: Threads overlap on long polls
ExampleHelper.run_example("Concurrency - Thread Overlap") do |_, queues, _|
  workers = 4
  client = ExampleHelper.create_client(pool_size: workers)
  queue = ExampleHelper.unique_queue_name("overlap_threads")
  queues << queue
  client.create(queue)

  tracker = FiberSchedulerHelper.create_tracker(workers)

  workers.times.map do |i|
    Thread.new do
      tracker.track(i) { client.read_with_poll(queue, vt: 30, max_poll_seconds: 1, poll_interval_ms: 100) }
    end
  end.each(&:join)

  pairs = workers * (workers - 1) / 2
  overlaps = tracker.overlapping_operations.size
  raise "Expected all #{pairs} thread pairs to overlap, got #{overlaps}" unless overlaps == pairs

  client.close
  puts "Thread overlap: #{tracker.summary}"
end

# Test 2: Fibers overlap on long polls and LISTEN waits without blocking the reactor
ExampleHelper.run_example("Concurrency - Fiber Overlap") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 4)
    queue = ExampleHelper.unique_queue_name("overlap_fibers")
    queues << queue
    client.create(queue)

    tracker = FiberSchedulerHelper.create_tracker(4)
    ticks = 0
    done = false

    ticker = task.async do
      until done
        sleep(0.05)
        ticks += 1
      end
    end

    waiters = 2.times.map do |i|
      task.async do
        tracker.track("poll_#{i}") do
          client.read_with_poll(queue, vt: 30, max_poll_seconds: 1, poll_interval_ms: 100)
        end
      end
    end

    waiters += 2.times.map do |i|
      task.async { tracker.track("notify_#{i}") { client.wait_for_notify(queue, timeout: 1) } }
    end

    waiters.each(&:wait)
    done = true
    ticker.wait

    overlaps = tracker.overlapping_operations.size
    raise "Expected all 6 fiber pairs to overlap, got #{overlaps}" unless overlaps == 6
    raise "Reactor was blocked: ticker never ran" unless ticks.positive?

    client.close
    puts "Fiber overlap: #{tracker.summary}, ticks: #{ticks}"
  end
end

# Test 3: A pool of one serializes work (intervals recorded inside the checkout)
ExampleHelper.run_example("Concurrency - Pool Contention") do |_, _, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 1, pool_timeout: 5)
    tracker = FiberSchedulerHelper.create_tracker(3)

    3.times.map do |i|
      task.async do
        client.with_connection do |conn|
          tracker.track(i) { conn.exec("SELECT pg_sleep(0.2)") }
        end
      end
    end.each(&:wait)

    raise "Expected 3 operations, got #{tracker.timings.size}" unless tracker.timings.size == 3

    overlaps = tracker.overlapping_operations
    raise "Pool of one must serialize work, got overlaps: #{overlaps}" unless overlaps.empty?

    client.close
    puts "Pool contention: #{tracker.summary}"
  end
end

# Test 4: A transactional client is bound to the fiber that opened the transaction
ExampleHelper.run_example("Concurrency - Transaction Ownership") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 2)
    queue = ExampleHelper.unique_queue_name("overlap_txn")
    queues << queue
    client.create(queue)

    errors = []

    begin
      client.transaction do |txn|
        txn.produce(queue, ExampleHelper.to_json({ from: "owner" }))

        task.async do
          txn.produce(queue, ExampleHelper.to_json({ from: "child_fiber" }))
        rescue PGMQ::Errors::TransactionError => e
          errors << e
        end.wait

        Thread.new do
          txn.produce(queue, ExampleHelper.to_json({ from: "child_thread" }))
        rescue PGMQ::Errors::TransactionError => e
          errors << e
        end.join

        raise "Rollback"
      end
    rescue PGMQ::Errors::ConnectionError
      nil
    end

    raise "Expected 2 TransactionErrors, got #{errors.size}" unless errors.size == 2

    remaining = client.read_batch(queue, vt: 30, qty: 10)
    raise "Expected rollback to leave 0 messages, got #{remaining.size}" unless remaining.empty?

    client.close
    puts "Transaction ownership: child fiber and child thread rejected, nothing committed"
  end
end
