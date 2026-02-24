# frozen_string_literal: true

# Example: Fiber Scheduler Integration Tests
#
# Demonstrates that pgmq-ruby works correctly with Ruby's Fiber Scheduler API,
# proving true concurrent I/O behavior when used with async/await patterns.
#
# Run: bundle exec ruby spec/integration/fiber_scheduler_spec.rb

require_relative "support/example_helper"
require_relative "support/fiber_scheduler_helper"

# Test 1: Basic Operations Under Fiber Scheduler
ExampleHelper.run_example("Fiber Scheduler - Basic Operations") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do
    client = ExampleHelper.create_client(pool_size: 3)
    queue = ExampleHelper.unique_queue_name("fiber_basic")
    queues << queue

    client.create(queue)
    msg_id = client.produce(queue, ExampleHelper.to_json({ test: "fiber" }))
    msg = client.read(queue, vt: 30)

    raise "Read returned wrong message" unless msg&.msg_id == msg_id

    client.delete(queue, msg.msg_id)
    remaining = client.read(queue, vt: 30)

    raise "Queue should be empty" unless remaining.nil?

    client.close
    puts "Basic operations work under fiber scheduler"
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

    fibers = producer_count.times.map do |i|
      task.async do
        sleep(0.05)
        client.produce(queue, ExampleHelper.to_json({ fiber: i }))
      end
    end

    fibers.each(&:wait)

    messages = client.read_batch(queue, vt: 30, qty: producer_count)
    raise "Expected #{producer_count} messages, got #{messages.size}" unless messages.size == producer_count

    client.delete_batch(queue, messages.map(&:msg_id))
    client.close
    puts "Concurrent producers: #{producer_count} messages produced"
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

    message_count.times do |i|
      client.produce(queue, ExampleHelper.to_json({ msg: i }))
    end

    consumed = []
    mutex = Mutex.new

    fibers = consumer_count.times.map do
      task.async do
        msgs = client.read_batch(queue, vt: 30, qty: 2)
        sleep(0.03)
        mutex.synchronize { consumed.concat(msgs) }
      end
    end

    fibers.each(&:wait)

    msg_ids = consumed.map(&:msg_id)
    raise "Duplicate messages consumed!" if msg_ids.uniq.size != msg_ids.size

    client.delete_batch(queue, msg_ids)
    client.close
    puts "No duplicate consumption (visibility timeout working)"
  end
end

# Test 4: Pool Exhaustion with Fiber-Aware Waiting
ExampleHelper.run_example("Fiber Scheduler - Pool Exhaustion") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 2, pool_timeout: 10)
    queue = ExampleHelper.unique_queue_name("fiber_pool")
    queues << queue
    client.create(queue)

    fiber_count = 6

    fibers = fiber_count.times.map do |i|
      task.async do
        sleep(0.1)
        client.produce(queue, ExampleHelper.to_json({ fiber: i }))
      end
    end

    fibers.each(&:wait)

    messages = client.read_batch(queue, vt: 30, qty: fiber_count)
    raise "Expected #{fiber_count} messages, got #{messages.size}" unless messages.size == fiber_count

    client.delete_batch(queue, messages.map(&:msg_id))
    client.close
    puts "Pool exhaustion handled gracefully (no deadlock)"
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

    producer_fibers = 3.times.map do |i|
      task.async do
        5.times do |j|
          sleep(0.02)
          client.produce(queue, ExampleHelper.to_json({ producer: i, msg: j }))
          mutex.synchronize { produced_count += 1 }
        end
      end
    end

    sleep(0.05)
    2.times.map do
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

    producer_fibers.each(&:wait)
    sleep(0.2)

    loop do
      msg = client.read(queue, vt: 30)
      break unless msg

      client.delete(queue, msg.msg_id)
      consumed_count += 1
    end

    raise "Mismatch: produced #{produced_count}, consumed #{consumed_count}" unless produced_count == consumed_count

    client.close
    puts "All #{produced_count} messages consumed"
  end
end

# Test 6: Long Polling with Multiple Fibers
ExampleHelper.run_example("Fiber Scheduler - Long Polling") do |_, queues, _|
  FiberSchedulerHelper.with_scheduler do |task|
    client = ExampleHelper.create_client(pool_size: 4)
    queue = ExampleHelper.unique_queue_name("fiber_poll")
    queues << queue
    client.create(queue)

    results = []
    mutex = Mutex.new

    poll_fibers = 3.times.map do |i|
      task.async do
        msgs = client.read_with_poll(queue, vt: 30, qty: 1, max_poll_seconds: 2, poll_interval_ms: 100)
        mutex.synchronize { results << { fiber: i, found: msgs.size } }
      end
    end

    task.async do
      sleep(0.3)
      3.times { |i| client.produce(queue, ExampleHelper.to_json({ for_poller: i })) }
    end.wait

    poll_fibers.each(&:wait)

    total_found = results.sum { |r| r[:found] }
    puts "Long polling: #{total_found} messages received"

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

    producer_fibers = queue_names.map do |q|
      task.async do
        sleep(0.03)
        client.produce(q, ExampleHelper.to_json({ queue: q }))
      end
    end

    producer_fibers.each(&:wait)

    read_results = []
    mutex = Mutex.new

    reader_fibers = 3.times.map do
      task.async do
        sleep(0.02)
        msgs = client.read_multi(queue_names, vt: 30, qty: 2)
        mutex.synchronize { read_results.concat(msgs) }
      end
    end

    reader_fibers.each(&:wait)

    puts "Multi-queue: #{read_results.size} messages from #{read_results.map(&:queue_name).uniq.size} queues"

    read_results.each { |m| client.delete(m.queue_name, m.msg_id) }
    client.close
  end
end

puts
puts "All Fiber Scheduler integration tests completed!"
