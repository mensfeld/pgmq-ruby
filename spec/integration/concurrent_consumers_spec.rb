# frozen_string_literal: true

# Example: Concurrent Consumers in One Process
#
# Runs several PGMQ::Consumer instances in threads of one Ruby process:
# 1. Consumers sharing one client and one queue process every message exactly once
# 2. Consumers with their own clients and queues stay independent
# 3. A consumer whose block raises stops alone; the others pick up its message after vt
# 4. Stopping all consumers while they wait on empty queues returns within one poll
#

ExampleHelper.run_example("Concurrent Consumers") do |client, queues, _interrupted|
  # Waits for the consumer threads and returns their exceptions (nil for a clean stop)
  join_all = lambda do |threads, timeout|
    threads.map do |thread|
      raise "Consumer thread did not stop within #{timeout}s" unless thread.join(timeout)

      thread.value
      nil
    rescue => e
      e
    end
  end

  # 1. Shared client, shared queue
  shared_queue = ExampleHelper.unique_queue_name("concurrent_shared")
  queues << shared_queue
  client.create(shared_queue)

  total = 100
  client.produce_batch(shared_queue, Array.new(total) { |i| ExampleHelper.to_json({ n: i }) })

  shared_client = ExampleHelper.create_client(pool_size: 4)
  processed = Queue.new
  consumers = Array.new(4) do
    PGMQ::Consumer.new(shared_client, shared_queue, vt: 30, batch_size: 5, max_poll_seconds: 1, poll_interval_ms: 10)
  end

  threads = consumers.each_with_index.map do |consumer, index|
    Thread.new do
      consumer.each do |message|
        sleep 0.001
        processed << [index, ExampleHelper.parse_message(message)["n"]]
        consumers.each(&:stop) if processed.size == total
      end
    end
  end

  errors = join_all.call(threads, 10)
  results = Array.new(processed.size) { processed.pop }
  numbers = results.map(&:last)
  per_consumer = results.group_by(&:first).transform_values(&:size).sort.to_h

  puts "Shared queue: #{numbers.size} messages, per consumer: #{per_consumer.inspect}"
  raise "Consumer errors: #{errors.compact.inspect}" unless errors.compact.empty?
  raise "Duplicates or gaps: #{numbers.sort.inspect}" unless numbers.sort == (0...total).to_a
  raise "Shared queue should be empty" unless client.read(shared_queue, vt: 0).nil?

  shared_client.close

  # 2. Independent consumers: own client, own queue, own settings
  setups = [
    { name: "independent_a", count: 10, on_success: :delete, batch_size: 1 },
    { name: "independent_b", count: 20, on_success: :archive, batch_size: 4 },
    { name: "independent_c", count: 15, on_success: :delete, batch_size: 10 }
  ]

  setups.each do |setup|
    setup[:queue] = ExampleHelper.unique_queue_name(setup[:name])
    queues << setup[:queue]
    client.create(setup[:queue])
    client.produce_batch(
      setup[:queue],
      Array.new(setup[:count]) { |i| ExampleHelper.to_json({ queue: setup[:queue], n: i }) }
    )
    setup[:client] = ExampleHelper.create_client(pool_size: 1)
    setup[:received] = []
    setup[:consumer] = PGMQ::Consumer.new(
      setup[:client],
      setup[:queue],
      batch_size: setup[:batch_size],
      on_success: setup[:on_success],
      max_poll_seconds: 1,
      poll_interval_ms: 10
    )
  end

  threads = setups.map do |setup|
    Thread.new do
      setup[:consumer].each do |message|
        setup[:received] << ExampleHelper.parse_message(message)
        setup[:consumer].stop if setup[:received].size == setup[:count]
      end
    end
  end

  errors = join_all.call(threads, 10)
  raise "Consumer errors: #{errors.compact.inspect}" unless errors.compact.empty?

  setups.each do |setup|
    foreign = setup[:received].reject { |payload| payload["queue"] == setup[:queue] }
    numbers = setup[:received].map { |payload| payload["n"] }
    archived = client.connection.with_connection do |conn|
      conn.exec("SELECT count(*) AS n FROM pgmq.a_#{setup[:queue]}").first["n"].to_i
    end
    expected_archived = (setup[:on_success] == :archive) ? setup[:count] : 0

    puts "#{setup[:name]}: #{numbers.size} messages (#{setup[:on_success]}), archived: #{archived}"
    raise "#{setup[:name]} got messages of other queues: #{foreign.inspect}" unless foreign.empty?
    raise "#{setup[:name]} expected 0...#{setup[:count]} in order" unless numbers == (0...setup[:count]).to_a
    raise "#{setup[:name]} expected #{expected_archived} archived, got #{archived}" unless archived == expected_archived
    raise "#{setup[:name]} queue should be empty" unless client.read(setup[:queue], vt: 0).nil?

    setup[:client].close
  end

  # 3. Failure isolation: one consumer fails on a message, the others continue and process it after vt
  failing_queue = ExampleHelper.unique_queue_name("concurrent_failing")
  queues << failing_queue
  client.create(failing_queue)
  client.produce_batch(failing_queue, Array.new(10) { |i| ExampleHelper.to_json({ n: i }) })

  failing_client = ExampleHelper.create_client(pool_size: 3)
  done = Queue.new
  failures = Queue.new
  # Closed once consumer 0 has failed, so the healthy consumers cannot drain the queue before it reads
  failed_gate = Queue.new
  consumers = Array.new(3) do
    PGMQ::Consumer.new(failing_client, failing_queue, vt: 1, max_poll_seconds: 1, poll_interval_ms: 10)
  end

  threads = consumers.each_with_index.map do |consumer, index|
    Thread.new do
      Thread.current.report_on_exception = false
      failed_gate.pop(timeout: 5) unless index.zero?

      consumer.each do |message|
        payload = ExampleHelper.parse_message(message)

        if index.zero?
          failures << payload["n"]
          failed_gate.close
          raise "consumer 0 failed on message #{payload["n"]}"
        end

        done << payload["n"]
        consumers.each(&:stop) if done.size == 10
      end
    end
  end

  # Guards against a hang: the assertions below then report the missing messages
  watchdog = Thread.new do
    sleep 8
    consumers.each(&:stop)
  end

  errors = join_all.call(threads, 15)
  watchdog.kill
  numbers = Array.new(done.size) { done.pop }

  puts "Failure isolation: #{numbers.size} processed, #{failures.size} failure(s), " \
    "errors: #{errors.map { |e| e&.message }.inspect}"
  raise "Expected all 10 messages processed, got #{numbers.sort.inspect}" unless numbers.sort == (0...10).to_a
  raise "Only consumer 0 may fail" unless errors.drop(1).compact.empty?
  raise "Consumer 0 should fail exactly once, failed #{failures.size} times" unless failures.size == 1
  raise "Consumer 0 should have re-raised its error" unless errors.first.is_a?(RuntimeError)
  raise "Failing queue should be empty" unless client.read(failing_queue, vt: 0).nil?

  failing_client.close

  # 4. Stop all consumers while they wait on empty queues
  idle_client = ExampleHelper.create_client(pool_size: 3)
  idle_consumers = Array.new(3) do |i|
    queue = ExampleHelper.unique_queue_name("concurrent_idle_#{i}")
    queues << queue
    client.create(queue)
    PGMQ::Consumer.new(idle_client, queue, max_poll_seconds: 2, poll_interval_ms: 50)
  end

  threads = idle_consumers.map do |consumer|
    Thread.new { consumer.each { raise "empty queue must not yield" } }
  end

  sleep 0.5
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  idle_consumers.each(&:stop)
  errors = join_all.call(threads, 5)
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

  puts "Idle stop: #{idle_consumers.size} consumers returned in #{elapsed.round(2)}s"
  raise "Consumer errors: #{errors.compact.inspect}" unless errors.compact.empty?
  raise "Idle consumers took #{elapsed}s to stop" if elapsed > 3

  idle_client.close
end
