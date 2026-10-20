# frozen_string_literal: true

# Example: Minimal Consumer - single queue
#
# Demonstrates PGMQ::Consumer, the single-threaded consumer loop, on one queue:
# 1. Process messages with `each` (deleted after the block returns)
# 2. Retry a failing message after its visibility timeout, then give up via max_attempts
# 3. Stop gracefully on SIGTERM from a signal trap
#

ExampleHelper.run_example("Minimal Consumer - single queue") do |client, queues, _interrupted|
  queue = ExampleHelper.unique_queue_name("consumer")
  queues << queue
  client.create(queue)

  # 1. Basic processing loop
  5.times { |i| client.produce(queue, ExampleHelper.to_json({ order_id: i })) }

  consumer = PGMQ::Consumer.new(client, queue, vt: 30, batch_size: 2, max_poll_seconds: 1)
  processed = []

  consumer.each do |message|
    processed << ExampleHelper.parse_message(message)["order_id"]
    consumer.stop if processed.size == 5
  end

  puts "Processed: #{processed.inspect}"
  raise "Expected orders 0..4, got #{processed.inspect}" unless processed == [0, 1, 2, 3, 4]
  raise "Queue should be empty" unless client.read(queue, vt: 0).nil?

  # 2. Retries and max_attempts
  poison_id = client.produce(queue, ExampleHelper.to_json({ poison: true }))
  attempts = 0

  2.times do
    retrying = PGMQ::Consumer.new(client, queue, vt: 1, max_poll_seconds: 2, max_attempts: 2)

    begin
      retrying.each do |_message|
        attempts += 1
        raise "processing failed"
      end
    rescue RuntimeError => e
      puts "Attempt #{attempts} failed: #{e.message}"
    end
  end

  sleep 1.1
  giving_up = PGMQ::Consumer.new(client, queue, vt: 1, max_poll_seconds: 1, max_attempts: 2)
  Thread.new do
    sleep 1.5
    giving_up.stop
  end
  giving_up.each { raise "message over max_attempts must not be yielded" }

  archived = client.connection.with_connection do |conn|
    conn.exec("SELECT msg_id FROM pgmq.a_#{queue}").map { |row| row["msg_id"] }
  end

  puts "Archived after #{attempts} failed attempts: #{archived.inspect}"
  raise "Expected 2 attempts, got #{attempts}" unless attempts == 2
  raise "Poison message should be archived" unless archived == [poison_id]

  # 3. Graceful shutdown on SIGTERM in a separate process
  3.times { |i| client.produce(queue, ExampleHelper.to_json({ job: i })) }

  reader, writer = IO.pipe

  pid = fork do
    reader.close
    worker_client = ExampleHelper.create_client(pool_size: 1)
    worker = PGMQ::Consumer.new(worker_client, queue, max_poll_seconds: 1)

    trap("TERM") { worker.stop }

    worker.each do |message|
      writer.puts(ExampleHelper.parse_message(message)["job"])
      writer.flush
    end

    worker_client.close
    writer.puts("stopped")
    exit!(0)
  end

  writer.close
  jobs = Array.new(3) { reader.gets.to_i }
  Process.kill("TERM", pid)
  _, status = Process.wait2(pid)
  last_line = reader.gets&.strip
  reader.close

  puts "Worker processed #{jobs.inspect}, then #{last_line} (exit #{status.exitstatus})"
  raise "Expected jobs 0..2, got #{jobs.inspect}" unless jobs == [0, 1, 2]
  raise "Worker did not stop gracefully" unless status.success? && last_line == "stopped"
  raise "Queue should be empty" unless client.read(queue, vt: 0).nil?
end
