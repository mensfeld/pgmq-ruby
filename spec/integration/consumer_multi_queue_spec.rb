# frozen_string_literal: true

# Example: Minimal Consumer - several queues
#
# Demonstrates PGMQ::Consumer reading several queues with one consumer:
# 1. Process messages from all queues with `each` (each deleted on its own queue)
# 2. Retry a failing message, then archive it on its own queue via max_attempts
# 3. Stop gracefully on SIGTERM from a signal trap
#

ExampleHelper.run_example("Minimal Consumer - several queues") do |client, queues, _interrupted|
  orders = ExampleHelper.unique_queue_name("consumer_orders")
  emails = ExampleHelper.unique_queue_name("consumer_emails")
  queues.push(orders, emails)
  client.create(orders)
  client.create(emails)

  # Rows still in a table, including messages hidden by their visibility timeout
  count = lambda do |table|
    client.connection.with_connection do |conn|
      conn.exec("SELECT count(*) FROM pgmq.#{table}")[0]["count"].to_i
    end
  end

  # 1. Basic processing loop over both queues
  3.times { |i| client.produce(orders, ExampleHelper.to_json({ n: i })) }
  3.times { |i| client.produce(emails, ExampleHelper.to_json({ n: i })) }

  consumer = PGMQ::Consumer.new(client, [orders, emails], vt: 30, batch_size: 2, max_poll_seconds: 1)
  received = []

  consumer.each do |message|
    received << [message.queue_name, ExampleHelper.parse_message(message)["n"]]
    consumer.stop if received.size == 6
  end

  puts "Received: #{received.inspect}"
  expected = [orders, emails].flat_map { |name| [0, 1, 2].map { |n| [name, n] } }
  raise "Expected #{expected.inspect}, got #{received.inspect}" unless received.sort == expected.sort
  raise "Queues should be empty" unless count.call("q_#{orders}").zero? && count.call("q_#{emails}").zero?

  # 2. Retries and max_attempts: a poison message in emails does not block orders
  poison_id = client.produce(emails, ExampleHelper.to_json({ poison: true }))
  attempts = 0

  2.times do
    retrying = PGMQ::Consumer.new(client, [orders, emails], vt: 1, max_poll_seconds: 2, max_attempts: 2)

    begin
      retrying.each do |_message|
        attempts += 1
        raise "processing failed"
      end
    rescue RuntimeError => e
      puts "Attempt #{attempts} failed: #{e.message}"
    end
  end

  order_id = client.produce(orders, ExampleHelper.to_json({ n: 3 }))
  sleep 1.1
  giving_up = PGMQ::Consumer.new(client, [orders, emails], vt: 1, max_poll_seconds: 1, max_attempts: 2)
  healthy = []
  Thread.new do
    sleep 1.5
    giving_up.stop
  end

  giving_up.each do |message|
    raise "message over max_attempts must not be yielded" if message.queue_name == emails

    healthy << message.msg_id
  end

  archived = client.connection.with_connection do |conn|
    conn.exec("SELECT msg_id FROM pgmq.a_#{emails}").map { |row| row["msg_id"] }
  end

  puts "Archived after #{attempts} failed attempts: #{archived.inspect}, processed orders: #{healthy.inspect}"
  raise "Expected 2 attempts, got #{attempts}" unless attempts == 2
  raise "Poison message should be archived in #{emails}" unless archived == [poison_id]
  raise "Nothing should be archived in #{orders}" unless count.call("a_#{orders}").zero?
  raise "Expected order #{order_id} to be processed, got #{healthy.inspect}" unless healthy == [order_id]
  raise "Queues should be empty" unless count.call("q_#{orders}").zero? && count.call("q_#{emails}").zero?

  # 3. Graceful shutdown on SIGTERM in a separate process
  2.times { |i| client.produce(orders, ExampleHelper.to_json({ job: i })) }
  2.times { |i| client.produce(emails, ExampleHelper.to_json({ job: i })) }

  reader, writer = IO.pipe

  pid = fork do
    reader.close
    worker_client = ExampleHelper.create_client(pool_size: 1)
    worker = PGMQ::Consumer.new(worker_client, [orders, emails], max_poll_seconds: 1)

    trap("TERM") { worker.stop }

    worker.each do |message|
      writer.puts("#{message.queue_name}:#{ExampleHelper.parse_message(message)["job"]}")
      writer.flush
    end

    worker_client.close
    writer.puts("stopped")
    exit!(0)
  end

  writer.close
  jobs = Array.new(4) { reader.gets.strip }
  Process.kill("TERM", pid)
  _, status = Process.wait2(pid)
  last_line = reader.gets&.strip
  reader.close

  puts "Worker processed #{jobs.inspect}, then #{last_line} (exit #{status.exitstatus})"
  expected_jobs = [orders, emails].flat_map { |name| ["#{name}:0", "#{name}:1"] }
  raise "Expected #{expected_jobs.inspect}, got #{jobs.inspect}" unless jobs.sort == expected_jobs.sort
  raise "Worker did not stop gracefully" unless status.success? && last_line == "stopped"
  raise "Queues should be empty" unless count.call("q_#{orders}").zero? && count.call("q_#{emails}").zero?
end
