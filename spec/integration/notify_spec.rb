# frozen_string_literal: true

# Example: LISTEN/NOTIFY-based consumption
#
# Demonstrates event-driven message consumption using PostgreSQL LISTEN/NOTIFY:
# - enable_notify_insert attaches a server-side trigger that sends NOTIFY on each insert
# - wait_for_notify blocks until a notification arrives (or timeout expires)
# - Unlike read_with_poll, the connection is released as soon as the notification fires
#
# Key constraint: PostgreSQL only delivers a NOTIFY to connections that were already
# LISTENing at the time of the notification. Messages produced before LISTEN is issued
# are in the queue but trigger no wake-up. The correct pattern is to start listening
# before producers begin, then drain all available messages after each notification.
#

ExampleHelper.run_example("LISTEN/NOTIFY") do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("notify")
  queues << queue

  client.create(queue)

  # ── timeout path: empty queue returns nil ────────────────────────────────
  client.enable_notify_insert(queue)
  start = Time.now
  result = client.wait_for_notify(queue, timeout: 1)
  elapsed = (Time.now - start).round(1)
  puts "Empty queue wait: #{result.nil? ? "timed out" : "notified"} after #{elapsed}s"

  break if interrupted.call

  # ── single-message path ──────────────────────────────────────────────────
  Thread.new do
    sleep 0.3
    client.produce(queue, ExampleHelper.to_json({ event: "order_placed", id: 1 }))
  end

  start = Time.now
  client.wait_for_notify(queue, timeout: 5) do |channel, _pid, _payload|
    puts "Notified on channel: #{channel}"
  end
  elapsed = (Time.now - start).round(2)
  puts "Notification received after #{elapsed}s"
  client.purge_queue(queue)

  break if interrupted.call

  # ── consumer loop: start LISTENing before producing ──────────────────────
  # Messages must be produced AFTER wait_for_notify issues its LISTEN, otherwise
  # the notifications are missed. A background thread simulates an external producer.
  client.disable_notify_insert(queue)
  client.enable_notify_insert(queue, throttle_interval_ms: 0)

  producer = Thread.new do
    sleep 0.2
    3.times do |i|
      client.produce(queue, ExampleHelper.to_json({ job: i + 1 }))
      sleep 0.05
    end
  end

  processed = 0
  loop do
    break if interrupted.call

    next unless client.wait_for_notify(queue, timeout: 3)

    msgs = client.read_batch(queue, vt: 30, qty: 10)
    unless msgs.empty?
      client.delete_batch(queue, msgs.map(&:msg_id))
      processed += msgs.size
    end
    break if processed >= 3
  end

  producer.join
  puts "Processed #{processed} messages via LISTEN/NOTIFY"

  client.disable_notify_insert(queue)
end
