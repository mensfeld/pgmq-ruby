# frozen_string_literal: true

# Example: LISTEN/NOTIFY-based consumption
#
# Demonstrates event-driven message consumption using PostgreSQL LISTEN/NOTIFY:
# - enable_notify_insert attaches a server-side trigger that sends NOTIFY on each insert
# - wait_for_notify blocks until a notification arrives (or timeout expires)
# - Unlike read_with_poll, the connection is released as soon as the notification fires
#
# Important: wait_for_notify is a one-shot primitive — it LISTENs, waits for one
# notification, then UNLISTENs and returns the connection to the pool. In a real
# consumer loop, after waking up you should drain all available messages with
# read_batch (a burst of inserts may produce only one NOTIFY due to throttling, and
# any extra notifications queued during the wait are discarded on UNLISTEN).
#

ExampleHelper.run_example("LISTEN/NOTIFY") do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("notify")
  queues << queue

  client.create(queue)
  client.enable_notify_insert(queue, throttle_interval_ms: 0)

  # ── timeout path: empty queue returns nil ────────────────────────────────
  start = Time.now
  result = client.wait_for_notify(queue, timeout: 1)
  elapsed = (Time.now - start).round(1)
  puts "Empty queue wait: #{result.nil? ? "timed out" : "notified"} after #{elapsed}s"

  break if interrupted.call

  # ── notification path: producer wakes up a sleeping consumer ─────────────
  # Start the consumer wait BEFORE the producer inserts — PostgreSQL only delivers
  # a NOTIFY to connections that were already LISTENing at insert time.
  producer = Thread.new do
    sleep 0.3
    client.produce(queue, ExampleHelper.to_json({ event: "order_placed" }))
  end

  start = Time.now
  result = client.wait_for_notify(queue, timeout: 5) do |channel, _pid, _payload|
    puts "Notified on channel: #{channel}"
  end
  elapsed = (Time.now - start).round(2)

  if result
    msg = client.read(queue, vt: 30)
    client.delete(queue, msg.msg_id) if msg
    puts "Woke up after #{elapsed}s, processed #{msg ? 1 : 0} message"
  else
    puts "Timed out after #{elapsed}s (no notification)"
  end

  producer.join

  client.disable_notify_insert(queue)
end
