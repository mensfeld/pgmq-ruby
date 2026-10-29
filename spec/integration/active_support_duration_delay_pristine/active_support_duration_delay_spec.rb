# frozen_string_literal: true

# Example: ActiveSupport delays
#
# Runs in its own bundle with real activesupport (not in the main Gemfile):
# - produce, produce_batch, produce_topic and produce_batch_topic accept a Duration delay
# - a whole-second (5.minutes) and a fractional (1.5.seconds) Duration
# - an absolute ActiveSupport::TimeWithZone delay
#

require "active_support"
require "active_support/time"

Time.zone = "Europe/Warsaw"

ExampleHelper.run_example("ActiveSupport Delays") do |client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("as_delay")
  queues << queue
  client.create(queue)

  routing_key = "as_delay.#{SecureRandom.hex(4)}"
  client.bind_topic(routing_key, queue)

  visibility_delay = lambda do |msg_id|
    result = client.with_connection do |conn|
      conn.exec_params(
        "SELECT EXTRACT(EPOCH FROM vt - enqueued_at)::float AS delay FROM pgmq.q_#{queue} WHERE msg_id = $1",
        [msg_id]
      )
    end

    result[0]["delay"].to_f
  end

  check = lambda do |label, expected, msg_ids|
    msg_ids.each do |msg_id|
      delay = visibility_delay.call(msg_id)
      raise "#{label}: expected #{expected}s delay, got #{delay}s" unless delay >= expected && delay < expected + 0.25
    end

    puts "#{label}: #{expected}s"
  end

  messages = [ExampleHelper.to_json({ id: 1 }), ExampleHelper.to_json({ id: 2 })]
  headers = ExampleHelper.to_json({ trace_id: "as-1" })

  [5.minutes, 1.5.seconds].each do |delay|
    expected = delay.to_f

    check.call("produce(#{delay.inspect})", expected, [client.produce(queue, messages.first, delay: delay)])
    check.call("produce_batch(#{delay.inspect})", expected, client.produce_batch(queue, messages, delay: delay))

    client.produce_topic(routing_key, messages.first, delay: delay)
    client.produce_topic(routing_key, messages.first, headers: headers, delay: delay)
    msg_ids = client.with_connection do |conn|
      conn.exec("SELECT msg_id FROM pgmq.q_#{queue} ORDER BY msg_id DESC LIMIT 2").map { |row| row["msg_id"] }
    end
    check.call("produce_topic(#{delay.inspect})", expected, msg_ids)

    results = client.produce_batch_topic(routing_key, messages, delay: delay)
    check.call("produce_batch_topic(#{delay.inspect})", expected, results.map { |r| r[:msg_id] })

    break if interrupted.call
  end

  visible_at = 10.minutes.from_now
  raise "expected a TimeWithZone, got #{visible_at.class}" unless visible_at.instance_of?(ActiveSupport::TimeWithZone)

  msg_id = client.produce(queue, messages.first, delay: visible_at)
  vt = client.with_connection do |conn|
    conn.exec_params("SELECT vt FROM pgmq.q_#{queue} WHERE msg_id = $1", [msg_id])[0]["vt"]
  end
  raise "TimeWithZone: expected vt #{visible_at.utc.iso8601(6)}, got #{vt}" unless (Time.parse(vt) - visible_at).abs < 0.001

  puts "produce(#{visible_at.class}): #{vt}"

  raise "a delayed message is visible" if client.read(queue, vt: 30)

  client.unbind_topic(routing_key, queue)
end
