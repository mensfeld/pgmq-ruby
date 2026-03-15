# frozen_string_literal: true

describe PGMQ::Client::MultiQueue do
  before do
    @client = create_test_client
    @queue1 = unique_queue_name("multi1")
    @queue2 = unique_queue_name("multi2")
    @queue3 = unique_queue_name("multi3")
    ensure_test_queue(@client, @queue1)
    ensure_test_queue(@client, @queue2)
    ensure_test_queue(@client, @queue3)
  end

  after do
    [@queue1, @queue2, @queue3].each do |q|
      @client.drop_queue(q)
    rescue
      nil
    end
    @client.close
  end

  describe "single connection, single query" do
    it "reads from multiple queues in one query" do
      # Send to different queues
      @client.produce(@queue1, to_json_msg({ from: "q1" }))
      @client.produce(@queue2, to_json_msg({ from: "q2" }))
      @client.produce(@queue3, to_json_msg({ from: "q3" }))

      messages = @client.read_multi([@queue1, @queue2, @queue3], vt: 30)

      assert_equal 3, messages.size
      assert_equal [@queue1, @queue2, @queue3].sort, messages.map(&:queue_name).sort
    end

    it "returns messages with queue_name attribute" do
      @client.produce(@queue1, to_json_msg({ data: "test" }))

      messages = @client.read_multi([@queue1, @queue2], vt: 30)

      assert_equal 1, messages.size
      assert_equal @queue1, messages.first.queue_name
      assert_equal "test", JSON.parse(messages.first.message)["data"]
    end

    it "respects limit parameter" do
      5.times { @client.produce(@queue1, to_json_msg({ n: 1 })) }
      5.times { @client.produce(@queue2, to_json_msg({ n: 2 })) }

      messages = @client.read_multi([@queue1, @queue2], vt: 30, qty: 5, limit: 3)

      assert_equal 3, messages.size
    end

    it "respects qty per queue" do
      10.times { @client.produce(@queue1, to_json_msg({ n: 1 })) }
      10.times { @client.produce(@queue2, to_json_msg({ n: 2 })) }

      messages = @client.read_multi([@queue1, @queue2], vt: 30, qty: 2)

      # qty=2 means max 2 per queue, so max 4 total
      assert_operator messages.size, :<=, 4
      q1_count = messages.count { |m| m.queue_name == @queue1 }
      q2_count = messages.count { |m| m.queue_name == @queue2 }

      assert_operator q1_count, :<=, 2
      assert_operator q2_count, :<=, 2
    end

    it "returns empty array when no messages" do
      messages = @client.read_multi([@queue1, @queue2, @queue3], vt: 30)

      assert_equal [], messages
    end

    it "gets first available message from any queue with limit 1" do
      # Only send to queue2
      @client.produce(@queue2, to_json_msg({ from: "q2" }))

      messages = @client.read_multi([@queue1, @queue2, @queue3], vt: 30, limit: 1)

      assert_equal 1, messages.size
      assert_equal @queue2, messages.first.queue_name
    end
  end

  describe "validation" do
    it "requires array of queue names" do
      e = assert_raises(ArgumentError) do
        @client.read_multi("not_an_array", vt: 30)
      end
      assert_match(/must be an array/, e.message)
    end

    it "rejects empty array" do
      e = assert_raises(ArgumentError) do
        @client.read_multi([], vt: 30)
      end
      assert_match(/cannot be empty/, e.message)
    end

    it "validates all queue names" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.read_multi(["valid_queue", "invalid-queue!"], vt: 30)
      end
    end

    it "limits to 50 queues maximum" do
      many_queues = (1..51).map { |i| "queue#{i}" }

      e = assert_raises(ArgumentError) do
        @client.read_multi(many_queues, vt: 30)
      end
      assert_match(/cannot exceed 50/, e.message)
    end
  end

  describe "practical use cases" do
    it "supports round-robin processing" do
      # Simulate worker pattern: process first available from multiple queues
      @client.produce(@queue1, to_json_msg({ job: "email" }))
      @client.produce(@queue2, to_json_msg({ job: "notification" }))
      @client.produce(@queue3, to_json_msg({ job: "webhook" }))

      processed = []
      3.times do
        messages = @client.read_multi([@queue1, @queue2, @queue3], vt: 30, limit: 1)
        break if messages.empty?

        msg = messages.first
        processed << msg.queue_name
        @client.delete(msg.queue_name, msg.msg_id)
      end

      assert_equal 3, processed.size
      assert_equal 3, processed.uniq.size # All different queues
    end

    it "supports batch processing across queues" do
      # Send multiple to each queue
      5.times { @client.produce(@queue1, to_json_msg({ type: "order" })) }
      5.times { @client.produce(@queue2, to_json_msg({ type: "email" })) }
      5.times { @client.produce(@queue3, to_json_msg({ type: "sms" })) }

      # Get messages from all queues
      messages = @client.read_multi([@queue1, @queue2, @queue3], vt: 1, qty: 2, limit: 5)

      # Should get up to 5 messages total
      assert_operator messages.size, :<=, 5
      assert_operator messages.size, :>, 0

      # Collect queue names
      queues_with_messages = messages.map(&:queue_name).uniq

      # Should get messages from multiple queues
      assert_operator queues_with_messages.size, :>=, 2

      # Delete all retrieved messages
      messages.each do |msg|
        @client.delete(msg.queue_name, msg.msg_id)
      end

      # Wait for vt to expire, then check remaining
      sleep 2
      remaining = @client.read_multi([@queue1, @queue2, @queue3], vt: 1, qty: 10)

      assert_operator remaining.size, :<, 15 # Some were deleted
    end
  end

  describe "#read_multi_with_poll" do
    it "waits for messages to arrive" do
      # Start polling in background
      result = nil
      thread = Thread.new do
        result = @client.read_multi_with_poll(
          [@queue1, @queue2, @queue3],
          vt: 30,
          limit: 1,
          max_poll_seconds: 3,
          poll_interval_ms: 100
        )
      end

      # Send message after short delay
      sleep 0.5
      @client.produce(@queue2, to_json_msg({ delayed: true }))

      thread.join

      assert_equal 1, result.size
      assert_equal @queue2, result.first.queue_name
    end

    it "returns immediately if messages exist" do
      @client.produce(@queue1, to_json_msg({ immediate: true }))

      start = Time.now
      messages = @client.read_multi_with_poll(
        [@queue1, @queue2, @queue3],
        vt: 30,
        max_poll_seconds: 5
      )
      elapsed = Time.now - start

      assert_equal 1, messages.size
      assert_operator elapsed, :<, 0.5 # Should return immediately
    end

    it "times out when no messages arrive" do
      start = Time.now
      messages = @client.read_multi_with_poll(
        [@queue1, @queue2, @queue3],
        vt: 30,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start

      assert_empty messages
      assert_operator elapsed, :>=, 1.0
      assert_operator elapsed, :<, 1.5
    end

    it "respects qty and limit parameters" do
      5.times { @client.produce(@queue1, to_json_msg({ n: 1 })) }
      5.times { @client.produce(@queue2, to_json_msg({ n: 2 })) }

      messages = @client.read_multi_with_poll(
        [@queue1, @queue2, @queue3],
        vt: 30,
        qty: 2,
        limit: 3,
        max_poll_seconds: 1
      )

      assert_operator messages.size, :<=, 3
    end

    it "validates queue names array" do
      e = assert_raises(ArgumentError) do
        @client.read_multi_with_poll("not_array", vt: 30)
      end
      assert_match(/must be an array/, e.message)
    end

    it "validates non-empty array" do
      e = assert_raises(ArgumentError) do
        @client.read_multi_with_poll([], vt: 30)
      end
      assert_match(/cannot be empty/, e.message)
    end

    it "gets first available from any queue" do
      # Only queue3 has messages
      @client.produce(@queue3, to_json_msg({ only_in_q3: true }))

      messages = @client.read_multi_with_poll(
        [@queue1, @queue2, @queue3],
        vt: 30,
        limit: 1,
        max_poll_seconds: 1
      )

      assert_equal 1, messages.size
      assert_equal @queue3, messages.first.queue_name
    end
  end

  describe "#pop_multi" do
    it "pops and deletes from first available queue" do
      @client.produce(@queue2, to_json_msg({ data: "test" }))

      msg = @client.pop_multi([@queue1, @queue2, @queue3])

      refute_nil msg
      assert_equal @queue2, msg.queue_name
      assert_equal "test", JSON.parse(msg.message)["data"]

      # Verify deleted
      remaining = @client.read(@queue2, vt: 30)

      assert_nil remaining
    end

    it "returns nil when all queues empty" do
      msg = @client.pop_multi([@queue1, @queue2, @queue3])

      assert_nil msg
    end

    it "pops from first non-empty queue" do
      # Send to queue3 only
      @client.produce(@queue3, to_json_msg({ from: "q3" }))

      msg = @client.pop_multi([@queue1, @queue2, @queue3])

      assert_equal @queue3, msg.queue_name
    end

    it "validates queue names" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.pop_multi(["invalid-name!"])
      end
    end

    it "validates array input" do
      e = assert_raises(ArgumentError) do
        @client.pop_multi("not_array")
      end
      assert_match(/must be an array/, e.message)
    end

    it "validates non-empty array" do
      e = assert_raises(ArgumentError) do
        @client.pop_multi([])
      end
      assert_match(/cannot be empty/, e.message)
    end

    it "limits to 50 queues" do
      many_queues = (1..51).map { |i| "queue#{i}" }
      e = assert_raises(ArgumentError) do
        @client.pop_multi(many_queues)
      end
      assert_match(/cannot exceed 50/, e.message)
    end

    it "has queue_name attribute on returned message" do
      @client.produce(@queue1, to_json_msg({ test: "data" }))
      msg = @client.pop_multi([@queue1, @queue2])

      assert_respond_to msg, :queue_name
      assert_equal @queue1, msg.queue_name
    end
  end

  describe "#delete_multi" do
    it "deletes messages from multiple queues" do
      # Send messages
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))
      id2 = @client.produce(@queue1, to_json_msg({ n: 2 }))
      id3 = @client.produce(@queue2, to_json_msg({ n: 3 }))

      result = @client.delete_multi({
        @queue1 => [id1, id2],
        @queue2 => [id3]
      })

      assert_equal [id1, id2].sort, result[@queue1].sort
      assert_equal [id3], result[@queue2]

      # Verify deleted
      assert_nil @client.read(@queue1, vt: 30)
      assert_nil @client.read(@queue2, vt: 30)
    end

    it "returns empty hash for empty input" do
      result = @client.delete_multi({})

      assert_equal({}, result)
    end

    it "skips empty message ID arrays" do
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))

      result = @client.delete_multi({
        @queue1 => [id1],
        @queue2 => []
      })

      assert_equal [id1], result[@queue1]
      assert_nil result[@queue2]
    end

    it "validates hash input" do
      e = assert_raises(ArgumentError) do
        @client.delete_multi("not_hash")
      end
      assert_match(/must be a hash/, e.message)
    end

    it "validates all queue names" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.delete_multi({ "invalid-name!" => [1, 2] })
      end
    end

    it "is transactional (all or nothing)" do
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))
      id2 = @client.produce(@queue2, to_json_msg({ n: 2 }))

      # This should work atomically
      result = @client.delete_multi({
        @queue1 => [id1],
        @queue2 => [id2]
      })

      assert_equal 2, result.values.flatten.size
    end

    it "works with batch processing pattern" do
      5.times { @client.produce(@queue1, to_json_msg({ q: 1 })) }
      5.times { @client.produce(@queue2, to_json_msg({ q: 2 })) }

      messages = @client.read_multi([@queue1, @queue2], vt: 30, qty: 10)
      deletions = messages.group_by(&:queue_name).transform_values { |msgs| msgs.map(&:msg_id) }

      result = @client.delete_multi(deletions)

      assert_equal 10, result.values.flatten.size
    end
  end

  describe "#archive_multi" do
    it "archives messages from multiple queues" do
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))
      id2 = @client.produce(@queue2, to_json_msg({ n: 2 }))

      result = @client.archive_multi({
        @queue1 => [id1],
        @queue2 => [id2]
      })

      assert_equal [id1], result[@queue1]
      assert_equal [id2], result[@queue2]

      # Messages should be gone from main queue
      assert_nil @client.read(@queue1, vt: 30)
      assert_nil @client.read(@queue2, vt: 30)
    end

    it "returns empty hash for empty input" do
      result = @client.archive_multi({})

      assert_equal({}, result)
    end

    it "skips empty message ID arrays" do
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))

      result = @client.archive_multi({
        @queue1 => [id1],
        @queue2 => []
      })

      assert_equal [id1], result[@queue1]
      assert_nil result[@queue2]
    end

    it "validates hash input" do
      e = assert_raises(ArgumentError) do
        @client.archive_multi("not_hash")
      end
      assert_match(/must be a hash/, e.message)
    end

    it "validates all queue names" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.archive_multi({ "invalid-name!" => [1] })
      end
    end

    it "is transactional" do
      id1 = @client.produce(@queue1, to_json_msg({ n: 1 }))
      id2 = @client.produce(@queue2, to_json_msg({ n: 2 }))

      result = @client.archive_multi({
        @queue1 => [id1],
        @queue2 => [id2]
      })

      assert_equal 2, result.values.flatten.size
    end

    it "archives multiple messages from same queue" do
      ids = Array.new(3) { @client.produce(@queue1, to_json_msg({ n: 1 })) }

      result = @client.archive_multi({
        @queue1 => ids
      })

      assert_equal 3, result[@queue1].size
    end
  end
end
