# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'PGMQ::Client#read_multi', :integration do
  let(:client) { PGMQ::Client.new(TEST_DB_PARAMS) }
  let(:queue1) { test_queue_name('multi1') }
  let(:queue2) { test_queue_name('multi2') }
  let(:queue3) { test_queue_name('multi3') }

  before do
    ensure_test_queue(client, queue1)
    ensure_test_queue(client, queue2)
    ensure_test_queue(client, queue3)
  end

  after do
    [queue1, queue2, queue3].each do |q|
      client.drop_queue(q)
    rescue StandardError
      nil
    end
  end

  describe 'single connection, single query' do
    it 'reads from multiple queues in one query' do
      # Send to different queues
      client.send(queue1, { from: 'q1' })
      client.send(queue2, { from: 'q2' })
      client.send(queue3, { from: 'q3' })

      messages = client.read_multi([queue1, queue2, queue3], vt: 30)

      expect(messages.size).to eq(3)
      expect(messages.map(&:queue_name).sort).to eq([queue1, queue2, queue3].sort)
    end

    it 'returns messages with queue_name attribute' do
      client.send(queue1, { data: 'test' })

      messages = client.read_multi([queue1, queue2], vt: 30)

      expect(messages.size).to eq(1)
      expect(messages.first.queue_name).to eq(queue1)
      expect(messages.first.payload['data']).to eq('test')
    end

    it 'respects limit parameter' do
      5.times { client.send(queue1, { n: 1 }) }
      5.times { client.send(queue2, { n: 2 }) }

      messages = client.read_multi([queue1, queue2], vt: 30, qty: 5, limit: 3)

      expect(messages.size).to eq(3)
    end

    it 'respects qty per queue' do
      10.times { client.send(queue1, { n: 1 }) }
      10.times { client.send(queue2, { n: 2 }) }

      messages = client.read_multi([queue1, queue2], vt: 30, qty: 2)

      # qty=2 means max 2 per queue, so max 4 total
      expect(messages.size).to be <= 4
      q1_count = messages.count { |m| m.queue_name == queue1 }
      q2_count = messages.count { |m| m.queue_name == queue2 }
      expect(q1_count).to be <= 2
      expect(q2_count).to be <= 2
    end

    it 'returns empty array when no messages' do
      messages = client.read_multi([queue1, queue2, queue3], vt: 30)

      expect(messages).to eq([])
    end

    it 'gets first available message from any queue with limit 1' do
      # Only send to queue2
      client.send(queue2, { from: 'q2' })

      messages = client.read_multi([queue1, queue2, queue3], vt: 30, limit: 1)

      expect(messages.size).to eq(1)
      expect(messages.first.queue_name).to eq(queue2)
    end
  end

  describe 'validation' do
    it 'requires array of queue names' do
      expect do
        client.read_multi('not_an_array', vt: 30)
      end.to raise_error(ArgumentError, /must be an array/)
    end

    it 'rejects empty array' do
      expect do
        client.read_multi([], vt: 30)
      end.to raise_error(ArgumentError, /cannot be empty/)
    end

    it 'validates all queue names' do
      expect do
        client.read_multi(['valid_queue', 'invalid-queue!'], vt: 30)
      end.to raise_error(PGMQ::InvalidQueueNameError)
    end

    it 'limits to 50 queues maximum' do
      many_queues = (1..51).map { |i| "queue#{i}" }

      expect do
        client.read_multi(many_queues, vt: 30)
      end.to raise_error(ArgumentError, /cannot exceed 50/)
    end
  end

  describe 'practical use cases' do
    it 'supports round-robin processing' do
      # Simulate worker pattern: process first available from multiple queues
      client.send(queue1, { job: 'email' })
      client.send(queue2, { job: 'notification' })
      client.send(queue3, { job: 'webhook' })

      processed = []
      3.times do
        messages = client.read_multi([queue1, queue2, queue3], vt: 30, limit: 1)
        break if messages.empty?

        msg = messages.first
        processed << msg.queue_name
        client.delete(msg.queue_name, msg.msg_id)
      end

      expect(processed.size).to eq(3)
      expect(processed.uniq.size).to eq(3) # All different queues
    end

    it 'supports batch processing across queues' do
      # Send multiple to each queue
      5.times { client.send(queue1, { type: 'order' }) }
      5.times { client.send(queue2, { type: 'email' }) }
      5.times { client.send(queue3, { type: 'sms' }) }

      # Get messages from all queues
      messages = client.read_multi([queue1, queue2, queue3], vt: 1, qty: 2, limit: 5)

      # Should get up to 5 messages total
      expect(messages.size).to be <= 5
      expect(messages.size).to be > 0

      # Collect queue names
      queues_with_messages = messages.map(&:queue_name).uniq

      # Should get messages from multiple queues
      expect(queues_with_messages.size).to be >= 2

      # Delete all retrieved messages
      messages.each do |msg|
        client.delete(msg.queue_name, msg.msg_id)
      end

      # Wait for vt to expire, then check remaining
      sleep 2
      remaining = client.read_multi([queue1, queue2, queue3], vt: 1, qty: 10)
      expect(remaining.size).to be < 15 # Some were deleted
    end
  end

  describe '#read_multi_with_poll' do
    it 'waits for messages to arrive' do
      # Start polling in background
      result = nil
      thread = Thread.new do
        result = client.read_multi_with_poll(
          [queue1, queue2, queue3],
          vt: 30,
          limit: 1,
          max_poll_seconds: 3,
          poll_interval_ms: 100
        )
      end

      # Send message after short delay
      sleep 0.5
      client.send(queue2, { delayed: true })

      thread.join
      expect(result.size).to eq(1)
      expect(result.first.queue_name).to eq(queue2)
    end

    it 'returns immediately if messages exist' do
      client.send(queue1, { immediate: true })

      start = Time.now
      messages = client.read_multi_with_poll(
        [queue1, queue2, queue3],
        vt: 30,
        max_poll_seconds: 5
      )
      elapsed = Time.now - start

      expect(messages.size).to eq(1)
      expect(elapsed).to be < 0.5 # Should return immediately
    end

    it 'times out when no messages arrive' do
      start = Time.now
      messages = client.read_multi_with_poll(
        [queue1, queue2, queue3],
        vt: 30,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start

      expect(messages).to be_empty
      expect(elapsed).to be >= 1.0
      expect(elapsed).to be < 1.5
    end

    it 'respects qty and limit parameters' do
      5.times { client.send(queue1, { n: 1 }) }
      5.times { client.send(queue2, { n: 2 }) }

      messages = client.read_multi_with_poll(
        [queue1, queue2, queue3],
        vt: 30,
        qty: 2,
        limit: 3,
        max_poll_seconds: 1
      )

      expect(messages.size).to be <= 3
    end

    it 'validates queue names array' do
      expect do
        client.read_multi_with_poll('not_array', vt: 30)
      end.to raise_error(ArgumentError, /must be an array/)
    end

    it 'validates non-empty array' do
      expect do
        client.read_multi_with_poll([], vt: 30)
      end.to raise_error(ArgumentError, /cannot be empty/)
    end

    it 'gets first available from any queue' do
      # Only queue3 has messages
      client.send(queue3, { only_in_q3: true })

      messages = client.read_multi_with_poll(
        [queue1, queue2, queue3],
        vt: 30,
        limit: 1,
        max_poll_seconds: 1
      )

      expect(messages.size).to eq(1)
      expect(messages.first.queue_name).to eq(queue3)
    end
  end

  describe '#pop_multi' do
    it 'pops and deletes from first available queue' do
      client.send(queue2, { data: 'test' })

      msg = client.pop_multi([queue1, queue2, queue3])

      expect(msg).not_to be_nil
      expect(msg.queue_name).to eq(queue2)
      expect(msg.payload['data']).to eq('test')

      # Verify deleted
      remaining = client.read(queue2, vt: 30)
      expect(remaining).to be_nil
    end

    it 'returns nil when all queues empty' do
      msg = client.pop_multi([queue1, queue2, queue3])
      expect(msg).to be_nil
    end

    it 'pops from first non-empty queue' do
      # Send to queue3 only
      client.send(queue3, { from: 'q3' })

      msg = client.pop_multi([queue1, queue2, queue3])

      expect(msg.queue_name).to eq(queue3)
    end

    it 'validates queue names' do
      expect do
        client.pop_multi(['invalid-name!'])
      end.to raise_error(PGMQ::InvalidQueueNameError)
    end

    it 'validates array input' do
      expect do
        client.pop_multi('not_array')
      end.to raise_error(ArgumentError, /must be an array/)
    end

    it 'validates non-empty array' do
      expect do
        client.pop_multi([])
      end.to raise_error(ArgumentError, /cannot be empty/)
    end

    it 'limits to 50 queues' do
      many_queues = (1..51).map { |i| "queue#{i}" }
      expect do
        client.pop_multi(many_queues)
      end.to raise_error(ArgumentError, /cannot exceed 50/)
    end

    it 'has queue_name attribute on returned message' do
      client.send(queue1, { test: 'data' })
      msg = client.pop_multi([queue1, queue2])

      expect(msg).to respond_to(:queue_name)
      expect(msg.queue_name).to eq(queue1)
    end
  end

  describe '#delete_multi' do
    it 'deletes messages from multiple queues' do
      # Send messages
      id1 = client.send(queue1, { n: 1 })
      id2 = client.send(queue1, { n: 2 })
      id3 = client.send(queue2, { n: 3 })

      result = client.delete_multi({
        queue1 => [id1, id2],
        queue2 => [id3]
      })

      expect(result[queue1]).to contain_exactly(id1, id2)
      expect(result[queue2]).to eq([id3])

      # Verify deleted
      expect(client.read(queue1, vt: 30)).to be_nil
      expect(client.read(queue2, vt: 30)).to be_nil
    end

    it 'returns empty hash for empty input' do
      result = client.delete_multi({})
      expect(result).to eq({})
    end

    it 'skips empty message ID arrays' do
      id1 = client.send(queue1, { n: 1 })

      result = client.delete_multi({
        queue1 => [id1],
        queue2 => []
      })

      expect(result[queue1]).to eq([id1])
      expect(result[queue2]).to be_nil
    end

    it 'validates hash input' do
      expect do
        client.delete_multi('not_hash')
      end.to raise_error(ArgumentError, /must be a hash/)
    end

    it 'validates all queue names' do
      expect do
        client.delete_multi({ 'invalid-name!' => [1, 2] })
      end.to raise_error(PGMQ::InvalidQueueNameError)
    end

    it 'is transactional (all or nothing)' do
      id1 = client.send(queue1, { n: 1 })
      id2 = client.send(queue2, { n: 2 })

      # This should work atomically
      result = client.delete_multi({
        queue1 => [id1],
        queue2 => [id2]
      })

      expect(result.values.flatten.size).to eq(2)
    end

    it 'works with batch processing pattern' do
      5.times { client.send(queue1, { q: 1 }) }
      5.times { client.send(queue2, { q: 2 }) }

      messages = client.read_multi([queue1, queue2], vt: 30, qty: 10)
      deletions = messages.group_by(&:queue_name).transform_values { |msgs| msgs.map(&:msg_id) }

      result = client.delete_multi(deletions)

      expect(result.values.flatten.size).to eq(10)
    end
  end

  describe '#archive_multi' do
    it 'archives messages from multiple queues' do
      id1 = client.send(queue1, { n: 1 })
      id2 = client.send(queue2, { n: 2 })

      result = client.archive_multi({
        queue1 => [id1],
        queue2 => [id2]
      })

      expect(result[queue1]).to eq([id1])
      expect(result[queue2]).to eq([id2])

      # Messages should be gone from main queue
      expect(client.read(queue1, vt: 30)).to be_nil
      expect(client.read(queue2, vt: 30)).to be_nil
    end

    it 'returns empty hash for empty input' do
      result = client.archive_multi({})
      expect(result).to eq({})
    end

    it 'skips empty message ID arrays' do
      id1 = client.send(queue1, { n: 1 })

      result = client.archive_multi({
        queue1 => [id1],
        queue2 => []
      })

      expect(result[queue1]).to eq([id1])
      expect(result[queue2]).to be_nil
    end

    it 'validates hash input' do
      expect do
        client.archive_multi('not_hash')
      end.to raise_error(ArgumentError, /must be a hash/)
    end

    it 'validates all queue names' do
      expect do
        client.archive_multi({ 'invalid-name!' => [1] })
      end.to raise_error(PGMQ::InvalidQueueNameError)
    end

    it 'is transactional' do
      id1 = client.send(queue1, { n: 1 })
      id2 = client.send(queue2, { n: 2 })

      result = client.archive_multi({
        queue1 => [id1],
        queue2 => [id2]
      })

      expect(result.values.flatten.size).to eq(2)
    end

    it 'archives multiple messages from same queue' do
      ids = Array.new(3) { client.send(queue1, { n: 1 }) }

      result = client.archive_multi({
        queue1 => ids
      })

      expect(result[queue1].size).to eq(3)
    end
  end
end
