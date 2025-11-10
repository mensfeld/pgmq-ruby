# frozen_string_literal: true

RSpec.describe PGMQ::Client, :integration do
  let(:client) { create_test_client }
  let(:queue_name) { test_queue_name }

  after do
    begin
      client.drop_queue(queue_name)
    rescue StandardError
      nil
    end
    client.close
  end

  describe '#create' do
    it 'creates a new queue' do
      client.create(queue_name)
      queues = client.list_queues
      expect(queues.map(&:queue_name)).to include(queue_name)
    end

    it 'raises error for invalid queue name' do
      expect { client.create('123invalid') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end

    it 'raises error for empty queue name' do
      expect { client.create('') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end

  describe '#send and #read' do
    before { ensure_test_queue(client, queue_name) }

    it 'sends and reads a message' do
      message_data = { order_id: 123, status: 'pending' }
      msg_id = client.send(queue_name, message_data)

      expect(msg_id).to be_a(Integer)
      expect(msg_id).to be > 0

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_a(PGMQ::Message)
      expect(msg.msg_id).to eq(msg_id)
      expect(msg.payload).to eq({ 'order_id' => 123, 'status' => 'pending' })
    end

    it 'returns nil when queue is empty' do
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end

    it 'sends message with delay' do
      msg_id = client.send(queue_name, { test: 'data' }, delay: 2)
      expect(msg_id).to be_a(Integer)

      # Message should not be visible immediately
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil

      # Wait for delay
      sleep 2.5

      # Now message should be visible
      msg = client.read(queue_name, vt: 30)
      expect(msg).not_to be_nil
      expect(msg.payload).to eq({ 'test' => 'data' })
    end
  end

  describe '#send_batch and #read_batch' do
    before { ensure_test_queue(client, queue_name) }

    it 'sends and reads multiple messages' do
      messages = [
        { id: 1, data: 'first' },
        { id: 2, data: 'second' },
        { id: 3, data: 'third' }
      ]

      msg_ids = client.send_batch(queue_name, messages)
      expect(msg_ids).to be_an(Array)
      expect(msg_ids.size).to eq(3)

      read_messages = client.read_batch(queue_name, vt: 30, qty: 3)
      expect(read_messages.size).to eq(3)
      expect(read_messages.map { |m| m[:data] }).to contain_exactly('first', 'second', 'third')
    end

    it 'handles empty batch' do
      msg_ids = client.send_batch(queue_name, [])
      expect(msg_ids).to eq([])
    end
  end

  describe '#delete' do
    before { ensure_test_queue(client, queue_name) }

    it 'deletes a message' do
      client.send(queue_name, { test: 'data' })
      msg = client.read(queue_name, vt: 30)

      result = client.delete(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be readable again
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it 'returns false for non-existent message' do
      result = client.delete(queue_name, 99_999)
      expect(result).to be false
    end
  end

  describe '#delete_batch' do
    before { ensure_test_queue(client, queue_name) }

    it 'deletes multiple messages' do
      client.send_batch(queue_name, [{ a: 1 }, { b: 2 }, { c: 3 }])
      messages = client.read_batch(queue_name, vt: 30, qty: 3)

      deleted_ids = client.delete_batch(queue_name, messages.map(&:msg_id))
      expect(deleted_ids.size).to eq(3)

      # No messages should remain
      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end
  end

  describe '#archive' do
    before { ensure_test_queue(client, queue_name) }

    it 'archives a message' do
      client.send(queue_name, { test: 'archive' })
      msg = client.read(queue_name, vt: 30)

      result = client.archive(queue_name, msg.msg_id)
      expect(result).to be true

      # Message should not be in main queue
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end
  end

  describe '#archive_batch' do
    before { ensure_test_queue(client, queue_name) }

    it 'archives multiple messages' do
      client.send_batch(queue_name, [{ a: 1 }, { b: 2 }])
      messages = client.read_batch(queue_name, vt: 30, qty: 2)

      archived_ids = client.archive_batch(queue_name, messages.map(&:msg_id))
      expect(archived_ids.size).to eq(2)
    end
  end

  describe '#pop' do
    before { ensure_test_queue(client, queue_name) }

    it 'pops a message (atomic read+delete)' do
      client.send(queue_name, { test: 'pop' })

      msg = client.pop(queue_name)
      expect(msg).to be_a(PGMQ::Message)
      expect(msg.payload).to eq({ 'test' => 'pop' })

      # Message should be deleted
      msg2 = client.read(queue_name, vt: 30)
      expect(msg2).to be_nil
    end

    it 'returns nil for empty queue' do
      msg = client.pop(queue_name)
      expect(msg).to be_nil
    end
  end

  describe '#set_vt' do
    before { ensure_test_queue(client, queue_name) }

    it 'updates visibility timeout' do
      client.send(queue_name, { test: 'vt' })
      msg = client.read(queue_name, vt: 5)

      updated_msg = client.set_vt(queue_name, msg.msg_id, vt_offset: 60)
      expect(updated_msg).to be_a(PGMQ::Message)
      expect(updated_msg.vt).to be > msg.vt
    end
  end

  describe '#purge_queue' do
    before { ensure_test_queue(client, queue_name) }

    it 'purges all messages from queue' do
      client.send_batch(queue_name, [{ a: 1 }, { b: 2 }, { c: 3 }])

      count = client.purge_queue(queue_name)
      expect(count).to eq(3)

      msg = client.read(queue_name, vt: 30)
      expect(msg).to be_nil
    end
  end

  describe '#metrics' do
    before { ensure_test_queue(client, queue_name) }

    it 'returns queue metrics' do
      # Send some messages
      client.send_batch(queue_name, [{ a: 1 }, { b: 2 }, { c: 3 }])

      metrics = client.metrics(queue_name)
      expect(metrics).to be_a(PGMQ::Metrics)
      expect(metrics.queue_name).to eq(queue_name)
      expect(metrics.queue_length).to eq(3)
      expect(metrics.total_messages).to eq(3)
    end
  end

  describe '#metrics_all' do
    it 'returns metrics for all queues' do
      queue1 = test_queue_name('one')
      queue2 = test_queue_name('two')

      client.create(queue1)
      client.create(queue2)
      client.send(queue1, { test: 1 })
      client.send(queue2, { test: 2 })

      all_metrics = client.metrics_all
      expect(all_metrics).to be_an(Array)

      our_queues = all_metrics.select { |m| m.queue_name.start_with?('test_') }
      expect(our_queues.size).to be >= 2

      client.drop_queue(queue1)
      client.drop_queue(queue2)
    end
  end

  describe '#list_queues' do
    it 'lists all queues' do
      queue1 = test_queue_name('list1')
      queue2 = test_queue_name('list2')

      client.create(queue1)
      client.create(queue2)

      queues = client.list_queues
      queue_names = queues.map(&:queue_name)

      expect(queue_names).to include(queue1, queue2)
      expect(queues.first).to be_a(PGMQ::QueueMetadata)

      client.drop_queue(queue1)
      client.drop_queue(queue2)
    end
  end

  describe '#drop_queue' do
    it 'drops a queue' do
      client.create(queue_name)
      result = client.drop_queue(queue_name)

      expect(result).to be true

      queues = client.list_queues
      expect(queues.map(&:queue_name)).not_to include(queue_name)
    end

    it 'returns false for non-existent queue' do
      result = client.drop_queue('nonexistent_queue_xyz')
      expect(result).to be false
    end
  end

  describe 'visibility timeout behavior' do
    before { ensure_test_queue(client, queue_name) }

    it 'makes message invisible during visibility timeout' do
      client.send(queue_name, { test: 'vt' })

      # Read with 3 second VT
      msg1 = client.read(queue_name, vt: 3)
      expect(msg1).not_to be_nil

      # Should not be visible immediately
      msg2 = client.read(queue_name, vt: 3)
      expect(msg2).to be_nil

      # Wait for VT to expire
      sleep 3.5

      # Should be visible again
      msg3 = client.read(queue_name, vt: 3)
      expect(msg3).not_to be_nil
      expect(msg3.msg_id).to eq(msg1.msg_id)
      expect(msg3.read_ct).to eq(2) # Read twice now
    end
  end

  describe 'read_with_poll' do
    before { ensure_test_queue(client, queue_name) }

    it 'waits for messages with long-polling' do
      # Start with empty queue
      Thread.new do
        sleep 1
        client.send(queue_name, { delayed: 'message' })
      end

      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 3,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).not_to be_empty
      expect(elapsed).to be >= 1 # Waited for message
      expect(messages.first.payload).to eq({ 'delayed' => 'message' })
    end

    it 'returns empty array if no messages within timeout' do
      start_time = Time.now
      messages = client.read_with_poll(
        queue_name,
        vt: 30,
        qty: 1,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )
      elapsed = Time.now - start_time

      expect(messages).to be_empty
      expect(elapsed).to be >= 1 # Waited full timeout
    end
  end

  describe 'conditional JSONB filtering (v0.3.0)' do
    before { ensure_test_queue(client, queue_name) }

    describe '#read with conditional' do
      it 'filters by single key-value pair' do
        client.send(queue_name, { status: 'pending', priority: 'high' })
        client.send(queue_name, { status: 'completed', priority: 'low' })

        msg = client.read(queue_name, vt: 30, conditional: { status: 'pending' })

        expect(msg).not_to be_nil
        expect(msg.payload['status']).to eq('pending')
      end

      it 'filters by multiple conditions (AND logic)' do
        client.send(queue_name, { status: 'pending', priority: 'high' })
        client.send(queue_name, { status: 'pending', priority: 'low' })
        client.send(queue_name, { status: 'completed', priority: 'high' })

        msg = client.read(queue_name, vt: 30, conditional: { status: 'pending', priority: 'high' })

        expect(msg).not_to be_nil
        expect(msg.payload['status']).to eq('pending')
        expect(msg.payload['priority']).to eq('high')
      end

      it 'filters by nested object properties' do
        client.send(queue_name, { user: { role: 'admin', active: true } })
        client.send(queue_name, { user: { role: 'user', active: true } })

        msg = client.read(queue_name, vt: 30, conditional: { user: { role: 'admin' } })

        expect(msg).not_to be_nil
        expect(msg.payload['user']['role']).to eq('admin')
      end

      it 'returns nil when no messages match condition' do
        client.send(queue_name, { status: 'pending' })
        client.send(queue_name, { status: 'processing' })

        msg = client.read(queue_name, vt: 30, conditional: { status: 'completed' })

        expect(msg).to be_nil
      end

      it 'preserves visibility timeout with filtering' do
        client.send(queue_name, { status: 'pending' })

        # Read with conditional
        msg1 = client.read(queue_name, vt: 2, conditional: { status: 'pending' })
        expect(msg1).not_to be_nil

        # Should not be visible immediately
        msg2 = client.read(queue_name, vt: 2, conditional: { status: 'pending' })
        expect(msg2).to be_nil

        # Should be visible after VT expires
        sleep 2.5
        msg3 = client.read(queue_name, vt: 30, conditional: { status: 'pending' })
        expect(msg3).not_to be_nil
      end

      it 'handles numeric values correctly' do
        client.send(queue_name, { price: 99, quantity: 10 })
        client.send(queue_name, { price: 50, quantity: 5 })

        msg = client.read(queue_name, vt: 30, conditional: { price: 99 })

        expect(msg).not_to be_nil
        expect(msg.payload['price']).to eq(99)
      end

      it 'handles boolean values correctly' do
        client.send(queue_name, { active: true, verified: false })
        client.send(queue_name, { active: false, verified: true })

        msg = client.read(queue_name, vt: 30, conditional: { active: true })

        expect(msg).not_to be_nil
        expect(msg.payload['active']).to be(true)
      end

      it 'handles empty conditional as no filter' do
        client.send(queue_name, { status: 'pending' })

        msg = client.read(queue_name, vt: 30, conditional: {})

        expect(msg).not_to be_nil
      end
    end

    describe '#read_batch with conditional' do
      it 'returns only matching messages up to qty limit' do
        client.send(queue_name, { priority: 'high', id: 1 })
        client.send(queue_name, { priority: 'low', id: 2 })
        client.send(queue_name, { priority: 'high', id: 3 })
        client.send(queue_name, { priority: 'high', id: 4 })

        messages = client.read_batch(queue_name, vt: 30, qty: 2, conditional: { priority: 'high' })

        expect(messages.length).to eq(2)
        messages.each do |msg|
          expect(msg.payload['priority']).to eq('high')
        end
      end

      it 'returns empty array when no matches' do
        client.send(queue_name, { priority: 'low' })
        client.send(queue_name, { priority: 'medium' })

        messages = client.read_batch(queue_name, vt: 30, qty: 10, conditional: { priority: 'high' })

        expect(messages).to be_empty
      end

      it 'respects qty parameter after filtering' do
        5.times { |i| client.send(queue_name, { type: 'matching', id: i }) }

        messages = client.read_batch(queue_name, vt: 30, qty: 3, conditional: { type: 'matching' })

        expect(messages.length).to eq(3)
      end

      it 'handles mixed matching/non-matching messages' do
        client.send(queue_name, { status: 'pending', id: 1 })
        client.send(queue_name, { status: 'completed', id: 2 })
        client.send(queue_name, { status: 'pending', id: 3 })

        messages = client.read_batch(queue_name, vt: 30, qty: 10, conditional: { status: 'pending' })

        expect(messages.length).to eq(2)
        messages.each do |msg|
          expect(msg.payload['status']).to eq('pending')
        end
      end
    end

    describe '#read_with_poll with conditional' do
      it 'polls until matching message arrives' do
        # Send matching message after delay
        Thread.new do
          sleep 0.5
          client.send(queue_name, { type: 'urgent', data: 'test' })
        end

        start_time = Time.now
        messages = client.read_with_poll(
          queue_name,
          vt: 30,
          qty: 1,
          max_poll_seconds: 2,
          poll_interval_ms: 100,
          conditional: { type: 'urgent' }
        )
        elapsed = Time.now - start_time

        expect(messages).not_to be_empty
        expect(messages.first.payload['type']).to eq('urgent')
        expect(elapsed).to be_between(0.5, 2.0)
      end

      it 'times out if no matching messages' do
        client.send(queue_name, { type: 'normal' })

        start_time = Time.now
        messages = client.read_with_poll(
          queue_name,
          vt: 30,
          qty: 1,
          max_poll_seconds: 1,
          poll_interval_ms: 100,
          conditional: { type: 'urgent' }
        )
        elapsed = Time.now - start_time

        expect(messages).to be_empty
        expect(elapsed).to be >= 1
      end

      it 'returns immediately if matching message exists' do
        client.send(queue_name, { status: 'ready', data: 'test' })

        start_time = Time.now
        messages = client.read_with_poll(
          queue_name,
          vt: 30,
          qty: 1,
          max_poll_seconds: 5,
          poll_interval_ms: 100,
          conditional: { status: 'ready' }
        )
        elapsed = Time.now - start_time

        expect(messages).not_to be_empty
        expect(elapsed).to be < 1 # Should return quickly
      end
    end
  end

  describe 'transaction support (v0.3.0)' do
    let(:queue1) { test_queue_name('txn1') }
    let(:queue2) { test_queue_name('txn2') }

    before do
      ensure_test_queue(client, queue1)
      ensure_test_queue(client, queue2)
    end

    after do
      begin
        client.drop_queue(queue1)
      rescue StandardError
        nil
      end
      begin
        client.drop_queue(queue2)
      rescue StandardError
        nil
      end
    end

    describe 'successful transactions' do
      it 'commits messages to multiple queues atomically' do
        q1 = queue1
        q2 = queue2

        client.transaction do |txn|
          txn.send(q1, { data: 'message1' })
          txn.send(q2, { data: 'message2' })
        end

        # Both messages should be committed
        msg1 = client.read(q1, vt: 30)
        msg2 = client.read(q2, vt: 30)

        expect(msg1).not_to be_nil
        expect(msg1.payload['data']).to eq('message1')
        expect(msg2).not_to be_nil
        expect(msg2.payload['data']).to eq('message2')
      end

      it 'allows read and delete within transaction' do
        q1 = queue1

        client.send(q1, { order_id: 123 })

        client.transaction do |txn|
          msg = txn.read(q1, vt: 30)
          expect(msg).not_to be_nil
          txn.delete(q1, msg.msg_id)
        end

        # Message should be deleted
        msg = client.read(q1, vt: 30)
        expect(msg).to be_nil
      end

      it 'supports archive operations in transaction' do
        q1 = queue1

        msg_id = client.send(q1, { data: 'to_archive' })

        client.transaction do |txn|
          txn.archive(q1, msg_id)
        end

        # Message should be archived (not in main queue)
        msg = client.read(q1, vt: 30)
        expect(msg).to be_nil
      end

      it 'returns transaction block result' do
        q1 = queue1

        result = client.transaction do |txn|
          txn.send(q1, { test: 'data' })
          'transaction_result'
        end

        expect(result).to eq('transaction_result')
      end
    end

    describe 'transaction rollback' do
      it 'rolls back on raised exception' do
        q1 = queue1

        expect do
          client.transaction do |txn|
            txn.send(q1, { data: 'will_rollback' })
            raise 'Test error'
          end
        end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

        # Message should not be persisted
        msg = client.read(q1, vt: 30)
        expect(msg).to be_nil
      end

      it 'rolls back all queue operations on error' do
        q1 = queue1
        q2 = queue2

        expect do
          client.transaction do |txn|
            txn.send(q1, { data: 'message1' })
            txn.send(q2, { data: 'message2' })
            raise StandardError, 'Rollback trigger'
          end
        end.to raise_error(PGMQ::Errors::ConnectionError)

        # Neither message should be persisted
        msg1 = client.read(q1, vt: 30)
        msg2 = client.read(q2, vt: 30)

        expect(msg1).to be_nil
        expect(msg2).to be_nil
      end

      it 'does not persist messages after rollback' do
        q1 = queue1

        # Send message outside transaction first
        client.send(q1, { data: 'before_txn' })

        expect do
          client.transaction do |txn|
            txn.send(q1, { data: 'in_txn' })
            raise 'Rollback'
          end
        end.to raise_error(PGMQ::Errors::ConnectionError)

        # Only the first message should exist
        msg1 = client.read(q1, vt: 30)
        expect(msg1.payload['data']).to eq('before_txn')

        msg2 = client.read(q1, vt: 30)
        expect(msg2).to be_nil
      end

      it 'properly handles PG::Error during transaction' do
        q1 = queue1

        expect do
          client.transaction do |txn|
            txn.send(q1, { data: 'test' })
            # Trigger a PG error by using invalid SQL
            txn.connection.with_connection do |conn|
              conn.exec('INVALID SQL STATEMENT')
            end
          end
        end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)

        # Message should not be persisted
        msg = client.read(q1, vt: 30)
        expect(msg).to be_nil
      end
    end

    describe 'connection management' do
      it 'does not leak connections from transaction' do
        q1 = queue1

        pool_size_before = client.connection.pool.available

        10.times do
          client.transaction do |txn|
            txn.send(q1, { iteration: 'test' })
          end
        end

        # Clean up messages
        client.purge_queue(q1)

        pool_size_after = client.connection.pool.available
        expect(pool_size_after).to eq(pool_size_before)
      end
    end
  end

  describe 'queue name validation (v0.3.0)' do
    it 'creates queue with maximum length name (47 chars)' do
      long_name = 'a' * 47

      expect { client.create(long_name) }.not_to raise_error

      queues = client.list_queues
      expect(queues.map(&:queue_name)).to include(long_name)

      client.drop_queue(long_name)
    end

    it 'rejects queue name at boundary (48 chars)' do
      too_long_name = 'a' * 48

      expect { client.create(too_long_name) }.to raise_error(
        PGMQ::Errors::InvalidQueueNameError,
        /exceeds maximum length of 48 characters.*current length: 48/
      )
    end

    it 'rejects queue name exceeding limit (60 chars)' do
      way_too_long = 'q' * 60

      expect { client.create(way_too_long) }.to raise_error(
        PGMQ::Errors::InvalidQueueNameError,
        /exceeds maximum length of 48 characters.*current length: 60/
      )
    end

    it 'allows queue operations with max-length names' do
      long_name = "test_#{'x' * 42}" # 47 chars total

      client.create(long_name)

      # Send and read should work
      msg_id = client.send(long_name, { data: 'test' })
      expect(msg_id).to be_a(Integer)

      msg = client.read(long_name, vt: 30)
      expect(msg).not_to be_nil
      expect(msg.payload['data']).to eq('test')

      client.drop_queue(long_name)
    end

    it 'handles queue names with underscores correctly' do
      queue_with_underscores = 'my_test_queue_name'

      client.create(queue_with_underscores)
      expect(client.list_queues.map(&:queue_name)).to include(queue_with_underscores)

      client.drop_queue(queue_with_underscores)
    end

    it 'handles queue names with uppercase letters' do
      mixed_case_queue = 'MyTestQueue'

      client.create(mixed_case_queue)
      expect(client.list_queues.map(&:queue_name)).to include(mixed_case_queue)

      client.drop_queue(mixed_case_queue)
    end

    it 'verifies PostgreSQL table prefixes work with long names' do
      # PGMQ creates tables with prefixes: pgmq.q_<name> and pgmq.a_<name>
      long_name = "prefix_test_#{'y' * 35}" # 47 chars

      client.create(long_name)

      # Verify tables exist by successfully using the queue
      client.send(long_name, { test: 'data' })
      msg = client.read(long_name, vt: 30)
      expect(msg).not_to be_nil

      # Archive should also work (uses a_<name> table)
      client.archive(long_name, msg.msg_id)

      client.drop_queue(long_name)
    end

    it 'provides clear error messages for invalid names' do
      expect { client.create('123invalid') }.to raise_error(
        PGMQ::Errors::InvalidQueueNameError,
        /must start with a letter or underscore/
      )

      expect { client.create('my-queue') }.to raise_error(
        PGMQ::Errors::InvalidQueueNameError,
        /must start with a letter or underscore/
      )

      expect { client.create('') }.to raise_error(
        PGMQ::Errors::InvalidQueueNameError,
        /cannot be empty/
      )
    end
  end

  describe '#create_unlogged' do
    it 'creates an unlogged queue' do
      client.create_unlogged(queue_name)
      queues = client.list_queues
      expect(queues.map(&:queue_name)).to include(queue_name)

      # Verify it works by sending/reading a message
      msg_id = client.send(queue_name, { test: 'unlogged' })
      msg = client.read(queue_name, vt: 30)
      expect(msg.msg_id).to eq(msg_id)
      expect(msg.payload['test']).to eq('unlogged')
    end

    it 'raises error for invalid queue name' do
      expect { client.create_unlogged('123invalid') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end

  describe '#detach_archive' do
    it 'detaches archive table from queue management' do
      client.create(queue_name)

      # Send and archive a message first
      msg_id = client.send(queue_name, { test: 'archive' })
      client.archive(queue_name, msg_id)

      # Detach the archive
      client.detach_archive(queue_name)

      # Queue should still exist and be usable
      new_msg_id = client.send(queue_name, { test: 'after_detach' })
      msg = client.read(queue_name, vt: 30)
      expect(msg.msg_id).to eq(new_msg_id)
    end

    it 'raises error for invalid queue name' do
      expect { client.detach_archive('123invalid') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
    end
  end

  describe 'connection injection' do
    it 'accepts a PGMQ::Connection object' do
      connection = PGMQ::Connection.new(TEST_DB_PARAMS)
      injected_client = PGMQ::Client.new(connection)

      expect(injected_client.connection).to eq(connection)

      # Verify it works
      injected_client.create(queue_name)
      queues = injected_client.list_queues
      expect(queues.map(&:queue_name)).to include(queue_name)

      injected_client.drop_queue(queue_name)
      injected_client.close
    end
  end
end
