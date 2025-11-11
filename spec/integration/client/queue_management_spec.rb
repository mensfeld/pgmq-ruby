# frozen_string_literal: true

RSpec.describe PGMQ::Client::QueueManagement, :integration do
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
      msg_id = client.send(queue_name, to_json_msg({ test: 'unlogged' }))
      msg = client.read(queue_name, vt: 30)
      expect(msg.msg_id).to eq(msg_id)
      expect(JSON.parse(msg.message)['test']).to eq('unlogged')
    end

    it 'raises error for invalid queue name' do
      expect { client.create_unlogged('123invalid') }.to raise_error(PGMQ::Errors::InvalidQueueNameError)
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
end
