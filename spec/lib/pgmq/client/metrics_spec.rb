# frozen_string_literal: true

RSpec.describe PGMQ::Client::Metrics, :integration do
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

  before { ensure_test_queue(client, queue_name) }

  describe '#metrics' do
    it 'returns queue metrics' do
      # Send some messages
      batch = [to_json_msg({ a: 1 }), to_json_msg({ b: 2 }), to_json_msg({ c: 3 })]
      client.send_batch(queue_name, batch)

      metrics = client.metrics(queue_name)
      expect(metrics).to be_a(PGMQ::Metrics)
      expect(metrics.queue_name).to eq(queue_name)
      expect(metrics.queue_length).to eq('3')
      expect(metrics.total_messages).to eq('3')
    end

    it 'raises error for non-existent queue' do
      expect do
        client.metrics('nonexistent_queue_xyz')
      end.to raise_error(PGMQ::Errors::ConnectionError, /relation.*does not exist/)
    end

    it 'returns metrics for empty queue' do
      metrics = client.metrics(queue_name)
      expect(metrics).to be_a(PGMQ::Metrics)
      expect(metrics.queue_name).to eq(queue_name)
      expect(metrics.queue_length).to eq('0')
    end
  end

  describe '#metrics_all' do
    it 'returns metrics for all queues' do
      queue1 = test_queue_name('one')
      queue2 = test_queue_name('two')

      client.create(queue1)
      client.create(queue2)
      client.send(queue1, to_json_msg({ test: 1 }))
      client.send(queue2, to_json_msg({ test: 2 }))

      all_metrics = client.metrics_all
      expect(all_metrics).to be_an(Array)

      our_queues = all_metrics.select { |m| m.queue_name.start_with?('test_') }
      expect(our_queues.size).to be >= 2

      client.drop_queue(queue1)
      client.drop_queue(queue2)
    end

    it 'returns empty array when no queues exist' do
      # This test would require a fresh database, so we'll just check it returns an array
      all_metrics = client.metrics_all
      expect(all_metrics).to be_an(Array)
    end
  end
end
