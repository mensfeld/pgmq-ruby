# frozen_string_literal: true

RSpec.describe PGMQ::Message do
  subject(:message) { described_class.new(row) }

  let(:row) do
    {
      'msg_id' => '123',
      'read_ct' => '2',
      'enqueued_at' => '2025-01-15 10:00:00 UTC',
      'vt' => '2025-01-15 10:00:30 UTC',
      'message' => '{"order_id":456,"status":"pending"}',
      'headers' => '{"trace_id":"abc123"}',
      'queue_name' => nil
    }
  end

  describe '#initialize' do
    it 'returns raw message ID as string' do
      expect(message.msg_id).to eq('123')
    end

    it 'returns raw read count as string' do
      expect(message.read_ct).to eq('2')
    end

    it 'returns raw enqueued_at as string' do
      expect(message.enqueued_at).to eq('2025-01-15 10:00:00 UTC')
    end

    it 'returns raw vt as string' do
      expect(message.vt).to eq('2025-01-15 10:00:30 UTC')
    end

    it 'returns raw JSON message string' do
      expect(message.message).to eq('{"order_id":456,"status":"pending"}')
    end

    it 'returns raw headers as JSON string' do
      expect(message.headers).to eq('{"trace_id":"abc123"}')
    end

    it 'returns queue_name when present' do
      row_with_queue = row.merge('queue_name' => 'test_queue')
      msg = described_class.new(row_with_queue)
      expect(msg.queue_name).to eq('test_queue')
    end
  end

  describe '#id' do
    it 'returns message ID (alias for msg_id)' do
      expect(message.id).to eq('123')
    end
  end

  describe 'Data immutability' do
    it 'is immutable (Data object)' do
      expect { message.msg_id = '456' }.to raise_error(NoMethodError)
    end

    it 'provides equality based on values' do
      msg1 = described_class.new(row)
      msg2 = described_class.new(row)
      expect(msg1).to eq(msg2)
    end
  end

  describe '#to_h' do
    it 'returns hash representation from Data' do
      hash = message.to_h
      expect(hash[:msg_id]).to eq('123')
      expect(hash[:read_ct]).to eq('2')
      expect(hash[:message]).to eq('{"order_id":456,"status":"pending"}')
    end
  end

  describe '#inspect' do
    it 'returns Data string representation' do
      expect(message.inspect).to include('PGMQ::Message')
      expect(message.inspect).to include('msg_id="123"')
    end
  end

  context 'with Hash message (from PostgreSQL JSONB)' do
    let(:row_with_hash) do
      row.merge('message' => { 'already' => 'parsed' })
    end

    it 'returns the hash as-is (pg gem can return JSONB as Hash)' do
      msg = described_class.new(row_with_hash)
      expect(msg.message).to eq({ 'already' => 'parsed' })
    end
  end
end
