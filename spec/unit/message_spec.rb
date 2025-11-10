# frozen_string_literal: true

RSpec.describe PGMQ::Message do
  subject(:message) { described_class.new(row) }

  let(:row) do
    {
      'msg_id' => '123',
      'read_ct' => '2',
      'enqueued_at' => '2025-01-15 10:00:00 UTC',
      'vt' => '2025-01-15 10:00:30 UTC',
      'message' => '{"order_id":456,"status":"pending"}'
    }
  end

  describe '#initialize' do
    it 'parses message ID' do
      expect(message.msg_id).to eq(123)
    end

    it 'parses read count' do
      expect(message.read_ct).to eq(2)
    end

    it 'parses enqueued_at timestamp' do
      expect(message.enqueued_at).to be_a(Time)
      expect(message.enqueued_at.year).to eq(2025)
    end

    it 'parses vt timestamp' do
      expect(message.vt).to be_a(Time)
    end

    it 'deserializes JSON message' do
      expect(message.message).to eq({ 'order_id' => 456, 'status' => 'pending' })
    end
  end

  describe '#id' do
    it 'returns message ID' do
      expect(message.id).to eq(123)
    end
  end

  describe '#payload' do
    it 'returns message payload' do
      expect(message.payload).to eq({ 'order_id' => 456, 'status' => 'pending' })
    end
  end

  describe '#[]' do
    it 'provides hash-like access with string keys' do
      expect(message['order_id']).to eq(456)
    end

    it 'provides hash-like access with symbol keys' do
      expect(message[:order_id]).to eq(456)
    end

    it 'returns nil for non-existent keys' do
      expect(message[:nonexistent]).to be_nil
    end
  end

  describe '#to_h' do
    it 'returns hash representation' do
      hash = message.to_h
      expect(hash[:msg_id]).to eq(123)
      expect(hash[:read_ct]).to eq(2)
      expect(hash[:message]).to be_a(Hash)
    end
  end

  describe '#inspect' do
    it 'returns string representation' do
      expect(message.inspect).to include('PGMQ::Message')
      expect(message.inspect).to include('msg_id=123')
      expect(message.inspect).to include('read_ct=2')
    end
  end

  context 'with custom serializer' do
    let(:serializer) do
      double('Serializer', deserialize: { 'custom' => 'data' })
    end

    it 'uses custom serializer' do
      msg = described_class.new(row, serializer: serializer)
      expect(msg.payload).to eq({ 'custom' => 'data' })
    end
  end

  context 'with already parsed Hash message' do
    let(:row_with_hash) do
      row.merge('message' => { 'already' => 'parsed' })
    end

    it 'uses the hash directly' do
      msg = described_class.new(row_with_hash)
      expect(msg.payload).to eq({ 'already' => 'parsed' })
    end
  end

  context 'with invalid JSON' do
    let(:row_with_invalid_json) do
      row.merge('message' => 'not valid json{')
    end

    it 'raises SerializationError' do
      expect { described_class.new(row_with_invalid_json) }.to raise_error(PGMQ::Errors::SerializationError)
    end
  end
end
