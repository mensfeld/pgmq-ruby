# frozen_string_literal: true

RSpec.describe PGMQ::QueueMetadata do
  subject(:metadata) { described_class.new(row) }

  let(:row) do
    {
      'queue_name' => 'my_queue',
      'created_at' => '2025-01-15 09:00:00 UTC',
      'is_partitioned' => 't',
      'is_unlogged' => 'f'
    }
  end

  describe '#initialize' do
    it 'returns queue name as string' do
      expect(metadata.queue_name).to eq('my_queue')
    end

    it 'returns created_at as string timestamp' do
      expect(metadata.created_at).to eq('2025-01-15 09:00:00 UTC')
    end

    it 'returns is_partitioned as PostgreSQL boolean string' do
      expect(metadata.is_partitioned).to eq('t')
    end

    it 'returns is_unlogged as PostgreSQL boolean string' do
      expect(metadata.is_unlogged).to eq('f')
    end
  end

  describe '#partitioned?' do
    it 'returns partitioned status' do
      expect(metadata.partitioned?).to eq('t')
    end
  end

  describe '#unlogged?' do
    it 'returns unlogged status' do
      expect(metadata.unlogged?).to eq('f')
    end
  end

  describe '#to_h' do
    it 'returns hash representation' do
      hash = metadata.to_h
      expect(hash[:queue_name]).to eq('my_queue')
      expect(hash[:is_partitioned]).to eq('t')
      expect(hash[:is_unlogged]).to eq('f')
    end
  end

  describe '#inspect' do
    it 'returns string representation' do
      expect(metadata.inspect).to include('PGMQ::QueueMetadata')
      # Data.define uses quotes for string values
      expect(metadata.inspect).to include('queue_name="my_queue"')
      expect(metadata.inspect).to include('is_partitioned="t"')
    end
  end

  context 'with different PostgreSQL boolean representations' do
    it 'handles PostgreSQL boolean "f"' do
      row_with_f = row.merge('is_partitioned' => 'f', 'is_unlogged' => 'f')
      m = described_class.new(row_with_f)
      expect(m.is_partitioned).to eq('f')
      expect(m.is_unlogged).to eq('f')
    end

    it 'handles PostgreSQL boolean "t"' do
      row_with_t = row.merge('is_partitioned' => 't', 'is_unlogged' => 't')
      m = described_class.new(row_with_t)
      expect(m.is_partitioned).to eq('t')
      expect(m.is_unlogged).to eq('t')
    end
  end
end
