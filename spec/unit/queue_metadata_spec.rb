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
    it 'parses queue name' do
      expect(metadata.queue_name).to eq('my_queue')
    end

    it 'parses created_at timestamp' do
      expect(metadata.created_at).to be_a(Time)
      expect(metadata.created_at.year).to eq(2025)
    end

    it 'parses is_partitioned boolean' do
      expect(metadata.is_partitioned).to be true
    end

    it 'parses is_unlogged boolean' do
      expect(metadata.is_unlogged).to be false
    end
  end

  describe '#partitioned?' do
    it 'returns partitioned status' do
      expect(metadata.partitioned?).to be true
    end
  end

  describe '#unlogged?' do
    it 'returns unlogged status' do
      expect(metadata.unlogged?).to be false
    end
  end

  describe '#to_h' do
    it 'returns hash representation' do
      hash = metadata.to_h
      expect(hash[:queue_name]).to eq('my_queue')
      expect(hash[:is_partitioned]).to be true
      expect(hash[:is_unlogged]).to be false
    end
  end

  describe '#inspect' do
    it 'returns string representation' do
      expect(metadata.inspect).to include('PGMQ::QueueMetadata')
      # Data.define uses quotes for string values
      expect(metadata.inspect).to include('queue_name="my_queue"')
      expect(metadata.inspect).to include('is_partitioned=true')
    end
  end

  context 'with boolean values as actual booleans' do
    let(:row_with_booleans) do
      row.merge('is_partitioned' => false, 'is_unlogged' => true)
    end

    it 'handles boolean values' do
      m = described_class.new(row_with_booleans)
      expect(m.is_partitioned).to be false
      expect(m.is_unlogged).to be true
    end
  end

  context 'with string boolean values' do
    let(:row_with_strings) do
      row.merge('is_partitioned' => 'true', 'is_unlogged' => 'false')
    end

    it 'parses string booleans' do
      m = described_class.new(row_with_strings)
      expect(m.is_partitioned).to be true
      expect(m.is_unlogged).to be false
    end
  end
end
