# frozen_string_literal: true

RSpec.describe PGMQ::Metrics do
  subject(:metrics) { described_class.new(row) }

  let(:row) do
    {
      'queue_name' => 'orders',
      'queue_length' => '42',
      'newest_msg_age_sec' => '5',
      'oldest_msg_age_sec' => '120',
      'total_messages' => '1000',
      'scrape_time' => '2025-01-15 10:00:00 UTC'
    }
  end

  describe '#initialize' do
    it 'returns queue name as string' do
      expect(metrics.queue_name).to eq('orders')
    end

    it 'returns queue length as string' do
      expect(metrics.queue_length).to eq('42')
    end

    it 'returns newest message age as string' do
      expect(metrics.newest_msg_age_sec).to eq('5')
    end

    it 'returns oldest message age as string' do
      expect(metrics.oldest_msg_age_sec).to eq('120')
    end

    it 'returns total messages as string' do
      expect(metrics.total_messages).to eq('1000')
    end

    it 'returns scrape time as string' do
      expect(metrics.scrape_time).to eq('2025-01-15 10:00:00 UTC')
    end
  end

  describe '#to_h' do
    it 'returns hash representation' do
      hash = metrics.to_h
      expect(hash[:queue_name]).to eq('orders')
      expect(hash[:queue_length]).to eq('42')
      expect(hash[:total_messages]).to eq('1000')
    end
  end

  describe '#inspect' do
    it 'returns string representation' do
      expect(metrics.inspect).to include('PGMQ::Metrics')
      # Data.define uses quotes for string values
      expect(metrics.inspect).to include('queue_name="orders"')
      expect(metrics.inspect).to include('queue_length="42"')
    end
  end

  context 'with nil age values' do
    let(:row_with_nils) do
      row.merge('newest_msg_age_sec' => nil, 'oldest_msg_age_sec' => nil)
    end

    it 'handles nil ages gracefully' do
      m = described_class.new(row_with_nils)
      expect(m.newest_msg_age_sec).to be_nil
      expect(m.oldest_msg_age_sec).to be_nil
    end
  end
end
