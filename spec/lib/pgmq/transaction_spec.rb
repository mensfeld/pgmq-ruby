# frozen_string_literal: true

require 'spec_helper'

RSpec.describe PGMQ::Transaction do
  let(:client) { PGMQ::Client.new(TEST_DB_PARAMS) }
  let(:mock_conn) { instance_double(PG::Connection) }
  let(:mock_pool_conn) { instance_double(PG::Connection) }

  before do
    allow(client.connection).to receive(:with_connection).and_yield(mock_pool_conn)
  end

  describe '#transaction' do
    it 'yields a transactional client' do
      allow(mock_pool_conn).to receive(:transaction).and_yield

      expect { |b| client.transaction(&b) }.to yield_with_args(
        an_instance_of(PGMQ::Transaction::TransactionalClient)
      )
    end

    it 'executes block within a database transaction' do
      expect(mock_pool_conn).to receive(:transaction).and_yield

      result = client.transaction do |_txn_client|
        'transaction_result'
      end

      expect(result).to eq('transaction_result')
    end

    it 'rolls back on error' do
      expect(mock_pool_conn).to receive(:transaction).and_raise(PG::Error.new('test error'))

      expect do
        client.transaction do |txn_client|
          # This should rollback
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /Transaction failed/)
    end
  end

  describe PGMQ::Transaction::TransactionalClient do
    let(:parent_client) { PGMQ::Client.new(TEST_DB_PARAMS) }
    let(:txn_conn) { instance_double(PG::Connection) }
    let(:txn_client) { described_class.new(parent_client, txn_conn) }

    describe '#method_missing' do
      it 'delegates to parent client' do
        allow(parent_client).to receive(:respond_to?).with(:list_queues, true).and_return(true)
        allow(parent_client).to receive(:list_queues).and_return([])

        result = txn_client.list_queues
        expect(result).to eq([])
      end

      it 'passes through method arguments' do
        allow(parent_client).to receive(:respond_to?).with(:create, true).and_return(true)
        allow(parent_client).to receive(:create).with('test_queue')

        txn_client.create('test_queue')

        expect(parent_client).to have_received(:create).with('test_queue')
      end

      it 'raises NoMethodError for undefined methods' do
        expect { txn_client.undefined_method }.to raise_error(NoMethodError)
      end
    end

    describe '#respond_to_missing?' do
      it 'returns true for methods parent responds to' do
        allow(parent_client).to receive(:respond_to?).with(:create, false).and_return(true)

        expect(txn_client.respond_to?(:create)).to be true
      end

      it 'returns false for methods parent does not respond to' do
        allow(parent_client).to receive(:respond_to?).with(:undefined, false).and_return(false)

        expect(txn_client.respond_to?(:undefined)).to be false
      end
    end
  end
end
