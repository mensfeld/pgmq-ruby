# frozen_string_literal: true

RSpec.describe PGMQ do
  describe '.new' do
    it 'creates a client via the convenience method with hash params' do
      client = described_class.new(TEST_DB_PARAMS)
      expect(client).to be_a(PGMQ::Client)
      expect(client.connection).to be_a(PGMQ::Connection)
    end

    it 'supports connection string parameter' do
      conn_string = 'postgres://postgres:postgres@localhost:5433/pgmq_test'
      client = described_class.new(conn_string)
      expect(client).to be_a(PGMQ::Client)
    end

    it 'supports custom serializer' do
      client = described_class.new(TEST_DB_PARAMS, serializer: PGMQ::Serializers::JSON.new)
      expect(client).to be_a(PGMQ::Client)
      expect(client.serializer).to be_a(PGMQ::Serializers::JSON)
    end
  end
end
