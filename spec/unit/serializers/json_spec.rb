# frozen_string_literal: true

RSpec.describe PGMQ::Serializers::JSON do
  subject(:serializer) { described_class.new }

  describe '#serialize' do
    it 'serializes a hash to JSON string' do
      input = { foo: 'bar', count: 42 }
      result = serializer.serialize(input)
      expect(result).to be_a(String)
      expect(JSON.parse(result)).to eq({ 'foo' => 'bar', 'count' => 42 })
    end

    it 'serializes nested structures' do
      input = { user: { name: 'John', tags: %w[admin user] } }
      result = serializer.serialize(input)
      parsed = JSON.parse(result)
      expect(parsed['user']['name']).to eq('John')
      expect(parsed['user']['tags']).to eq(%w[admin user])
    end

    it 'returns string input as-is' do
      input = '{"already":"json"}'
      result = serializer.serialize(input)
      expect(result).to eq(input)
    end

    it 'raises SerializationError on failure' do
      # Create an object that can't be serialized to JSON
      unserializable = Object.new
      def unserializable.to_json(*_args)
        raise JSON::GeneratorError, 'Cannot serialize'
      end

      expect { serializer.serialize(unserializable) }.to raise_error(PGMQ::Errors::SerializationError)
    end
  end

  describe '#deserialize' do
    it 'deserializes JSON string to hash' do
      input = '{"foo":"bar","count":42}'
      result = serializer.deserialize(input)
      expect(result).to eq({ 'foo' => 'bar', 'count' => 42 })
    end

    it 'deserializes nested JSON' do
      input = '{"user":{"name":"John","tags":["admin","user"]}}'
      result = serializer.deserialize(input)
      expect(result['user']['name']).to eq('John')
      expect(result['user']['tags']).to eq(%w[admin user])
    end

    it 'returns hash input as-is' do
      input = { 'already' => 'parsed' }
      result = serializer.deserialize(input)
      expect(result).to eq(input)
    end

    it 'raises SerializationError on invalid JSON' do
      input = 'not valid json{'
      expect { serializer.deserialize(input) }.to raise_error(PGMQ::Errors::SerializationError)
    end
  end

  describe 'round-trip' do
    it 'successfully serializes and deserializes' do
      original = { order_id: 123, items: [{ sku: 'ABC', qty: 2 }] }
      serialized = serializer.serialize(original)
      deserialized = serializer.deserialize(serialized)

      expect(deserialized['order_id']).to eq(123)
      expect(deserialized['items'].first['sku']).to eq('ABC')
    end
  end
end
