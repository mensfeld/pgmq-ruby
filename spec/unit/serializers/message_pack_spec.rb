# frozen_string_literal: true

require 'spec_helper'

RSpec.describe PGMQ::Serializers::MessagePack do
  let(:serializer) { described_class.new }

  describe '#serialize' do
    context 'when msgpack gem is not available' do
      before do
        hide_const('MessagePack')
      end

      it 'raises SerializationError' do
        expect { serializer.serialize({ foo: 'bar' }) }.to raise_error(
          PGMQ::SerializationError,
          /msgpack gem is required/
        )
      end
    end

    context 'when msgpack gem is available', if: defined?(MessagePack) do
      it 'serializes simple hash' do
        data = { foo: 'bar', baz: 123 }
        result = serializer.serialize(data)

        expect(result).to be_a(String)
        parsed = JSON.parse(result)
        expect(parsed).to have_key('_msgpack')
      end

      it 'serializes array' do
        data = [1, 2, 3, 'foo', 'bar']
        result = serializer.serialize(data)

        parsed = JSON.parse(result)
        expect(parsed).to have_key('_msgpack')
      end

      it 'serializes nested structures' do
        data = {
          user: { name: 'John', age: 30 },
          tags: %w[ruby rails],
          metadata: { created_at: '2024-01-01' }
        }
        result = serializer.serialize(data)

        parsed = JSON.parse(result)
        expect(parsed).to have_key('_msgpack')
      end

      it 'returns valid JSON' do
        data = { test: 'data' }
        result = serializer.serialize(data)

        expect { JSON.parse(result) }.not_to raise_error
      end

      it 'raises SerializationError on failure' do
        # Create an object that MessagePack can't serialize
        unpacker = Object.new

        expect { serializer.serialize(unpacker) }.to raise_error(
          PGMQ::SerializationError,
          /Failed to serialize with MessagePack/
        )
      end
    end
  end

  describe '#deserialize' do
    context 'when msgpack gem is not available' do
      before do
        hide_const('MessagePack')
      end

      it 'raises DeserializationError' do
        json_string = '{"_msgpack":"data"}'

        expect { serializer.deserialize(json_string) }.to raise_error(
          PGMQ::DeserializationError,
          /msgpack gem is required/
        )
      end
    end

    context 'when msgpack gem is available', if: defined?(MessagePack) do
      it 'deserializes to original data' do
        original_data = { foo: 'bar', baz: 123 }
        serialized = serializer.serialize(original_data)
        deserialized = serializer.deserialize(serialized)

        expect(deserialized).to eq('foo' => 'bar', 'baz' => 123)
      end

      it 'deserializes arrays' do
        original_data = [1, 2, 3, 'foo', 'bar']
        serialized = serializer.serialize(original_data)
        deserialized = serializer.deserialize(serialized)

        expect(deserialized).to eq(original_data)
      end

      it 'deserializes nested structures' do
        original_data = {
          user: { name: 'John', age: 30 },
          tags: %w[ruby rails]
        }
        serialized = serializer.serialize(original_data)
        deserialized = serializer.deserialize(serialized)

        expect(deserialized['user']['name']).to eq('John')
        expect(deserialized['user']['age']).to eq(30)
        expect(deserialized['tags']).to eq(%w[ruby rails])
      end

      it 'handles non-msgpack JSON by returning parsed JSON' do
        regular_json = '{"foo":"bar","baz":123}'
        result = serializer.deserialize(regular_json)

        expect(result).to eq('foo' => 'bar', 'baz' => 123)
      end

      it 'raises DeserializationError on invalid data' do
        invalid_json = '{"_msgpack":"not-valid-base64!!!"}'

        expect { serializer.deserialize(invalid_json) }.to raise_error(
          PGMQ::DeserializationError,
          /Failed to deserialize with MessagePack/
        )
      end
    end
  end

  describe 'round-trip serialization', if: defined?(MessagePack) do
    it 'preserves data through serialize/deserialize cycle' do
      test_cases = [
        { simple: 'hash' },
        [1, 2, 3],
        { nested: { data: { structure: 'yes' } } },
        { mixed: [1, 'two', { three: 3 }] },
        { unicode: '你好世界', emoji: '🚀' }
      ]

      test_cases.each do |original|
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)

        # Convert symbols to strings for comparison (MessagePack doesn't preserve symbols)
        normalized_original = JSON.parse(JSON.generate(original))
        expect(deserialized).to eq(normalized_original)
      end
    end
  end
end
