# frozen_string_literal: true

begin
  require 'msgpack'
rescue LoadError
  # msgpack is optional
end

module PGMQ
  module Serializers
    # MessagePack serializer for PGMQ messages
    #
    # Provides better performance and smaller message sizes compared to JSON.
    # Requires the 'msgpack' gem to be installed.
    #
    # @example
    #   PGMQ.configure do |config|
    #     config.serializer = PGMQ::Serializers::MessagePack.new
    #   end
    class MessagePack < Base
      # Serializes data to MessagePack format, then encodes to JSON
      # (PGMQ stores messages as JSONB, so we base64-encode the MessagePack data)
      #
      # @param data [Object] data to serialize
      # @return [String] JSON string containing base64-encoded MessagePack data
      # @raise [PGMQ::SerializationError] if msgpack gem is not available
      def serialize(data)
        ensure_msgpack_available!

        packed = ::MessagePack.pack(data)
        encoded = [packed].pack('m0') # Base64 encode
        ::JSON.generate({ '_msgpack' => encoded })
      rescue StandardError => e
        raise PGMQ::SerializationError, "Failed to serialize with MessagePack: #{e.message}"
      end

      # Deserializes MessagePack data from JSON
      #
      # @param json_string [String] JSON string containing base64-encoded MessagePack data
      # @return [Object] deserialized data
      # @raise [PGMQ::DeserializationError] if deserialization fails
      def deserialize(json_string)
        ensure_msgpack_available!

        parsed = ::JSON.parse(json_string)
        return parsed unless parsed.is_a?(Hash) && parsed.key?('_msgpack')

        encoded = parsed['_msgpack']
        packed = encoded.unpack1('m0') # Base64 decode
        ::MessagePack.unpack(packed)
      rescue StandardError => e
        raise PGMQ::DeserializationError, "Failed to deserialize with MessagePack: #{e.message}"
      end

      private

      # Checks if msgpack gem is available
      #
      # @raise [PGMQ::SerializationError] if msgpack is not available
      def ensure_msgpack_available!
        return if defined?(::MessagePack)

        raise PGMQ::SerializationError,
              'msgpack gem is required for MessagePack serializer. ' \
              'Add "gem \'msgpack\'" to your Gemfile.'
      end
    end
  end
end
