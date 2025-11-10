# frozen_string_literal: true

require 'json'

module PGMQ
  module Serializers
    # JSON serializer for PGMQ messages
    #
    # This is the default serializer and handles conversion between
    # Ruby objects and JSON strings for storage in PostgreSQL JSONB columns.
    #
    # @example Using the JSON serializer
    #   serializer = PGMQ::Serializers::JSON.new
    #   json_str = serializer.serialize({ foo: 'bar' })
    #   obj = serializer.deserialize(json_str)
    class JSON < Base
      # Serializes a Ruby object to JSON string
      #
      # @param obj [Object] the object to serialize (typically a Hash)
      # @return [String] JSON representation
      # @raise [PGMQ::Errors::SerializationError] if serialization fails
      def serialize(obj)
        # If already a string, assume it's JSON and return as-is
        return obj if obj.is_a?(String)

        ::JSON.generate(obj)
      rescue ::JSON::GeneratorError, StandardError => e
        raise PGMQ::Errors::SerializationError, "Failed to serialize object to JSON: #{e.message}"
      end

      # Deserializes a JSON string to a Ruby object
      #
      # @param str [String] JSON string to deserialize
      # @return [Object] deserialized object (typically a Hash)
      # @raise [PGMQ::Errors::SerializationError] if deserialization fails
      def deserialize(str)
        # If already a Hash, return as-is
        return str if str.is_a?(Hash)

        ::JSON.parse(str.to_s)
      rescue ::JSON::ParserError, StandardError => e
        raise PGMQ::Errors::SerializationError, "Failed to deserialize JSON: #{e.message}"
      end
    end
  end
end
