# frozen_string_literal: true

module PGMQ
  # Message serializers for encoding/decoding message payloads
  module Serializers
    # Base class for message serializers
    #
    # Custom serializers should inherit from this class and implement
    # the `serialize` and `deserialize` methods.
    #
    # @example Creating a custom serializer
    #   class MySerializer < PGMQ::Serializers::Base
    #     def serialize(obj)
    #       # Convert Ruby object to string
    #       obj.to_json
    #     end
    #
    #     def deserialize(str)
    #       # Convert string back to Ruby object
    #       JSON.parse(str)
    #     end
    #   end
    #
    #   PGMQ.configure { |c| c.serializer = MySerializer.new }
    class Base
      # Serializes a Ruby object into a string for storage
      #
      # @param obj [Object] the object to serialize
      # @return [String] serialized representation
      # @raise [NotImplementedError] if not implemented by subclass
      def serialize(obj)
        raise NotImplementedError, "#{self.class} must implement #serialize"
      end

      # Deserializes a string back into a Ruby object
      #
      # @param str [String] the string to deserialize
      # @return [Object] deserialized object
      # @raise [NotImplementedError] if not implemented by subclass
      def deserialize(str)
        raise NotImplementedError, "#{self.class} must implement #deserialize"
      end
    end
  end
end
