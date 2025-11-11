# frozen_string_literal: true

require 'time'
require 'json'

module PGMQ
  # Represents a message read from a PGMQ queue
  #
  # @example Reading and accessing a message
  #   msg = client.read("my_queue", vt: 30)
  #   puts msg.msg_id      # => 123
  #   puts msg.payload     # => { "order_id" => 456 }
  #   puts msg[:order_id]  # => 456
  class Message
    # @return [Integer] message ID
    attr_reader :msg_id

    # @return [Integer] number of times this message has been read
    attr_reader :read_ct

    # @return [Time] when the message was enqueued
    attr_reader :enqueued_at

    # @return [Time] when the message visibility timeout expires
    attr_reader :vt

    # @return [Hash, Object] the message payload
    attr_reader :message

    # @return [String, Hash] the raw message before deserialization
    attr_reader :raw_message

    # Creates a new Message from a database row
    # @param row [Hash] database row from PG result
    # @param serializer [PGMQ::Serializers::Base] serializer for deserializing message
    def initialize(row, serializer: nil)
      @msg_id = row['msg_id'].to_i
      @read_ct = row['read_ct'].to_i
      @enqueued_at = parse_timestamp(row['enqueued_at'])
      @vt = parse_timestamp(row['vt'])

      # Deferred deserialization: store raw message and serializer
      # Message will be deserialized on first access (Karafka-style)
      @raw_message = row['message']
      @serializer = serializer
      @message = nil
      @deserialized = false
    end

    # Lazy deserialization - only deserialize when message is accessed
    # @return [Hash, Object]
    def message
      return @message if @deserialized

      @message = deserialize_message(@raw_message, @serializer)
      @deserialized = true
      @message
    end

    # Alias for message_id
    # @return [Integer]
    alias id msg_id

    # Alias for message (more semantic)
    # @return [Hash, Object]
    alias payload message

    # Provides hash-like access to message payload
    # @param key [String, Symbol] the key to access
    # @return [Object, nil]
    def [](key)
      return nil unless message.respond_to?(:[])

      message[key.to_s] || message[key.to_sym]
    end

    # Returns a hash representation of the message
    # @return [Hash]
    def to_h
      {
        msg_id: @msg_id,
        read_ct: @read_ct,
        enqueued_at: @enqueued_at,
        vt: @vt,
        message: message
      }
    end

    # String representation
    # @return [String]
    def inspect
      "#<#{self.class.name} msg_id=#{@msg_id} read_ct=#{@read_ct} " \
        "enqueued_at=#{@enqueued_at} vt=#{@vt}>"
    end

    private

    # Parses a timestamp string or object into a Time
    # @param value [String, Time, nil]
    # @return [Time, nil]
    def parse_timestamp(value)
      return nil if value.nil?
      return value if value.is_a?(Time)

      Time.parse(value.to_s)
    rescue ArgumentError
      nil
    end

    # Deserializes the message using the provided serializer or defaults to JSON
    # @param value [String, Hash]
    # @param serializer [PGMQ::Serializers::Base, nil]
    # @return [Hash, Object]
    def deserialize_message(value, serializer)
      # If already a Hash, return as-is
      return value if value.is_a?(Hash)

      # Use provided serializer or fallback to JSON parsing
      if serializer
        serializer.deserialize(value)
      else
        JSON.parse(value.to_s)
      end
    rescue JSON::ParserError, StandardError => e
      raise PGMQ::Errors::SerializationError, "Failed to deserialize message: #{e.message}"
    end
  end
end
