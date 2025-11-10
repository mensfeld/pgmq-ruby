# frozen_string_literal: true

module PGMQ
  # PGMQ errors namespace
  module Errors
    # Base error class for all PGMQ errors
    class BaseError < StandardError; end

    # Raised when connection to PostgreSQL fails or is lost
    class ConnectionError < BaseError; end

    # Raised when a queue operation is attempted on a non-existent queue
    class QueueNotFoundError < BaseError; end

    # Raised when a message cannot be found
    class MessageNotFoundError < BaseError; end

    # Raised when message serialization fails
    class SerializationError < BaseError; end

    # Raised when message deserialization fails
    class DeserializationError < BaseError; end

    # Raised when configuration is invalid
    class ConfigurationError < BaseError; end

    # Raised when an invalid queue name is provided
    class InvalidQueueNameError < BaseError; end
  end
end
