# frozen_string_literal: true

module PGMQ
  # Base error class for all PGMQ errors
  class Error < StandardError; end

  # Raised when connection to PostgreSQL fails or is lost
  class ConnectionError < Error; end

  # Raised when a queue operation is attempted on a non-existent queue
  class QueueNotFoundError < Error; end

  # Raised when a message cannot be found
  class MessageNotFoundError < Error; end

  # Raised when message serialization fails
  class SerializationError < Error; end

  # Raised when message deserialization fails
  class DeserializationError < Error; end

  # Raised when configuration is invalid
  class ConfigurationError < Error; end

  # Raised when an invalid queue name is provided
  class InvalidQueueNameError < Error; end
end
