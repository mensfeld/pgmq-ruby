# frozen_string_literal: true

module PGMQ
  # Represents a message read from a PGMQ queue
  #
  # Returns raw values from PostgreSQL without transformation.
  # Higher-level frameworks should handle parsing, deserialization, etc.
  #
  # @example Reading a message (raw values)
  #   msg = client.read("my_queue", vt: 30)
  #   puts msg.msg_id         # => "123" (String from PG)
  #   puts msg.read_ct        # => "1" (String from PG)
  #   puts msg.enqueued_at    # => "2025-01-15 10:30:00+00" (String from PG)
  #   puts msg.vt             # => "2025-01-15 10:30:30+00" (String from PG)
  #   puts msg.message        # => "{\"order_id\":456}" (Raw JSONB string)
  #   puts msg.headers        # => "{\"trace_id\":\"abc123\"}" (Raw JSONB string, optional)
  #   puts msg.queue_name     # => "my_queue" (only present for multi-queue operations)
  class Message < Data.define(
    :msg_id, :read_ct, :enqueued_at, :vt, :message, :headers, :queue_name
  )
    class << self
      # Creates a new Message from a database row
      # @param row [Hash] database row from PG result
      # @return [Message]
      def new(row, **)
        # Return raw values as-is from PostgreSQL
        # No parsing, no deserialization, no transformation
        # The pg gem returns JSONB as String by default
        super(
          msg_id: row['msg_id'],
          read_ct: row['read_ct'],
          enqueued_at: row['enqueued_at'],
          vt: row['vt'],
          message: row['message'],
          headers: row['headers'], # JSONB column for metadata (optional)
          queue_name: row['queue_name'] # nil for single-queue operations
        )
      end
    end

    # Alias for msg_id (common in messaging systems)
    # @return [String]
    alias id msg_id
  end
end
