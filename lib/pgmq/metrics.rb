# frozen_string_literal: true

require 'time'

module PGMQ
  # Represents metrics for a PGMQ queue
  #
  # @example Getting queue metrics
  #   metrics = client.metrics("my_queue")
  #   puts metrics.queue_length        # => 42
  #   puts metrics.oldest_msg_age_sec  # => 3600
  class Metrics
    # @return [String] name of the queue
    attr_reader :queue_name

    # @return [Integer] current number of messages in the queue
    attr_reader :queue_length

    # @return [Integer, nil] age of the newest message in seconds
    attr_reader :newest_msg_age_sec

    # @return [Integer, nil] age of the oldest message in seconds
    attr_reader :oldest_msg_age_sec

    # @return [Integer] total number of messages ever sent to this queue
    attr_reader :total_messages

    # @return [Time] when these metrics were captured
    attr_reader :scrape_time

    # Creates a new Metrics object from a database row
    # @param row [Hash] database row from PG result
    def initialize(row)
      @queue_name = row['queue_name']
      @queue_length = row['queue_length'].to_i
      @newest_msg_age_sec = row['newest_msg_age_sec']&.to_i
      @oldest_msg_age_sec = row['oldest_msg_age_sec']&.to_i
      @total_messages = row['total_messages'].to_i
      @scrape_time = parse_timestamp(row['scrape_time'])
    end

    # Returns a hash representation of the metrics
    # @return [Hash]
    def to_h
      {
        queue_name: @queue_name,
        queue_length: @queue_length,
        newest_msg_age_sec: @newest_msg_age_sec,
        oldest_msg_age_sec: @oldest_msg_age_sec,
        total_messages: @total_messages,
        scrape_time: @scrape_time
      }
    end

    # String representation
    # @return [String]
    def inspect
      "#<#{self.class.name} queue_name=#{@queue_name} " \
        "queue_length=#{@queue_length} " \
        "oldest_msg_age_sec=#{@oldest_msg_age_sec}>"
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
  end
end
