# frozen_string_literal: true

require 'time'

module PGMQ
  # Represents metrics for a PGMQ queue
  #
  # @example Getting queue metrics
  #   metrics = client.metrics("my_queue")
  #   puts metrics.queue_length        # => 42
  #   puts metrics.oldest_msg_age_sec  # => 3600
  class Metrics < Data.define(
    :queue_name,
    :queue_length,
    :newest_msg_age_sec,
    :oldest_msg_age_sec,
    :total_messages,
    :scrape_time
  )
    class << self
      # Creates a new Metrics object from a database row
      # @param row [Hash] database row from PG result
      # @return [Metrics]
      def new(row)
        queue_name = row['queue_name']
        queue_length = row['queue_length'].to_i
        newest_msg_age_sec = row['newest_msg_age_sec']&.to_i
        oldest_msg_age_sec = row['oldest_msg_age_sec']&.to_i
        total_messages = row['total_messages'].to_i
        scrape_time = parse_timestamp(row['scrape_time'])

        super(
          queue_name:,
          queue_length:,
          newest_msg_age_sec:,
          oldest_msg_age_sec:,
          total_messages:,
          scrape_time:
        )
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
end
