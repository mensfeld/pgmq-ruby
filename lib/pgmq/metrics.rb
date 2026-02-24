# frozen_string_literal: true

require "time"

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
      def new(row, **)
        # Return raw values as-is from PostgreSQL
        super(
          queue_name: row["queue_name"],
          queue_length: row["queue_length"],
          newest_msg_age_sec: row["newest_msg_age_sec"],
          oldest_msg_age_sec: row["oldest_msg_age_sec"],
          total_messages: row["total_messages"],
          scrape_time: row["scrape_time"]
        )
      end
    end
  end
end
