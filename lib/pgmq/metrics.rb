# frozen_string_literal: true

require "time"

module PGMQ
  # Represents metrics for a PGMQ queue
  #
  # `queue_visible_length` (messages whose visibility timeout has passed) needs PGMQ v1.5.0+.
  # `default_partition_length` (estimated rows in the default partitions of a partitioned queue
  # and its archive) needs PGMQ v1.13.0+ and is `nil` for non-partitioned queues. A non-zero value
  # means pg_partman maintenance is failing. On older PGMQ versions both are `nil`.
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
    :scrape_time,
    :queue_visible_length,
    :default_partition_length
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
          scrape_time: row["scrape_time"],
          queue_visible_length: row["queue_visible_length"],
          default_partition_length: row["default_partition_length"]
        )
      end
    end
  end
end
