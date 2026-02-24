# frozen_string_literal: true

require "time"

module PGMQ
  # Represents metadata about a PGMQ queue
  #
  # @example Listing queues
  #   queues = client.list_queues
  #   queues.each do |q|
  #     puts "#{q.queue_name} (partitioned: #{q.is_partitioned})"
  #   end
  class QueueMetadata < Data.define(:queue_name, :created_at, :is_partitioned, :is_unlogged)
    class << self
      # Creates a new QueueMetadata object from a database row
      # @param row [Hash] database row from PG result
      # @return [QueueMetadata]
      def new(row, **)
        # Return raw values as-is from PostgreSQL
        super(
          queue_name: row["queue_name"],
          created_at: row["created_at"],
          is_partitioned: row["is_partitioned"],
          is_unlogged: row["is_unlogged"]
        )
      end
    end

    # Alias for is_partitioned
    # @return [String] 't' or 'f' from PostgreSQL
    alias_method :partitioned?, :is_partitioned

    # Alias for is_unlogged
    # @return [String] 't' or 'f' from PostgreSQL
    alias_method :unlogged?, :is_unlogged
  end
end
