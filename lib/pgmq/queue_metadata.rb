# frozen_string_literal: true

require 'time'

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
      def new(row)
        queue_name = row['queue_name']
        created_at = parse_timestamp(row['created_at'])
        is_partitioned = parse_boolean(row['is_partitioned'])
        is_unlogged = parse_boolean(row['is_unlogged'])

        super(queue_name:, created_at:, is_partitioned:, is_unlogged:)
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

      # Parses a boolean value from various inputs
      # @param value [Boolean, String, nil]
      # @return [Boolean]
      def parse_boolean(value)
        return false if value.nil?
        return value if [true, false].include?(value)

        %w[t true].include?(value.to_s.downcase)
      end
    end

    # Alias for is_partitioned
    # @return [Boolean]
    alias partitioned? is_partitioned

    # Alias for is_unlogged
    # @return [Boolean]
    alias unlogged? is_unlogged
  end
end
