# frozen_string_literal: true

module PGMQ
  # Represents the NOTIFY throttle configuration for a queue
  #
  # @example Inspecting notification configuration
  #   throttles = client.list_notify_insert_throttles
  #   throttles.each do |t|
  #     puts "#{t.queue_name}: #{t.throttle_interval_ms}ms (last notified: #{t.last_notified_at})"
  #   end
  class NotifyThrottle < Data.define(:queue_name, :throttle_interval_ms, :last_notified_at)
    class << self
      # Creates a new NotifyThrottle from a database row
      # @param row [Hash] database row from PG result
      # @return [NotifyThrottle]
      def new(row, **)
        super(
          queue_name: row["queue_name"],
          throttle_interval_ms: row["throttle_interval_ms"],
          last_notified_at: row["last_notified_at"]
        )
      end
    end
  end
end
