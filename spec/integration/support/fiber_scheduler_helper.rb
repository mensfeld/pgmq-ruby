# frozen_string_literal: true

# Helper module for Fiber Scheduler integration tests.
#
# Provides utilities for running code under Ruby's Fiber Scheduler API
# and measuring concurrent execution to verify true non-blocking I/O.

# Suppress async gem's console logging during tests
ENV["CONSOLE_LEVEL"] ||= "fatal"

require "async"

module FiberSchedulerHelper
  # Runs a block under the Async fiber scheduler.
  #
  # @yield [task] Block to execute with fiber scheduler
  # @yieldparam task [Async::Task] The root async task
  # @return [Object] Result of the block
  def self.with_scheduler(&block)
    Async(&block)
  end

  # Tracks execution timing for multiple operations to verify concurrent execution.
  #
  # Returns timing data that can be used to prove operations ran concurrently:
  # - If operations ran sequentially: total_time ≈ sum of individual times
  # - If operations ran concurrently: total_time ≈ max of individual times
  #
  # @param operation_count [Integer] Number of operations being tracked
  # @return [ConcurrencyTracker] Tracker instance
  def self.create_tracker(operation_count)
    ConcurrencyTracker.new(operation_count)
  end

  # Tracks concurrent operation timing and overlap detection.
  class ConcurrencyTracker
    attr_reader :timings

    def initialize(operation_count)
      @operation_count = operation_count
      @timings = []
      @mutex = Mutex.new
      @start_time = nil
    end

    # Records an operation's execution time.
    #
    # @param operation_id [Object] Identifier for this operation
    # @yield Block to time
    # @return [Object] Result of the block
    def track(operation_id)
      @mutex.synchronize { @start_time ||= Time.now }

      op_start = Time.now
      result = yield
      op_end = Time.now

      @mutex.synchronize do
        @timings << {
          id: operation_id,
          start: op_start,
          end: op_end,
          duration: op_end - op_start
        }
      end

      result
    end

    # Returns the total elapsed time from first start to last end.
    #
    # @return [Float] Total elapsed time in seconds
    def total_elapsed
      return 0.0 if @timings.empty?

      first_start = @timings.map { |t| t[:start] }.min
      last_end = @timings.map { |t| t[:end] }.max
      last_end - first_start
    end

    # Returns the sum of all individual operation durations.
    #
    # @return [Float] Sum of durations in seconds
    def sum_of_durations
      @timings.sum { |t| t[:duration] }
    end

    # Calculates the concurrency ratio.
    #
    # A ratio close to 1.0 indicates sequential execution.
    # A ratio > 1.0 indicates concurrent execution (higher = more concurrent).
    #
    # @return [Float] Ratio of summed durations to total elapsed time
    def concurrency_ratio
      return 0.0 if total_elapsed.zero?

      sum_of_durations / total_elapsed
    end

    # Detects overlapping execution periods.
    #
    # @return [Array<Array>] Pairs of operation IDs that overlapped
    def overlapping_operations
      overlaps = []

      @timings.each_with_index do |t1, i|
        @timings[(i + 1)..].each do |t2|
          # Check if time ranges overlap
          if t1[:start] < t2[:end] && t2[:start] < t1[:end]
            overlaps << [t1[:id], t2[:id]]
          end
        end
      end

      overlaps
    end

    # Checks if operations showed true concurrent behavior.
    #
    # @param min_concurrency_ratio [Float] Minimum ratio to consider concurrent
    # @return [Boolean] True if concurrent execution detected
    def concurrent?(min_concurrency_ratio: 1.5)
      concurrency_ratio >= min_concurrency_ratio || overlapping_operations.any?
    end

    # Returns a summary of the concurrency analysis.
    #
    # @return [Hash] Summary statistics
    def summary
      {
        operation_count: @timings.size,
        total_elapsed: total_elapsed.round(4),
        sum_of_durations: sum_of_durations.round(4),
        concurrency_ratio: concurrency_ratio.round(2),
        overlapping_pairs: overlapping_operations.size,
        concurrent: concurrent?
      }
    end
  end
end
