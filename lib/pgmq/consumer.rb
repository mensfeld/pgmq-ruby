# frozen_string_literal: true

module PGMQ
  # Minimal single-threaded consumer for one or more queues
  #
  # Wraps an existing {PGMQ::Client} with a blocking `each` loop, similar to the simple consumer of rdkafka-ruby. It
  # long-polls the queue with `read_with_poll` (or several queues with `read_multi_with_poll`), yields one message at a
  # time and acknowledges it (delete or archive) on its own queue when the block returns. The consumer does not own the
  # client: closing the client stays the caller's job.
  #
  # Delivery is at-least-once. When the block raises, the message is not acknowledged and becomes visible again once
  # its visibility timeout expires. When processing takes longer than `vt`, another reader may receive the same message.
  #
  # @example Basic usage
  #   client = PGMQ::Client.new(ENV["DATABASE_URL"])
  #   consumer = PGMQ::Consumer.new(client, "orders", vt: 30)
  #
  #   trap("TERM") { consumer.stop }
  #   trap("INT") { consumer.stop }
  #
  #   consumer.each do |message|
  #     process(JSON.parse(message.message))
  #   end
  #
  #   client.close
  #
  # @example Several queues
  #   consumer = PGMQ::Consumer.new(client, %w[orders emails], vt: 30)
  #
  #   consumer.each do |message|
  #     puts "#{message.queue_name}: #{message.message}"
  #   end
  class Consumer
    # Acknowledgement actions available for successfully processed messages
    ON_SUCCESS_ACTIONS = %i[delete archive].freeze

    private_constant :ON_SUCCESS_ACTIONS

    # @return [PGMQ::Client] client used for all queue operations
    attr_reader :client

    # @return [Array<String>] names of the consumed queues
    attr_reader :queue_names

    # Creates a new consumer
    #
    # @param client [PGMQ::Client] client used for all queue operations
    # @param queue_names [String, Array<String>] name of the queue to consume, or names of up to 50 queues
    # @param vt [Integer] visibility timeout in seconds for read messages
    # @param batch_size [Integer] number of messages to read per queue per poll (still yielded one at a time)
    # @param max_poll_seconds [Integer] how long a single poll waits for messages
    # @param poll_interval_ms [Integer] interval between server-side checks within a poll
    # @param on_success [Symbol] `:delete` or `:archive`, applied after the block returns
    # @param max_attempts [Integer, nil] archive without yielding once `read_ct` exceeds this value (nil disables).
    #   Requires `batch_size: 1`; with several queues, one message is then read per poll
    # @raise [PGMQ::Errors::ConfigurationError] when an option or the list of queues is invalid
    # @raise [PGMQ::Errors::InvalidQueueNameError] when a queue name is invalid
    #
    # @example
    #   consumer = PGMQ::Consumer.new(client, "orders", on_success: :archive, max_attempts: 5)
    def initialize(
      client,
      queue_names,
      vt: Client::DEFAULT_VT,
      batch_size: 1,
      max_poll_seconds: 5,
      poll_interval_ms: 100,
      on_success: :delete,
      max_attempts: nil
    )
      unless ON_SUCCESS_ACTIONS.include?(on_success)
        raise Errors::ConfigurationError, "on_success must be one of #{ON_SUCCESS_ACTIONS.inspect}"
      end

      validate_positive_integer!(:vt, vt)
      validate_positive_integer!(:batch_size, batch_size)
      validate_positive_integer!(:max_poll_seconds, max_poll_seconds)
      validate_positive_integer!(:poll_interval_ms, poll_interval_ms)

      unless max_attempts.nil?
        validate_positive_integer!(:max_attempts, max_attempts)

        # Every read raises read_ct, so with larger batches the messages released after a failure would have their
        # unprocessed read counted as a failed attempt
        raise Errors::ConfigurationError, "max_attempts requires batch_size: 1" unless batch_size == 1
      end

      @client = client
      @queue_names = validate_queue_names!(queue_names)
      @vt = vt
      @batch_size = batch_size
      @max_poll_seconds = max_poll_seconds
      @poll_interval_ms = poll_interval_ms
      @on_success = on_success
      @max_attempts = max_attempts
      @poll_offset = 0
      @stopped = false
    end

    # Consumes messages until {#stop} is called
    #
    # Each message is acknowledged with the `on_success` action on its own queue after the block returns. An exception
    # raised by the block stops the loop and is re-raised: that message is left to reappear after its visibility
    # timeout, and the not yet yielded messages of the same batch are released at once. Messages read more than
    # `max_attempts` times are archived without being yielded.
    #
    # {#stop} takes effect once the messages of the current poll are processed, or after the current poll (up to
    # `max_poll_seconds`) when the queue is empty. Messages that were read are always yielded, so a stop never counts
    # a read that was not processed.
    #
    # @yieldparam message [PGMQ::Message] message to process, with `queue_name` set
    # @return [nil]
    # @raise [ArgumentError] when no block is given
    #
    # @example
    #   consumer.each { |message| puts message.message }
    def each
      raise ArgumentError, "a block is required" unless block_given?

      until @stopped
        pending = poll

        begin
          until pending.empty?
            message = pending.shift

            if exhausted?(message)
              @client.archive(message.queue_name, message.msg_id)
              next
            end

            yield message

            acknowledge(message)
          end
        ensure
          release(pending)
        end
      end

      nil
    end

    # Requests the loop in {#each} to finish
    #
    # Only sets a flag, so it is safe to call from a signal trap or from another thread. A stopped consumer stays
    # stopped: further calls to {#each} return at once.
    #
    # @return [void]
    def stop
      @stopped = true
    end

    # @return [Boolean] true once {#stop} was called
    def stopped?
      @stopped
    end

    private

    # @param queue_names [String, Array<String>] queue name or names
    # @return [Array<String>] validated, frozen queue names
    # @raise [PGMQ::Errors::ConfigurationError] when the list is empty, too long or has duplicates
    def validate_queue_names!(queue_names)
      names = Array(queue_names)

      raise Errors::ConfigurationError, "at least one queue name is required" if names.empty?
      raise Errors::ConfigurationError, "at most 50 queues can be consumed" if names.size > 50
      # pgmq folds queue names to lower case, so names differing only in case refer to the same queue
      unless names.map(&:downcase).uniq.size == names.size
        raise Errors::ConfigurationError, "queue names must be unique (case-insensitive)"
      end

      names.map { |name| QueueName.validate!(name) }.freeze
    end

    # Reads the next messages
    #
    # With several queues the read order rotates each poll, so a busy queue listed first does not starve the others.
    #
    # @return [Array<PGMQ::Message>] read messages, each with its `queue_name` set
    def poll
      if @queue_names.size == 1
        queue_name = @queue_names.first

        messages = @client.read_with_poll(
          queue_name,
          vt: @vt,
          qty: @batch_size,
          max_poll_seconds: @max_poll_seconds,
          poll_interval_ms: @poll_interval_ms
        )

        return messages.map { |message| message.with(queue_name: queue_name) }
      end

      queue_names = @queue_names.rotate(@poll_offset)
      @poll_offset = (@poll_offset + 1) % @queue_names.size

      @client.read_multi_with_poll(
        queue_names,
        vt: @vt,
        qty: @batch_size,
        # A message released after a failure keeps its raised read_ct, so max_attempts allows one read per poll
        limit: @max_attempts.nil? ? nil : 1,
        max_poll_seconds: @max_poll_seconds,
        poll_interval_ms: @poll_interval_ms
      )
    end

    # @param name [Symbol] option name used in the error message
    # @param value [Object] option value
    # @raise [PGMQ::Errors::ConfigurationError] when the value is not a positive Integer
    def validate_positive_integer!(name, value)
      return if value.is_a?(Integer) && value.positive?

      raise Errors::ConfigurationError, "#{name} must be a positive Integer, got: #{value.inspect}"
    end

    # @param message [PGMQ::Message] read message
    # @return [Boolean] true when the message exceeded the allowed number of reads
    def exhausted?(message)
      !@max_attempts.nil? && message.read_ct.to_i > @max_attempts
    end

    # @param message [PGMQ::Message] processed message
    def acknowledge(message)
      if @on_success == :archive
        @client.archive(message.queue_name, message.msg_id)
      else
        @client.delete(message.queue_name, message.msg_id)
      end
    end

    # Makes read but not yielded messages visible again right away
    #
    # Failures are ignored so they never replace an error raised by the block; the messages then reappear after
    # their visibility timeout.
    #
    # @param messages [Array<PGMQ::Message>] messages to release
    def release(messages)
      messages.group_by(&:queue_name).each do |queue_name, queue_messages|
        @client.set_vt_batch(queue_name, queue_messages.map(&:msg_id), vt: 0)
      rescue
        nil
      end
    end
  end
end
