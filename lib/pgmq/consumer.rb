# frozen_string_literal: true

module PGMQ
  # Minimal single-threaded consumer for one queue
  #
  # Wraps an existing {PGMQ::Client} with a blocking `each` loop, similar to the simple consumer of rdkafka-ruby. It
  # long-polls the queue with `read_with_poll`, yields one message at a time and acknowledges it (delete or archive)
  # when the block returns. The consumer does not own the client: closing the client stays the caller's job.
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
  class Consumer
    # Acknowledgement actions available for successfully processed messages
    ON_SUCCESS_ACTIONS = %i[delete archive].freeze

    private_constant :ON_SUCCESS_ACTIONS

    # @return [PGMQ::Client] client used for all queue operations
    attr_reader :client

    # @return [String] name of the consumed queue
    attr_reader :queue_name

    # Creates a new consumer
    #
    # @param client [PGMQ::Client] client used for all queue operations
    # @param queue_name [String] name of the queue to consume
    # @param vt [Integer] visibility timeout in seconds for read messages
    # @param batch_size [Integer] number of messages to read per poll (still yielded one at a time)
    # @param max_poll_seconds [Integer] how long a single poll waits for messages
    # @param poll_interval_ms [Integer] interval between server-side checks within a poll
    # @param on_success [Symbol] `:delete` or `:archive`, applied after the block returns
    # @param max_attempts [Integer, nil] archive without yielding once `read_ct` exceeds this value (nil disables).
    #   Requires `batch_size: 1`
    # @raise [PGMQ::Errors::ConfigurationError] when an option is invalid
    # @raise [PGMQ::Errors::InvalidQueueNameError] when the queue name is invalid
    #
    # @example
    #   consumer = PGMQ::Consumer.new(client, "orders", on_success: :archive, max_attempts: 5)
    def initialize(
      client,
      queue_name,
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
      @queue_name = QueueName.validate!(queue_name)
      @vt = vt
      @batch_size = batch_size
      @max_poll_seconds = max_poll_seconds
      @poll_interval_ms = poll_interval_ms
      @on_success = on_success
      @max_attempts = max_attempts
      @stopped = false
    end

    # Consumes messages until {#stop} is called
    #
    # Each message is acknowledged with the `on_success` action after the block returns. An exception raised by the
    # block stops the loop and is re-raised: that message is left to reappear after its visibility timeout, and the
    # not yet yielded messages of the same batch are released at once. Messages read more than `max_attempts` times
    # are archived without being yielded.
    #
    # {#stop} takes effect once the messages of the current poll are processed, or after the current poll (up to
    # `max_poll_seconds`) when the queue is empty. Messages that were read are always yielded, so a stop never counts
    # a read that was not processed.
    #
    # @yieldparam message [PGMQ::Message] message to process
    # @return [nil]
    # @raise [ArgumentError] when no block is given
    #
    # @example
    #   consumer.each { |message| puts message.message }
    def each
      raise ArgumentError, "a block is required" unless block_given?

      until @stopped
        messages = @client.read_with_poll(
          @queue_name,
          vt: @vt,
          qty: @batch_size,
          max_poll_seconds: @max_poll_seconds,
          poll_interval_ms: @poll_interval_ms
        )

        pending = messages.dup

        begin
          until pending.empty?
            message = pending.shift

            if exhausted?(message)
              @client.archive(@queue_name, message.msg_id)
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
        @client.archive(@queue_name, message.msg_id)
      else
        @client.delete(@queue_name, message.msg_id)
      end
    end

    # Makes read but not yielded messages visible again right away
    #
    # Failures are ignored so they never replace an error raised by the block; the messages then reappear after
    # their visibility timeout.
    #
    # @param messages [Array<PGMQ::Message>] messages to release
    def release(messages)
      return if messages.empty?

      @client.set_vt_batch(@queue_name, messages.map(&:msg_id), vt: 0)
    rescue
      nil
    end
  end
end
