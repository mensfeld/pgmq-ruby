# frozen_string_literal: true

module PGMQ
  module Contractable
    # Base contract for all the contracts that check data format
    #
    # @example
    #   class MyContract < PGMQ::Contractable::Contract
    #     self.error_messages = { "id_format" => "must be a String" }
    #
    #     required(:id) { |id| id.is_a?(String) }
    #   end
    #
    #   MyContract.new.validate!({ id: 1 }, ArgumentError)
    #
    # @note This contract does NOT support rules inheritance.
    class Contract
      # Constant representing a miss during dig
      # We use it as a result value not to return an array with found object and a state to prevent additional array
      # allocation
      DIG_MISS = Object.new

      # Empty array for scope default to avoid allocating a new Array on each `#call` / `#validate!` invocation. Safe
      # because scope is never mutated - it is only used in `scope + rule.path` which creates a new Array.
      EMPTY_ARRAY = [].freeze

      private_constant :DIG_MISS, :EMPTY_ARRAY

      # Class level API definitions
      class << self
        # @return [Array<Rule>] all the validation rules defined for a given contract
        attr_reader :rules

        # @return [Hash{String => String}] error messages keyed by "<path>_<error key>" or "<error key>"
        attr_accessor :error_messages

        # Allows for definition of a scope/namespace for nested validations
        #
        # @param path [Symbol] path in the hash for nesting
        #
        # @example
        #   nested(:key) do
        #     required(:inside) { |inside| inside.is_a?(String) }
        #   end
        def nested(path, &)
          init_accu
          @nested << path
          begin
            instance_eval(&)
          ensure
            # Always pop, even if the block raises, so a rescued exception during contract definition does not
            # prefix `path` onto every rule defined afterwards
            @nested.pop
          end
        end

        # Defines a rule for a required field (required means, that will automatically create an error if missing)
        #
        # @param keys [Array<Symbol>] single or full path
        # @param block [Proc] validation rule
        def required(*keys, &block)
          init_accu
          @rules << Rule.new(@nested + keys, :required, block).freeze
        end

        # Defines a rule for an optional field (no error when missing)
        #
        # @param keys [Array<Symbol>] single or full path
        # @param block [Proc] validation rule
        def optional(*keys, &block)
          init_accu
          @rules << Rule.new(@nested + keys, :optional, block).freeze
        end

        # Defines a virtual rule that validates the whole data rather than a single key. Unlike
        # `required`/`optional`, the block receives the full data and returns its own errors.
        #
        # @param block [Proc] validation rule, called with `(data, errors, contract)`. It must return either a
        #   non-Array (`true`/`nil`/`false`) for "no errors", or an `Array` of `[path, message]` error pairs (where
        #   `path` is itself an `Array` of symbols).
        #
        # @note The returned error `Array` is owned by the contract: `#call` prepends the current scope onto each pair
        #   in place and collects them, so a rule must return a freshly built `Array` on every call.
        def virtual(&block)
          init_accu
          @rules << Rule.new([], :virtual, block).freeze
        end

        private

        # Initializes nestings and rules building accumulator
        def init_accu
          @nested ||= []
          @rules ||= []
        end
      end

      # Runs the validation
      #
      # @param data [Hash] hash with data we want to validate
      # @param scope [Array<String>] scope of this contract (if any) or empty array if no parent scope is needed if
      #   contract starts from root
      # @return [Result] validation result
      def call(data, scope: EMPTY_ARRAY)
        errors = []

        # A non-Hash root has no keys to dig, so every required rule reports missing
        data_is_hash = data.is_a?(Hash)

        self.class.rules.each do |rule|
          if rule.type == :virtual
            result = rule.validator.call(data, errors, self)

            # Any non-Array result (true, but also nil/false) means "no errors"
            next unless result.is_a?(Array)

            result.each do |sub_result|
              sub_result[0] = scope + sub_result[0]
            end

            errors.push(*result)
          else
            for_checking = data_is_hash ? dig(data, rule.path) : DIG_MISS

            if DIG_MISS.equal?(for_checking)
              errors << [scope + rule.path, :missing] if rule.type == :required
            else
              result = rule.validator.call(for_checking, data, errors, self)

              next if result == true

              errors << [scope + rule.path, result || :format]
            end
          end
        end

        return Result.success if errors.empty?

        Result.new(errors, self)
      end

      # Runs the validation like `#call`, but raises instead of returning a failed `Result`
      #
      # @param data [Hash] data for validation
      # @param error_class [Class] error class that should be used when validation fails
      # @param scope [Array<String>] scope of this contract (if any) or empty array if no parent scope is needed if
      #   contract starts from root
      # @return [Boolean] true
      # @raise [StandardError] any error provided in the error_class that inherits from the standard error
      def validate!(data, error_class, scope: EMPTY_ARRAY)
        result = call(data, scope: scope)

        return true if result.success?

        raise error_class, result.errors
      end

      private

      # Tries to dig for a given key in a hash and returns it, or DIG_MISS when not found (dig returns nil and we
      # don't know if it wasn't the digged key value)
      #
      # @param data [Hash]
      # @param keys [Array<Symbol>]
      # @return [DIG_MISS, Object] found element or DIG_MISS indicating that not found
      def dig(data, keys)
        case keys.length
        when 1
          data.fetch(keys[0], DIG_MISS)
        when 2
          mid = data.fetch(keys[0], DIG_MISS)

          return DIG_MISS if DIG_MISS.equal?(mid)
          return DIG_MISS unless mid.is_a?(Hash)

          mid.fetch(keys[1], DIG_MISS)
        else
          current = data

          keys.each do |nesting|
            return DIG_MISS unless current.is_a?(Hash)

            current = current.fetch(nesting, DIG_MISS)

            return DIG_MISS if DIG_MISS.equal?(current)
          end

          current
        end
      end
    end
  end
end
