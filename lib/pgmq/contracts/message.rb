# frozen_string_literal: true

module PGMQ
  module Contracts
    # Validates a message and its headers before they are bound
    #
    # The pg gem reads a Hash bind parameter as a parameter spec and binds `nil` as SQL NULL, so a non-String value
    # could be stored as NULL without an error.
    class Message < Contractable::Contract
      self.error_messages = YAML.safe_load_file(
        File.join(PGMQ.gem_root, "config", "locales", "errors.yml")
      ).fetch("en").fetch("validations").fetch("message")

      required(:message) { |message| message.is_a?(String) }
      optional(:headers) { |headers| headers.nil? || headers.is_a?(String) }
    end
  end
end
