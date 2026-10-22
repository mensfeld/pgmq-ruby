# frozen_string_literal: true

module PGMQ
  # Contracts that validate user input before it reaches PostgreSQL
  module Contracts
    class << self
      # @param scope [String] contract key under `en.validations` in `config/locales/errors.yml`
      # @return [Hash{String => String}] error messages of one contract
      def error_messages(scope)
        @error_messages ||= YAML.safe_load_file(File.join(PGMQ.gem_root, "config", "locales", "errors.yml"))

        @error_messages.fetch("en").fetch("validations").fetch(scope)
      end
    end
  end
end
