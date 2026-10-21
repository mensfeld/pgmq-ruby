# frozen_string_literal: true

module PGMQ
  # Contract layer for validating user input before it reaches PostgreSQL
  #
  # Extracted from karafka-core's contractable layer (also by Maciej Mensfeld), without its dependencies. It allows for
  # nested validations, virtual rules and YAML based error messages.
  #
  # @note It is thread-safe to run but validations definitions should happen before threads are used.
  module Contractable
  end
end
