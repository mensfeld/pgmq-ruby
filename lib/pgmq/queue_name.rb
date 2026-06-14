# frozen_string_literal: true

module PGMQ
  # Queue name validation, normalization, and sanitization.
  #
  # PGMQ interpolates a queue's name into PostgreSQL identifiers (it creates tables named +pgmq.q_<name>+ and
  # +pgmq.a_<name>+), so a name has to be a valid, length-bounded SQL identifier. This module is the single source of
  # truth for those rules and offers three tiers depending on how much you trust the input:
  #
  # 1. {.validate!} / {.valid?} - assert a name is already valid. Use for names you control. {PGMQ::Client} calls
  #    {.validate!} before every operation.
  # 2. {.normalize} - lightly rewrite a name that is *meant* to be valid but uses a friendlier separator (hyphens,
  #    colons, dots, spaces), e.g. a Turbo Stream channel like +"chat:room-7"+. Raises if the result still can't be a
  #    valid name (empty, or starts with a digit).
  # 3. {.sanitize} - coerce *arbitrary, untrusted* input into a valid name on a best-effort basis. Never raises for
  #    content; always returns a usable identifier (falling back to a default when nothing usable remains).
  #
  # @example
  #   PGMQ::QueueName.valid?("orders")          # => true
  #   PGMQ::QueueName.validate!("my-queue")     # => raises InvalidQueueNameError
  #   PGMQ::QueueName.normalize("chat:room-7")  # => "chat_room_7"
  #   PGMQ::QueueName.sanitize("99 Problems!")  # => "q_99_problems"
  module QueueName
    # Maximum queue name length. PGMQ creates tables with prefixes (+q_+, +a_+) and PostgreSQL caps identifiers at 63
    # characters; PGMQ enforces 48 to leave room for those prefixes and suffixes.
    MAX_LENGTH = 48

    # A valid queue name: starts with a letter or underscore, then letters, digits, or underscores.
    PATTERN = /\A[a-zA-Z_][a-zA-Z0-9_]*\z/

    # Prefix used by {.sanitize} when the input would otherwise start with an illegal leading character (e.g. a
    # digit) but still has usable trailing characters.
    SANITIZE_PREFIX = "q_"

    # Name {.sanitize} falls back to when the input has no usable characters at all.
    SANITIZE_FALLBACK = "queue"

    module_function

    # Returns true if the name is already a valid queue name.
    #
    # @param name [String, #to_s] candidate queue name
    # @return [Boolean]
    def valid?(name)
      str = name.to_s
      !str.empty? && str.length < MAX_LENGTH && str.match?(PATTERN)
    end

    # Validates a queue name, returning it unchanged when valid and raising otherwise.
    #
    # @param name [String, #to_s] candidate queue name
    # @return [String] the validated name (as a String)
    # @raise [PGMQ::Errors::InvalidQueueNameError] if the name is empty, too long, or not a valid identifier
    def validate!(name)
      str = name.to_s

      if name.nil? || str.strip.empty?
        raise Errors::InvalidQueueNameError, "Queue name cannot be empty"
      end

      if str.length >= MAX_LENGTH
        raise Errors::InvalidQueueNameError,
          "Queue name '#{str}' exceeds maximum length of #{MAX_LENGTH} characters " \
          "(current length: #{str.length})"
      end

      return str if str.match?(PATTERN)

      raise Errors::InvalidQueueNameError,
        "Invalid queue name '#{str}': must start with a letter or underscore " \
        "and contain only letters, digits, and underscores"
    end

    # Rewrites a name that is meant to be valid but uses friendlier separators.
    #
    # Trims surrounding whitespace, replaces any run of non-identifier characters with a single underscore, and
    # collapses repeated underscores. This turns names like +"chat:room-7"+ or +"order events"+ into +"chat_room_7"+
    # / +"order_events"+. The result is then validated, so names that still can't be valid (empty, or starting with a
    # digit) raise rather than being silently mangled.
    #
    # @param name [String, #to_s] a name using friendly separators
    # @return [String] the normalized, validated queue name
    # @raise [PGMQ::Errors::InvalidQueueNameError] if the normalized result is not a valid queue name
    def normalize(name)
      normalized = name.to_s.strip
        .gsub(/[^a-zA-Z0-9_]+/, "_").squeeze("_")
        .gsub(/\A_+|_+\z/, "")

      validate!(normalized)
    end

    # Coerces arbitrary, untrusted input into a valid queue name on a best-effort basis.
    #
    # Unlike {.normalize}, this never raises for content: it lowercases, replaces every illegal character run with an
    # underscore, strips leading/trailing underscores, prefixes {SANITIZE_PREFIX} when the first surviving character
    # is not a valid identifier start (e.g. a digit), truncates to fit {MAX_LENGTH}, and falls back to
    # {SANITIZE_FALLBACK} when nothing usable remains. The return value always satisfies {.valid?}.
    #
    # Use for names derived from user input or external systems where you would rather get a deterministic, safe
    # queue name than an exception.
    #
    # @param name [String, #to_s] untrusted input
    # @return [String] a guaranteed-valid queue name
    def sanitize(name)
      cleaned = name.to_s.downcase
        .gsub(/[^a-z0-9_]+/, "_").squeeze("_")
        .gsub(/\A_+|_+\z/, "")

      # Nothing usable survived the scrub - use the fallback rather than emit a bare prefix like "q_".
      return SANITIZE_FALLBACK if cleaned.empty?

      # A valid identifier can't start with a digit; prefix so the leading character is legal.
      cleaned = "#{SANITIZE_PREFIX}#{cleaned}" unless cleaned.match?(/\A[a-z_]/)

      # Keep under MAX_LENGTH (valid? requires strictly less than), trimming any underscore left at the cut.
      cleaned = cleaned[0, MAX_LENGTH - 1].sub(/_+\z/, "") if cleaned.length >= MAX_LENGTH

      cleaned.empty? ? SANITIZE_FALLBACK : cleaned
    end
  end
end
