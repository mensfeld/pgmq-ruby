# frozen_string_literal: true

module PGMQ
  # Queue name validation, normalization, and sanitization.
  #
  # PGMQ interpolates a queue's name into PostgreSQL identifiers (it creates tables named +pgmq.q_<name>+ and
  # +pgmq.a_<name>+), so a name has to be a valid, length-bounded SQL identifier. This module is the single source of
  # truth for those rules and offers tiers depending on how much you trust the input:
  #
  # 1. {.validate!} / {.valid?} - assert a name is already valid. Use for names you control. {PGMQ::Client} calls
  #    {.validate!} before every operation.
  # 2. {.normalize} - lightly rewrite a name that is *meant* to be valid but uses a friendlier separator. Maps the
  #    common stream separators (hyphens, dots, colons) to underscores, strips any other invalid character, then
  #    validates - so a Turbo Stream channel like +"chat:room-7"+ becomes +"chat_room_7"+. Raises if the result still
  #    can't be a valid name (empty, or starts with a digit).
  # 3. {.sanitize!} - coerce *untrusted* input into a valid name by stripping every invalid character, then validate.
  #    Raises if nothing valid remains. Use this as a SQL-identifier guard: the result is always either a name you
  #    know is safe, or an exception - never a silent substitute.
  # 4. {.sanitize} - the lenient sibling of {.sanitize!}: best-effort coercion that *never* raises for content and
  #    always returns a usable identifier (falling back to a default). Convenient, but see the collision caveat on
  #    the method - distinct inputs can map to the same name.
  #
  # @example
  #   PGMQ::QueueName.valid?("orders")           # => true
  #   PGMQ::QueueName.validate!("my-queue")      # => raises InvalidQueueNameError
  #   PGMQ::QueueName.normalize("chat:room-7")   # => "chat_room_7"
  #   PGMQ::QueueName.sanitize!("orders!!")      # => "orders"
  #   PGMQ::QueueName.sanitize!("!!!")           # => raises InvalidQueueNameError
  #   PGMQ::QueueName.sanitize("99 Problems!")   # => "q_99_problems"
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

    # Rewrites a name that is meant to be valid but uses friendlier separators, then validates it.
    #
    # Maps the common stream-name separators - hyphens, dots, and colons - to underscores, strips any *other* invalid
    # character, then collapses repeated underscores and trims them from the ends. So +"chat:room-7"+ becomes
    # +"chat_room_7"+ and +"order.events"+ becomes +"order_events"+, while a stray +"a@b"+ becomes +"ab"+ (the +@+ is
    # dropped, not turned into a separator). The result is validated, so names that still can't be valid (empty, or
    # starting with a digit) raise rather than being silently mangled.
    #
    # Colons in particular are the turbo-rails stream-name separator, so they are mapped to a safe character rather
    # than stripped - otherwise +"a:b"+ and +"ab"+ would collide on the same queue.
    #
    # @param name [String, #to_s] a name using friendly separators
    # @return [String] the normalized, validated queue name
    # @raise [PGMQ::Errors::InvalidQueueNameError] if the normalized result is not a valid queue name
    def normalize(name)
      str = name.to_s
      return validate!(str) if str.match?(PATTERN)

      normalized = str
        .gsub(/[-.:]/, "_")          # hyphens / dots / colons -> underscores
        .gsub(/[^a-zA-Z0-9_]/, "")   # strip any remaining invalid character
        .squeeze("_")                # collapse consecutive underscores
        .gsub(/\A_+|_+\z/, "")       # trim leading / trailing underscores

      validate!(normalized)
    end

    # Strips every invalid character from untrusted input, then validates the result.
    #
    # This is the SQL-identifier guard: it removes anything outside +[A-Za-z0-9_]+ and passes the remainder through
    # {.validate!}. If nothing valid remains (empty result, leading digit, too long) it raises, so a caller can never
    # accidentally operate on a different queue than intended. Use this for names from untrusted sources (URL params,
    # external systems) where a wrong-but-valid name would be worse than an error.
    #
    # @param name [String, #to_s] untrusted input
    # @return [String] the sanitized, validated queue name
    # @raise [PGMQ::Errors::InvalidQueueNameError] if nothing valid remains after stripping
    def sanitize!(name)
      validate!(name.to_s.gsub(/[^a-zA-Z0-9_]/, ""))
    end

    # Best-effort coercion of arbitrary input into a valid queue name; the lenient sibling of {.sanitize!}.
    #
    # Never raises for content: it lowercases, replaces every illegal character run with an underscore, trims
    # underscores, prefixes {SANITIZE_PREFIX} when the first surviving character is not a valid identifier start (e.g.
    # a digit), truncates to fit {MAX_LENGTH}, and falls back to {SANITIZE_FALLBACK} when nothing usable remains. The
    # return value always satisfies {.valid?}.
    #
    # @note Because it coerces rather than rejects, distinct inputs can map to the *same* name (e.g. +"a/b"+ and
    #   +"a-b"+ both become +"a_b"+; +"!!!"+ and +""+ both become +"queue"+). If your name selects a queue table,
    #   that means two logically different inputs could share one queue. When that matters - especially for untrusted
    #   input - prefer {.sanitize!}, which raises instead of substituting.
    #
    # @param name [String, #to_s] arbitrary input
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
