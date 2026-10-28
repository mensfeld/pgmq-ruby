# frozen_string_literal: true

# A number of seconds that claims to be its value the way ActiveSupport::Duration (+5.minutes+) does: it
# overrides +is_a?+ / +kind_of?+ and forwards the rest to the value, but its class is not a Numeric, so
# +Integer === it+ and +Numeric === it+ are false. Mirrors activesupport's duration.rb without the dependency.
class DurationLike
  attr_reader :value

  def initialize(value)
    @value = value
  end

  def is_a?(klass)
    klass == DurationLike || value.is_a?(klass)
  end
  alias_method :kind_of?, :is_a?

  def instance_of?(klass)
    klass == DurationLike || value.instance_of?(klass)
  end

  def to_s
    @value.to_s
  end

  def to_i
    @value.to_i
  end

  private

  def respond_to_missing?(method, _)
    value.respond_to?(method)
  end

  def method_missing(...)
    value.public_send(...)
  end
end
