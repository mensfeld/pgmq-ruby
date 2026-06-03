# frozen_string_literal: true

require 'warning'

$VERBOSE = true

if Warning.respond_to?(:categories)
  (Warning.categories - %i[experimental]).each do |cat|
    Warning[cat] = true
  end
else
  Warning[:deprecated] = true
  Warning[:performance] = true if RUBY_VERSION >= '3.3'
end

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  next if warning.include?('_test')
  next if warning.include?('previous definition of')
  next if warning.include?('method redefined')
  next if warning.include?('vendor/')
  next if warning.include?('bundle/')
  next if warning.include?('.bundle/')
  raise "Warning in your code: #{warning}"
end

# SimpleCov must be loaded before application code
require "simplecov"

SimpleCov.start do
  add_filter "/test/"
  add_filter "/spec/"
  add_filter "/examples/"
  add_filter "/vendor/"

  minimum_coverage 96.5
end

require "minitest/autorun"
require "minitest/spec"
require "mocha/minitest"

require "pgmq"
require "json" # Tests need JSON for serialization (user responsibility)

# Database connection parameters for testing
# Uses port 5433 by default to avoid conflicts with existing PostgreSQL installations
TEST_DB_PARAMS = {
  host: ENV.fetch("PG_HOST", "localhost"),
  port: ENV.fetch("PG_PORT", 5433).to_i,
  dbname: ENV.fetch("PG_DATABASE", "pgmq_test"),
  user: ENV.fetch("PG_USER", "postgres"),
  password: ENV.fetch("PG_PASSWORD", "postgres")
}.freeze

# Helper to convert Ruby objects to JSON strings (user responsibility in real apps)
module JSONHelpers
  def to_json_msg(obj)
    obj.is_a?(String) ? obj : JSON.generate(obj)
  end
end

# Alias context to describe for nested grouping
class Minitest::Spec
  class << self
    alias_method :context, :describe
  end
end

# Support files
Dir[File.join(__dir__, "support", "**", "*.rb")].each { |f| require f }

# Include helpers in all specs
class Minitest::Spec
  include JSONHelpers
  include DatabaseHelpers
end
