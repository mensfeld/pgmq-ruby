# frozen_string_literal: true

require_relative "lib/pgmq/version"

Gem::Specification.new do |spec|
  spec.name = "pgmq-ruby"
  spec.version = PGMQ::VERSION
  spec.authors = ["Maciej Mensfeld"]
  spec.email = ["maciej@mensfeld.pl"]

  spec.summary = "Ruby client for PGMQ (Postgres Message Queue)"
  spec.description = "A Ruby driver for PGMQ - a lightweight message queue built on PostgreSQL. " \
                     "Like AWS SQS and RSMQ, but on Postgres."
  spec.homepage = "https://github.com/mensfeld/pgmq-ruby"
  spec.license = "LGPL-3.0"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/mensfeld/pgmq-ruby"
  spec.metadata["changelog_uri"] = "https://github.com/mensfeld/pgmq-ruby/blob/master/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "https://github.com/mensfeld/pgmq-ruby/issues"
  spec.metadata["documentation_uri"] = "https://github.com/mensfeld/pgmq-ruby#readme"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(spec|examples)/}) }
  spec.require_paths = ["lib"]

  # Runtime dependencies
  spec.add_dependency "connection_pool", "~> 2.4"
  spec.add_dependency "pg", "~> 1.5"
  spec.add_dependency "zeitwerk", "~> 2.6"
end
