# frozen_string_literal: true

source 'https://rubygems.org'

gemspec

# Platform-specific PostgreSQL adapters
platforms :jruby do
  gem 'jruby-pg', '~> 0.1'
end

group :development, :test do
  gem 'rake', '~> 13.0'
  gem 'rspec', '~> 3.12'
end

group :test do
  # Rails integration testing (optional)
  gem 'activejob', '~> 7.1', require: false
  gem 'database_cleaner-sequel', '~> 2.0'
  # MessagePack serializer testing (optional)
  gem 'msgpack', '~> 1.7', require: false
  gem 'rails', '~> 7.1', require: false
  gem 'simplecov', '~> 0.22', require: false
end

group :development do
  gem 'pry', '~> 0.14'
  gem 'pry-byebug', '~> 3.10'
end
