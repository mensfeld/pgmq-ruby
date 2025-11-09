# frozen_string_literal: true

source 'https://rubygems.org'

gemspec

group :development, :test do
  gem 'rake'
  gem 'rspec'
end

group :test do
  gem 'database_cleaner-sequel'
  # MessagePack serializer testing (optional)
  gem 'msgpack', require: false
  gem 'simplecov', require: false
end
