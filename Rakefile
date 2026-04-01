# frozen_string_literal: true

require "bundler/setup"
require "bundler/gem_tasks"

require "minitest/test_task"

Minitest::TestTask.create(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_globs = ["test/**/*_test.rb"]
  t.test_prelude = 'require "test_helper"; require "minitest/autorun"'
end

task default: :test

namespace :examples do
  desc "Run all examples (validates gem functionality)"
  task :run do
    exec("bin/integrations")
  end

  desc "Run a specific example by name (e.g., rake examples:run_one[basic_produce_consume])"
  task :run_one, [:name] do |_t, args|
    examples_dir = File.expand_path("spec/integration", __dir__)
    pattern = File.join(examples_dir, "*#{args[:name]}*_spec.rb")
    matches = Dir.glob(pattern)

    if matches.empty?
      puts "No example found matching: #{args[:name]}"
      exit(1)
    end

    exec("bin/integrations", matches.first)
  end

  desc "List all available examples"
  task :list do
    examples_dir = File.expand_path("spec/integration", __dir__)
    example_files = Dir.glob(File.join(examples_dir, "*_spec.rb")).sort

    puts "Available examples:"
    example_files.each do |f|
      name = File.basename(f, "_spec.rb")
      puts "  #{name}"
    end
    puts
    puts "Run with: bin/integrations spec/integration/NAME_spec.rb"
    puts "Example:  bin/integrations spec/integration/basic_produce_consume_spec.rb"
  end
end

# Shorthand task
desc "Run all examples"
task examples: "examples:run"
