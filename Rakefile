# frozen_string_literal: true

require 'bundler/setup'
require 'bundler/gem_tasks'

namespace :examples do
  desc 'Run all examples (validates gem functionality)'
  task :run do
    examples_dir = File.expand_path('spec/integration', __dir__)
    example_files = Dir.glob(File.join(examples_dir, '*_spec.rb')).sort

    puts "Running #{example_files.size} examples..."
    puts

    failed = []
    example_files.each_with_index do |example, index|
      name = File.basename(example)
      puts "[#{index + 1}/#{example_files.size}] Running #{name}..."

      success = system("bundle exec ruby #{example}")
      if success.nil?
        puts 'Interrupted. Aborting.'
        exit(130)
      elsif !success
        failed << name
        puts "FAILED: #{name}"
      end
      puts
    end

    puts '=' * 60
    if failed.empty?
      puts "All #{example_files.size} examples passed."
    else
      puts "#{failed.size} example(s) failed:"
      failed.each { |f| puts "  - #{f}" }
      exit(1)
    end
  end

  desc 'Run a specific example by name (e.g., rake examples:run_one[basic_produce_consume])'
  task :run_one, [:name] do |_t, args|
    examples_dir = File.expand_path('spec/integration', __dir__)
    pattern = File.join(examples_dir, "*#{args[:name]}*_spec.rb")
    matches = Dir.glob(pattern)

    if matches.empty?
      puts "No example found matching: #{args[:name]}"
      exit(1)
    end

    exec("bundle exec ruby #{matches.first}")
  end

  desc 'List all available examples'
  task :list do
    examples_dir = File.expand_path('spec/integration', __dir__)
    example_files = Dir.glob(File.join(examples_dir, '*_spec.rb')).sort

    puts 'Available examples:'
    example_files.each do |f|
      name = File.basename(f, '_spec.rb')
      puts "  #{name}"
    end
    puts
    puts 'Run with: bundle exec rake examples:run_one[NAME]'
    puts 'Example:  bundle exec rake examples:run_one[basic_produce_consume]'
  end
end

# Shorthand task
desc 'Run all examples'
task examples: 'examples:run'
