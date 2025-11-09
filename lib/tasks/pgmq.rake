# frozen_string_literal: true

namespace :pgmq do
  desc 'Create a PGMQ queue'
  task :create_queue, [:queue_name] => :environment do |_t, args|
    queue_name = args[:queue_name]

    if queue_name.nil? || queue_name.empty?
      puts 'Error: Queue name is required'
      puts 'Usage: rake pgmq:create_queue[queue_name]'
      exit 1
    end

    client = PGMQ::Client.new
    client.create(queue_name)
    puts "✓ Created queue: #{queue_name}"
  rescue PGMQ::Error => e
    puts "Error creating queue: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'Drop a PGMQ queue'
  task :drop_queue, [:queue_name] => :environment do |_t, args|
    queue_name = args[:queue_name]

    if queue_name.nil? || queue_name.empty?
      puts 'Error: Queue name is required'
      puts 'Usage: rake pgmq:drop_queue[queue_name]'
      exit 1
    end

    client = PGMQ::Client.new
    if client.drop_queue(queue_name)
      puts "✓ Dropped queue: #{queue_name}"
    else
      puts "Queue '#{queue_name}' does not exist"
    end
  rescue PGMQ::Error => e
    puts "Error dropping queue: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'List all PGMQ queues'
  task list_queues: :environment do
    client = PGMQ::Client.new
    queues = client.list_queues

    if queues.empty?
      puts 'No queues found'
    else
      puts "\nPGMQ Queues:"
      puts '=' * 80
      puts 'Name                           Created At           Partitioned  Unlogged    '
      puts '-' * 80

      queues.each do |queue|
        puts format(
          '%-30s %-20s %-12s %-12s',
          queue.queue_name,
          queue.created_at.strftime('%Y-%m-%d %H:%M:%S'),
          queue.partitioned? ? 'Yes' : 'No',
          queue.unlogged? ? 'Yes' : 'No'
        )
      end

      puts '-' * 80
      puts "Total: #{queues.size} queue(s)"
    end
  rescue PGMQ::Error => e
    puts "Error listing queues: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'Show metrics for a specific queue'
  task :metrics, [:queue_name] => :environment do |_t, args|
    queue_name = args[:queue_name]

    if queue_name.nil? || queue_name.empty?
      puts 'Error: Queue name is required'
      puts 'Usage: rake pgmq:metrics[queue_name]'
      exit 1
    end

    client = PGMQ::Client.new
    metrics = client.metrics(queue_name)

    if metrics.nil?
      puts "Queue '#{queue_name}' does not exist"
      exit 1
    end

    puts "\nMetrics for queue: #{queue_name}"
    puts '=' * 60
    puts "Queue Length:        #{metrics.queue_length}"
    puts "Total Messages:      #{metrics.total_messages}"
    puts "Oldest Message Age:  #{metrics.oldest_msg_age_sec || 'N/A'} seconds"
    puts "Newest Message Age:  #{metrics.newest_msg_age_sec || 'N/A'} seconds"
    puts "Scraped At:          #{metrics.scrape_time}"
    puts '=' * 60
  rescue PGMQ::Error => e
    puts "Error getting metrics: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'Show metrics for all queues'
  task metrics_all: :environment do
    client = PGMQ::Client.new
    all_metrics = client.metrics_all

    if all_metrics.empty?
      puts 'No queues found'
    else
      puts "\nMetrics for all queues:"
      puts '=' * 100
      puts 'Queue                           Length      Total Msgs    Oldest (sec)    Newest (sec)'
      puts '-' * 100

      all_metrics.each do |m|
        puts format(
          '%-25s %12d %15d %15s %15s',
          m.queue_name,
          m.queue_length,
          m.total_messages,
          m.oldest_msg_age_sec || 'N/A',
          m.newest_msg_age_sec || 'N/A'
        )
      end

      puts '-' * 100
      puts "Total: #{all_metrics.size} queue(s)"
    end
  rescue PGMQ::Error => e
    puts "Error getting metrics: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'Purge all messages from a queue'
  task :purge_queue, [:queue_name] => :environment do |_t, args|
    queue_name = args[:queue_name]

    if queue_name.nil? || queue_name.empty?
      puts 'Error: Queue name is required'
      puts 'Usage: rake pgmq:purge_queue[queue_name]'
      exit 1
    end

    print "Are you sure you want to purge all messages from '#{queue_name}'? (y/N): "
    confirmation = $stdin.gets.chomp.downcase

    unless confirmation == 'y'
      puts 'Cancelled'
      exit 0
    end

    client = PGMQ::Client.new
    count = client.purge_queue(queue_name)
    puts "✓ Purged #{count} message(s) from queue: #{queue_name}"
  rescue PGMQ::Error => e
    puts "Error purging queue: #{e.message}"
    exit 1
  ensure
    client&.close
  end

  desc 'Start PGMQ job worker'
  task :work, [:queues] => :environment do |_t, args|
    queues = args[:queues]&.split(',') || ['default']

    puts 'Starting PGMQ worker...'
    puts "Processing queues: #{queues.join(', ')}"
    puts 'Press Ctrl+C to stop'
    puts

    processor = PGMQ::JobProcessor.new(queues)
    processor.start
  end
end
