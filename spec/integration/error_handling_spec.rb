# frozen_string_literal: true

# Example: Error Handling
#
# Demonstrates error handling patterns:
# - PGMQ error hierarchy
# - Dead letter queue (DLQ) pattern
# - Retry with max attempts
#
# Run: bundle exec ruby spec/integration/error_handling_spec.rb

require_relative 'support/example_helper'

ExampleHelper.run_example('Error Handling') do |client, queues, interrupted|
  main = ExampleHelper.unique_queue_name('main')
  dlq = ExampleHelper.unique_queue_name('dlq')
  queues << main << dlq

  client.create(main)
  client.create(dlq)

  # Invalid queue name errors
  begin
    client.create('123-invalid')
  rescue PGMQ::Errors::InvalidQueueNameError
    puts 'InvalidQueueNameError: caught for bad queue name'
  end

  # Non-existent queue errors
  begin
    client.read('nonexistent_queue_xyz', vt: 30)
  rescue PGMQ::Errors::ConnectionError
    puts 'ConnectionError: caught for missing queue'
  end

  break if interrupted.call

  # DLQ pattern: retry failed messages, move to DLQ after max retries
  3.times { |i| client.produce(main, ExampleHelper.to_json({ task: i + 1, fail: i.even? })) }
  puts 'Produced 3 messages (some will fail)'

  max_retries = 2
  processed = 0
  to_dlq = 0

  loop do
    break if interrupted.call

    msg = client.read(main, vt: 5)
    break unless msg

    data = ExampleHelper.parse_message(msg)
    retries = data['retries'] || 0

    begin
      raise 'Simulated failure' if data['fail']

      processed += 1
    rescue StandardError
      if retries < max_retries
        data['retries'] = retries + 1
        client.produce(main, ExampleHelper.to_json(data))
      else
        client.produce(dlq, ExampleHelper.to_json(data.merge(failed: true)))
        to_dlq += 1
      end
    end
    client.delete(main, msg.msg_id)
  end

  puts "Processed: #{processed}, Moved to DLQ: #{to_dlq}"

  # Clean up
  loop { break unless client.pop(dlq) }
end
