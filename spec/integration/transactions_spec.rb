# frozen_string_literal: true

# Example: Transactions
#
# Demonstrates atomic operations across queues:
# - All operations in a transaction succeed or fail together
# - Automatic rollback on errors
# - Essential for read-process-forward patterns
#
# Run: bundle exec ruby spec/integration/transactions_spec.rb

require_relative "support/example_helper"

ExampleHelper.run_example("Transactions") do |client, queues, interrupted|
  inbox = ExampleHelper.unique_queue_name("inbox")
  processed = ExampleHelper.unique_queue_name("processed")
  queues << inbox << processed

  client.create(inbox)
  client.create(processed)

  3.times { |i| client.produce(inbox, ExampleHelper.to_json({ job: i + 1 })) }
  puts "Produced 3 jobs to inbox"

  break if interrupted.call

  # Successful transaction: read -> forward -> delete (atomic)
  client.transaction do |txn|
    msg = txn.read(inbox, vt: 30)
    if msg
      data = ExampleHelper.parse_message(msg)
      txn.produce(processed, ExampleHelper.to_json({ job: data["job"], status: "done" }))
      txn.delete(inbox, msg.msg_id)
      puts "Transaction committed: job #{data["job"]} moved to processed"
    end
  end

  break if interrupted.call

  # Failed transaction: automatic rollback
  begin
    client.transaction do |txn|
      msg = txn.read(inbox, vt: 30)
      txn.produce(processed, ExampleHelper.to_json({ status: "processing" })) if msg
      raise StandardError, "Simulated error!"
    end
  rescue => e
    puts "Transaction rolled back: #{e.message}"
  end

  # Verify rollback (message still in inbox)
  msg = client.read(inbox, vt: 1)
  puts "Message still in inbox after rollback: #{!msg.nil?}"
  # Make message immediately visible again (negative offset moves VT back in time)
  client.set_vt(inbox, msg.msg_id, vt_offset: -30) if msg

  # Clean up
  loop { break unless client.pop(inbox) }
  loop { break unless client.pop(processed) }
end
