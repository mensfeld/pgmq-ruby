# frozen_string_literal: true

module PGMQ
  class Client
    # Message lifecycle operations (pop, delete, archive, visibility timeout)
    #
    # This module handles message state transitions including popping (atomic read+delete),
    # deleting, archiving, and updating visibility timeouts.
    module MessageLifecycle
      # Pops a message (atomic read + delete)
      #
      # @param queue_name [String] name of the queue
      # @return [PGMQ::Message, nil] message object or nil if queue is empty
      #
      # @example
      #   msg = client.pop("orders")
      #   process(msg.payload) if msg
      def pop(queue_name)
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params('SELECT * FROM pgmq.pop($1::text)', [queue_name])
        end

        return nil if result.ntuples.zero?

        Message.new(result[0])
      end

      # Pops multiple messages atomically (atomic read + delete for batch)
      #
      # @param queue_name [String] name of the queue
      # @param qty [Integer] maximum number of messages to pop
      # @return [Array<PGMQ::Message>] array of message objects (empty if queue is empty)
      #
      # @example Pop up to 10 messages
      #   messages = client.pop_batch("orders", 10)
      #   messages.each { |msg| process(msg.payload) }
      def pop_batch(queue_name, qty)
        validate_queue_name!(queue_name)
        return [] if qty <= 0

        result = with_connection do |conn|
          conn.exec_params('SELECT * FROM pgmq.pop($1::text, $2::integer)', [queue_name, qty])
        end

        result.map { |row| Message.new(row) }
      end

      # Deletes a message from the queue
      #
      # @param queue_name [String] name of the queue
      # @param msg_id [Integer] message ID to delete
      # @return [Boolean] true if message was deleted
      #
      # @example
      #   client.delete("orders", 123)
      def delete(
        queue_name,
        msg_id
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            'SELECT pgmq.delete($1::text, $2::bigint)',
            [queue_name, msg_id]
          )
        end

        return false if result.ntuples.zero?

        result[0]['delete'] == 't'
      end

      # Deletes multiple messages from the queue
      #
      # @param queue_name [String] name of the queue
      # @param msg_ids [Array<Integer>] array of message IDs to delete
      # @return [Array<Integer>] array of successfully deleted message IDs
      #
      # @example
      #   deleted = client.delete_batch("orders", [101, 102, 103])
      def delete_batch(
        queue_name,
        msg_ids
      )
        validate_queue_name!(queue_name)
        return [] if msg_ids.empty?

        # Use PostgreSQL array parameter binding
        result = with_connection do |conn|
          encoder = PG::TextEncoder::Array.new
          encoded_array = encoder.encode(msg_ids)

          conn.exec_params(
            'SELECT * FROM pgmq.delete($1::text, $2::bigint[])',
            [queue_name, encoded_array]
          )
        end

        result.map { |row| row['delete'] }
      end

      # Deletes specific messages from multiple queues in a single transaction
      #
      # Efficiently deletes messages across different queues atomically.
      # Useful when processing related messages from different queues.
      #
      # @param deletions [Hash] hash of queue_name => array of msg_ids
      # @return [Hash] hash of queue_name => array of deleted msg_ids
      #
      # @example Delete messages from multiple queues
      #   client.delete_multi({
      #     'orders' => [1, 2, 3],
      #     'notifications' => [5, 6],
      #     'emails' => [10]
      #   })
      #   # => { 'orders' => [1, 2, 3], 'notifications' => [5, 6], 'emails' => [10] }
      #
      # @example Clean up after batch processing across queues
      #   messages = client.read_multi(['q1', 'q2', 'q3'], qty: 10)
      #   deletions = messages.group_by(&:queue_name).transform_values { |mss| mss.map(&:msg_id) }
      #   client.delete_multi(deletions)
      def delete_multi(deletions)
        raise ArgumentError, 'deletions must be a hash' unless deletions.is_a?(Hash)
        return {} if deletions.empty?

        # Validate all queue names
        deletions.each_key { |qn| validate_queue_name!(qn) }

        transaction do |txn|
          result = {}
          deletions.each do |queue_name, msg_ids|
            next if msg_ids.empty?

            deleted_ids = txn.delete_batch(queue_name, msg_ids)
            result[queue_name] = deleted_ids
          end
          result
        end
      end

      # Archives a message
      #
      # @param queue_name [String] name of the queue
      # @param msg_id [Integer] message ID to archive
      # @return [Boolean] true if message was archived
      #
      # @example
      #   client.archive("orders", 123)
      def archive(
        queue_name,
        msg_id
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            'SELECT pgmq.archive($1::text, $2::bigint)',
            [queue_name, msg_id]
          )
        end

        return false if result.ntuples.zero?

        result[0]['archive'] == 't'
      end

      # Archives multiple messages
      #
      # @param queue_name [String] name of the queue
      # @param msg_ids [Array<Integer>] array of message IDs to archive
      # @return [Array<Integer>] array of successfully archived message IDs
      #
      # @example
      #   archived = client.archive_batch("orders", [101, 102, 103])
      def archive_batch(
        queue_name,
        msg_ids
      )
        validate_queue_name!(queue_name)
        return [] if msg_ids.empty?

        # Use PostgreSQL array parameter binding
        result = with_connection do |conn|
          encoder = PG::TextEncoder::Array.new
          encoded_array = encoder.encode(msg_ids)

          conn.exec_params(
            'SELECT * FROM pgmq.archive($1::text, $2::bigint[])',
            [queue_name, encoded_array]
          )
        end

        result.map { |row| row['archive'] }
      end

      # Archives specific messages from multiple queues in a single transaction
      #
      # Efficiently archives messages across different queues atomically.
      #
      # @param archives [Hash] hash of queue_name => array of msg_ids
      # @return [Hash] hash of queue_name => array of archived msg_ids
      #
      # @example Archive messages from multiple queues
      #   client.archive_multi({
      #     'orders' => [1, 2],
      #     'notifications' => [5]
      #   })
      def archive_multi(archives)
        raise ArgumentError, 'archives must be a hash' unless archives.is_a?(Hash)
        return {} if archives.empty?

        # Validate all queue names
        archives.each_key { |qn| validate_queue_name!(qn) }

        transaction do |txn|
          result = {}
          archives.each do |queue_name, msg_ids|
            next if msg_ids.empty?

            archived_ids = txn.archive_batch(queue_name, msg_ids)
            result[queue_name] = archived_ids
          end
          result
        end
      end

      # Updates the visibility timeout for a message
      #
      # @param queue_name [String] name of the queue
      # @param msg_id [Integer] message ID
      # @param vt_offset [Integer] visibility timeout offset in seconds
      # @return [PGMQ::Message, nil] updated message or nil if not found
      #
      # @example
      #   # Extend processing time by 60 more seconds
      #   msg = client.set_vt("orders", 123, vt_offset: 60)
      def set_vt(
        queue_name,
        msg_id,
        vt_offset:
      )
        validate_queue_name!(queue_name)

        result = with_connection do |conn|
          conn.exec_params(
            'SELECT * FROM pgmq.set_vt($1::text, $2::bigint, $3::integer)',
            [queue_name, msg_id, vt_offset]
          )
        end

        return nil if result.ntuples.zero?

        Message.new(result[0])
      end

      # Updates visibility timeout for multiple messages
      #
      # @param queue_name [String] name of the queue
      # @param msg_ids [Array<Integer>] array of message IDs
      # @param vt_offset [Integer] visibility timeout offset in seconds
      # @return [Array<PGMQ::Message>] array of updated messages
      #
      # @example
      #   # Extend processing time for multiple messages
      #   messages = client.set_vt_batch("orders", [101, 102, 103], vt_offset: 60)
      def set_vt_batch(
        queue_name,
        msg_ids,
        vt_offset:
      )
        validate_queue_name!(queue_name)
        return [] if msg_ids.empty?

        result = with_connection do |conn|
          encoder = PG::TextEncoder::Array.new
          encoded_array = encoder.encode(msg_ids)

          conn.exec_params(
            'SELECT * FROM pgmq.set_vt($1::text, $2::bigint[], $3::integer)',
            [queue_name, encoded_array, vt_offset]
          )
        end

        result.map { |row| Message.new(row) }
      end

      # Updates visibility timeout for messages across multiple queues in a single transaction
      #
      # Efficiently updates visibility timeouts across different queues atomically.
      # Useful when processing related messages from different queues and needing
      # to extend their visibility timeouts together.
      #
      # @param updates [Hash] hash of queue_name => array of msg_ids
      # @param vt_offset [Integer] visibility timeout offset in seconds (applied to all)
      # @return [Hash] hash of queue_name => array of updated PGMQ::Message objects
      #
      # @example Extend visibility timeout for messages from multiple queues
      #   client.set_vt_multi({
      #     'orders' => [1, 2, 3],
      #     'notifications' => [5, 6],
      #     'emails' => [10]
      #   }, vt_offset: 60)
      #   # => { 'orders' => [<Message>, ...], 'notifications' => [...], 'emails' => [...] }
      #
      # @example Extend timeout after batch reading from multiple queues
      #   messages = client.read_multi(['q1', 'q2', 'q3'], qty: 10)
      #   updates = messages.group_by(&:queue_name).transform_values { |msgs| msgs.map(&:msg_id) }
      #   client.set_vt_multi(updates, vt_offset: 120)
      def set_vt_multi(updates, vt_offset:)
        raise ArgumentError, 'updates must be a hash' unless updates.is_a?(Hash)
        return {} if updates.empty?

        # Validate all queue names
        updates.each_key { |qn| validate_queue_name!(qn) }

        transaction do |txn|
          result = {}
          updates.each do |queue_name, msg_ids|
            next if msg_ids.empty?

            updated_messages = txn.set_vt_batch(queue_name, msg_ids, vt_offset: vt_offset)
            result[queue_name] = updated_messages
          end
          result
        end
      end
    end
  end
end
