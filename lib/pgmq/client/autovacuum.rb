# frozen_string_literal: true

module PGMQ
  class Client
    # Autovacuum tuning for queue and archive tables.
    #
    # PGMQ tables churn in a way PostgreSQL's defaults are not tuned for: a hot queue inserts, updates (visibility
    # timeout), and deletes rows constantly, so dead tuples accumulate fast. With the default
    # +autovacuum_vacuum_scale_factor+ of 0.2, autovacuum only kicks in after dead tuples reach 20% of the table -
    # by which point a busy queue has bloated its heap and indexes, slowing every read. The archive table grows
    # monotonically (append + occasional prune), so it benefits from a gentler but still-tightened setting.
    #
    # This module applies per-table storage parameters via +ALTER TABLE+ so autovacuum runs far more often on these
    # specific tables without touching cluster-wide settings. It is intentionally opt-in: the gem is a thin wrapper
    # and does not mutate table storage parameters unless you ask it to (either by calling {#tune_autovacuum} or by
    # passing +tune_autovacuum: true+ to a queue-creation method).
    #
    # @see https://www.postgresql.org/docs/current/runtime-config-autovacuum.html
    module Autovacuum
      # Aggressive scale factor for the active queue table. Triggers autovacuum once dead tuples reach 1% of the
      # table (plus the threshold), keeping a hot queue's heap and indexes from bloating under read+delete churn.
      DEFAULT_QUEUE_SCALE_FACTOR = 0.01

      # Flat dead-tuple floor for the queue table, added to the scale-factor term. Keeps small queues from waiting on
      # the percentage alone.
      DEFAULT_QUEUE_THRESHOLD = 50

      # Scale factor for the archive table. Archives grow mostly by append, so a looser factor than the queue is
      # enough while still beating the 0.2 default.
      DEFAULT_ARCHIVE_SCALE_FACTOR = 0.05

      # Flat dead-tuple floor for the archive table.
      DEFAULT_ARCHIVE_THRESHOLD = 50

      # Tunes autovacuum storage parameters on a queue's underlying tables.
      #
      # Sets +autovacuum_vacuum_scale_factor+ and +autovacuum_vacuum_threshold+ on the queue table
      # (+pgmq.q_<name>+) and, unless +archive: false+, on the archive table (+pgmq.a_<name>+). Existing PostgreSQL
      # defaults for any other parameter are left untouched. Safe to call repeatedly - +ALTER TABLE ... SET+ is
      # idempotent for a given value.
      #
      # @param queue_name [String] name of the queue
      # @param scale_factor [Float] queue table +autovacuum_vacuum_scale_factor+
      # @param threshold [Integer] queue table +autovacuum_vacuum_threshold+
      # @param archive [Boolean] also tune the archive table (default: true)
      # @param archive_scale_factor [Float] archive table +autovacuum_vacuum_scale_factor+
      # @param archive_threshold [Integer] archive table +autovacuum_vacuum_threshold+
      # @return [void]
      # @raise [PGMQ::Errors::InvalidQueueNameError] if the queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if the database operation fails (e.g. the queue does not exist)
      #
      # @example Apply PGMQ-tuned defaults to an existing queue
      #   client.tune_autovacuum("orders")
      #
      # @example Override the queue scale factor, skip the archive
      #   client.tune_autovacuum("orders", scale_factor: 0.005, archive: false)
      #
      # @note On a partitioned queue or archive, the parameters are set on the parent table. PostgreSQL does not
      #   cascade storage parameters to existing partitions, so pre-existing partitions keep their own settings;
      #   set them per-partition if needed.
      def tune_autovacuum(
        queue_name,
        scale_factor: DEFAULT_QUEUE_SCALE_FACTOR,
        threshold: DEFAULT_QUEUE_THRESHOLD,
        archive: true,
        archive_scale_factor: DEFAULT_ARCHIVE_SCALE_FACTOR,
        archive_threshold: DEFAULT_ARCHIVE_THRESHOLD
      )
        validate_queue_name!(queue_name)

        with_connection do |conn|
          tune_autovacuum_on(
            conn,
            queue_name,
            scale_factor: scale_factor,
            threshold: threshold,
            archive: archive,
            archive_scale_factor: archive_scale_factor,
            archive_threshold: archive_threshold
          )
        end

        nil
      end

      private

      # Resolves autovacuum options against the PGMQ-tuned defaults and applies them on an existing connection.
      #
      # Single source of truth for both entry points: {#tune_autovacuum} (which passes its keyword arguments through)
      # and the +tune_autovacuum:+ creation flag (which passes a possibly-empty Hash). Keeping the default resolution
      # and the archive-skip rule here means the two paths cannot drift - in particular, +archive+ is treated as a
      # single truthiness test in one place rather than being checked differently per caller.
      #
      # @param conn [PG::Connection] the connection to run the ALTER TABLE statements on
      # @param queue_name [String] name of the queue (already validated)
      # @param opts [Hash] resolved or partial options; missing keys fall back to the DEFAULT_* constants
      # @option opts [Float] :scale_factor queue table +autovacuum_vacuum_scale_factor+
      # @option opts [Integer] :threshold queue table +autovacuum_vacuum_threshold+
      # @option opts [Boolean] :archive also tune the archive table (default: true)
      # @option opts [Float] :archive_scale_factor archive table +autovacuum_vacuum_scale_factor+
      # @option opts [Integer] :archive_threshold archive table +autovacuum_vacuum_threshold+
      # @return [void]
      def tune_autovacuum_on(conn, queue_name, opts = {})
        alter_autovacuum(
          conn,
          "q_#{queue_name}",
          opts.fetch(:scale_factor, DEFAULT_QUEUE_SCALE_FACTOR),
          opts.fetch(:threshold, DEFAULT_QUEUE_THRESHOLD)
        )

        return unless opts.fetch(:archive, true)

        alter_autovacuum(
          conn,
          "a_#{queue_name}",
          opts.fetch(:archive_scale_factor, DEFAULT_ARCHIVE_SCALE_FACTOR),
          opts.fetch(:archive_threshold, DEFAULT_ARCHIVE_THRESHOLD)
        )
      end

      # Issues a single ALTER TABLE setting the two autovacuum storage parameters on one pgmq table.
      #
      # PGMQ folds queue names to lower case when it creates the backing tables (`pgmq.create('MyQueue')` produces
      # `pgmq.q_myqueue`), so the table name is lower-cased to match the physical table before it is quoted. Without
      # this, a mixed-case queue name would build `q_MyQueue`, and +quote_ident+ would preserve that case and target
      # a non-existent relation.
      #
      # Identifiers cannot be passed as bind parameters, so the table name is quoted with the connection's
      # +quote_ident+. The queue name has already passed {#validate_queue_name!} (strict identifier rules), and the
      # numeric values are coerced to Float/Integer, so no untrusted text reaches the statement.
      #
      # @param conn [PG::Connection] database connection
      # @param table [String] unqualified table name (e.g. "q_orders")
      # @param scale_factor [Float] autovacuum_vacuum_scale_factor
      # @param threshold [Integer] autovacuum_vacuum_threshold
      # @return [void]
      def alter_autovacuum(conn, table, scale_factor, threshold)
        qualified = "pgmq.#{conn.quote_ident(table.downcase)}"

        conn.exec(
          "ALTER TABLE #{qualified} SET (" \
          "autovacuum_vacuum_scale_factor = #{Float(scale_factor)}, " \
          "autovacuum_vacuum_threshold = #{Integer(threshold)})"
        )
      end
    end
  end
end
