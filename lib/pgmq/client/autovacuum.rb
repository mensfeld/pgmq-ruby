# frozen_string_literal: true

module PGMQ
  class Client
    # Autovacuum and storage tuning for queue and archive tables.
    #
    # PGMQ tables churn in a way PostgreSQL's defaults are not tuned for. A hot queue inserts, updates, and deletes
    # rows constantly: every read UPDATEs +vt+, +read_ct+, and +last_read_at+, and every read+archive cycle deletes
    # from the queue table and inserts into the archive. Two defaults hurt under that load:
    #
    # - +autovacuum_vacuum_scale_factor+ defaults to 0.2, so autovacuum only runs after dead tuples reach 20% of the
    #   table - by which point a busy queue has bloated its heap and B-tree indexes, slowing every read and lock.
    # - +fillfactor+ defaults to 100, so heap pages fill completely. Because +vt+ is indexed and changes on every
    #   read, those UPDATEs are not HOT-eligible; leaving page headroom reduces page density between vacuum passes.
    #
    # This module applies per-table storage parameters via +ALTER TABLE+ so autovacuum runs far more often (and
    # fillfactor reserves churn headroom) on these specific tables, without touching cluster-wide settings. It is
    # intentionally opt-in: the gem is a thin wrapper and does not mutate table storage parameters unless you ask it
    # to (either by calling {#tune_autovacuum} or by passing +tune_autovacuum: true+ to a queue-creation method).
    #
    # @see https://www.postgresql.org/docs/current/runtime-config-autovacuum.html
    # @see https://planetscale.com/blog/keeping-a-postgres-queue-healthy
    module Autovacuum
      # Default storage parameters for the active queue table. Aggressive: autovacuum and autoanalyze trigger at 1%/5%
      # dead-or-changed tuples, a small +cost_delay+ keeps each vacuum pass quick, and +fillfactor+ reserves 30% of
      # each page for the per-read UPDATE churn (the queue table is the only one that benefits from fillfactor).
      DEFAULT_QUEUE_SETTINGS = {
        autovacuum_vacuum_scale_factor: 0.01,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_cost_delay: 2,
        autovacuum_analyze_scale_factor: 0.05,
        fillfactor: 70
      }.freeze

      # Default storage parameters for the archive table. Archives are append-heavy with periodic purge, so a looser
      # scale factor and a slightly higher cost delay are enough while still beating the 0.2 default. No fillfactor:
      # archive rows are inserted once and never updated, so there is no page-churn headroom to reserve.
      DEFAULT_ARCHIVE_SETTINGS = {
        autovacuum_vacuum_scale_factor: 0.05,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_cost_delay: 5,
        autovacuum_analyze_scale_factor: 0.05
      }.freeze

      # Storage parameters whose values are interpolated as Floats; everything else is interpolated as an Integer.
      FLOAT_SETTINGS = %i[
        autovacuum_vacuum_scale_factor
        autovacuum_analyze_scale_factor
      ].freeze
      private_constant :FLOAT_SETTINGS

      # Tunes autovacuum and storage parameters on a queue's underlying tables.
      #
      # Applies {DEFAULT_QUEUE_SETTINGS} to the queue table (+pgmq.q_<name>+) and, unless +archive: false+,
      # {DEFAULT_ARCHIVE_SETTINGS} to the archive table (+pgmq.a_<name>+). Pass +queue_settings:+ / +archive_settings:+
      # to override or extend the parameters per table; the Hash you pass is merged onto the defaults, so you only
      # name the keys you want to change. Any other PostgreSQL storage default is left untouched. Safe to call
      # repeatedly - +ALTER TABLE ... SET+ is idempotent for a given value.
      #
      # @param queue_name [String] name of the queue
      # @param queue_settings [Hash{Symbol=>Numeric}] storage params for the queue table, merged onto
      #   {DEFAULT_QUEUE_SETTINGS}
      # @param archive [Boolean] also tune the archive table (default: true)
      # @param archive_settings [Hash{Symbol=>Numeric}] storage params for the archive table, merged onto
      #   {DEFAULT_ARCHIVE_SETTINGS}
      # @return [void]
      # @raise [PGMQ::Errors::InvalidQueueNameError] if the queue name is invalid
      # @raise [PGMQ::Errors::ConnectionError] if the database operation fails (e.g. the queue does not exist)
      #
      # @example Apply PGMQ-tuned defaults to an existing queue
      #   client.tune_autovacuum("orders")
      #
      # @example Override a couple of queue params, skip the archive
      #   client.tune_autovacuum("orders", queue_settings: { autovacuum_vacuum_scale_factor: 0.005, fillfactor: 80 },
      #                          archive: false)
      #
      # @note On a partitioned queue or archive, the parameters are set on the parent table. PostgreSQL does not
      #   cascade storage parameters to existing partitions, so pre-existing partitions keep their own settings;
      #   set them per-partition if needed.
      def tune_autovacuum(queue_name, queue_settings: {}, archive: true, archive_settings: {})
        validate_queue_name!(queue_name)

        with_connection do |conn|
          tune_autovacuum_on(
            conn,
            queue_name,
            queue_settings: queue_settings,
            archive: archive,
            archive_settings: archive_settings
          )
        end

        nil
      end

      private

      # Resolves storage settings against the PGMQ-tuned defaults and applies them on an existing connection.
      #
      # Single source of truth for both entry points: {#tune_autovacuum} (which passes its keyword arguments through)
      # and the +tune_autovacuum:+ creation flag (which passes a possibly-empty Hash). Keeping the default resolution
      # and the archive-skip rule here means the two paths cannot drift - +archive+ is a single truthiness test in one
      # place, and per-table overrides are merged onto the defaults in one place.
      #
      # @param conn [PG::Connection] the connection to run the ALTER TABLE statements on
      # @param queue_name [String] name of the queue (already validated)
      # @param queue_settings [Hash] queue-table overrides, merged onto {DEFAULT_QUEUE_SETTINGS}
      # @param archive [Boolean] also tune the archive table
      # @param archive_settings [Hash] archive-table overrides, merged onto {DEFAULT_ARCHIVE_SETTINGS}
      # @return [void]
      def tune_autovacuum_on(conn, queue_name, queue_settings: {}, archive: true, archive_settings: {})
        alter_storage(conn, "q_#{queue_name}", DEFAULT_QUEUE_SETTINGS.merge(symbolize(queue_settings)))

        return unless archive

        alter_storage(conn, "a_#{queue_name}", DEFAULT_ARCHIVE_SETTINGS.merge(symbolize(archive_settings)))
      end

      # Issues a single ALTER TABLE setting the given storage parameters on one pgmq table.
      #
      # PGMQ folds queue names to lower case when it creates the backing tables (`pgmq.create('MyQueue')` produces
      # `pgmq.q_myqueue`), so the table name is lower-cased to match the physical table before it is quoted. Without
      # this, a mixed-case queue name would build `q_MyQueue`, and +quote_ident+ would preserve that case and target a
      # non-existent relation.
      #
      # Identifiers cannot be passed as bind parameters, so the table name is quoted with the connection's
      # +quote_ident+. The queue name has already passed {#validate_queue_name!} (strict identifier rules); the
      # parameter names are validated against the known default keys; and the values are coerced to Float/Integer, so
      # no untrusted text reaches the statement.
      #
      # @param conn [PG::Connection] database connection
      # @param table [String] unqualified table name (e.g. "q_orders")
      # @param settings [Hash{Symbol=>Numeric}] storage parameters to set
      # @return [void]
      def alter_storage(conn, table, settings)
        qualified = "pgmq.#{conn.quote_ident(table.downcase)}"
        clause = settings.map { |key, value| "#{storage_param_name(key)} = #{coerce_setting(key, value)}" }.join(", ")

        conn.exec("ALTER TABLE #{qualified} SET (#{clause})")
      end

      # Validates a storage-parameter name against the known keys before it is interpolated into SQL.
      #
      # Parameter names cannot be bound; allow-listing them against the union of the queue and archive default keys
      # keeps an arbitrary Symbol from being injected as a raw identifier.
      #
      # @param key [Symbol] storage parameter name
      # @return [String] the validated parameter name
      # @raise [ArgumentError] if the key is not a recognised storage parameter
      def storage_param_name(key)
        return key.to_s if known_storage_params.include?(key)

        raise ArgumentError, "Unknown storage parameter #{key.inspect}; allowed: #{known_storage_params.to_a.sort}"
      end

      # @return [Set<Symbol>] the union of queue and archive default keys
      def known_storage_params
        @known_storage_params ||= (DEFAULT_QUEUE_SETTINGS.keys + DEFAULT_ARCHIVE_SETTINGS.keys).to_set
      end

      # Coerces a setting value to its SQL form: Float for scale factors, Integer for everything else. Raising on
      # non-numeric input is the injection guard for values.
      #
      # @param key [Symbol] storage parameter name
      # @param value [Numeric, String] the value to coerce
      # @return [Float, Integer]
      def coerce_setting(key, value)
        FLOAT_SETTINGS.include?(key) ? Float(value) : Integer(value)
      end

      # Symbolizes top-level keys of a settings Hash so callers may pass either Symbol or String keys.
      #
      # @param settings [Hash]
      # @return [Hash{Symbol=>Object}]
      def symbolize(settings)
        settings.to_h.transform_keys(&:to_sym)
      end
    end
  end
end
