# frozen_string_literal: true

describe PGMQ::Client::Autovacuum do
  before do
    @client = create_test_client
    @queue_name = unique_queue_name
  end

  after { teardown_client_and_queue }

  # Reads reloptions for a pgmq table (e.g. "q_<queue>" / "a_<queue>") as a Hash of String => String.
  def reloptions(table)
    result = @client.connection.with_connection do |conn|
      conn.exec_params(
        "SELECT unnest(reloptions) AS opt FROM pg_class WHERE relname = $1",
        [table]
      )
    end

    result.each_with_object({}) do |row, hash|
      key, value = row["opt"].split("=", 2)
      hash[key] = value
    end
  end

  describe "#tune_autovacuum" do
    before { @client.create(@queue_name) }

    it "applies all PGMQ-tuned defaults to the queue table" do
      @client.tune_autovacuum(@queue_name)

      opts = reloptions("q_#{@queue_name}")

      assert_in_delta 0.01, opts["autovacuum_vacuum_scale_factor"].to_f
      assert_equal "50", opts["autovacuum_vacuum_threshold"]
      assert_equal "2", opts["autovacuum_vacuum_cost_delay"]
      assert_in_delta 0.05, opts["autovacuum_analyze_scale_factor"].to_f
      assert_equal "70", opts["fillfactor"]
    end

    it "applies all PGMQ-tuned defaults to the archive table (no fillfactor)" do
      @client.tune_autovacuum(@queue_name)

      opts = reloptions("a_#{@queue_name}")

      assert_in_delta 0.05, opts["autovacuum_vacuum_scale_factor"].to_f
      assert_equal "50", opts["autovacuum_vacuum_threshold"]
      assert_equal "5", opts["autovacuum_vacuum_cost_delay"]
      assert_in_delta 0.05, opts["autovacuum_analyze_scale_factor"].to_f
      refute opts.key?("fillfactor"), "archive table should not get a fillfactor"
    end

    it "merges queue_settings overrides onto the defaults" do
      @client.tune_autovacuum(@queue_name, queue_settings: { autovacuum_vacuum_scale_factor: 0.005, fillfactor: 80 })

      opts = reloptions("q_#{@queue_name}")

      # Overridden keys take the new value...
      assert_in_delta 0.005, opts["autovacuum_vacuum_scale_factor"].to_f
      assert_equal "80", opts["fillfactor"]
      # ...while un-named keys keep the defaults.
      assert_equal "50", opts["autovacuum_vacuum_threshold"]
      assert_equal "2", opts["autovacuum_vacuum_cost_delay"]
    end

    it "merges archive_settings overrides onto the defaults" do
      @client.tune_autovacuum(@queue_name, archive_settings: { autovacuum_vacuum_scale_factor: 0.02 })

      opts = reloptions("a_#{@queue_name}")

      assert_in_delta 0.02, opts["autovacuum_vacuum_scale_factor"].to_f
      assert_equal "5", opts["autovacuum_vacuum_cost_delay"]
    end

    it "accepts string keys in settings hashes" do
      @client.tune_autovacuum(@queue_name, queue_settings: { "fillfactor" => 90 })

      assert_equal "90", reloptions("q_#{@queue_name}")["fillfactor"]
    end

    it "leaves the archive table untouched when archive: false" do
      @client.tune_autovacuum(@queue_name, archive: false)

      assert_empty reloptions("a_#{@queue_name}")
    end

    it "is idempotent" do
      @client.tune_autovacuum(@queue_name)
      @client.tune_autovacuum(@queue_name)

      assert_in_delta 0.01, reloptions("q_#{@queue_name}")["autovacuum_vacuum_scale_factor"].to_f
    end

    it "returns nil" do
      assert_nil @client.tune_autovacuum(@queue_name)
    end

    it "raises for an invalid queue name" do
      assert_raises(PGMQ::Errors::InvalidQueueNameError) do
        @client.tune_autovacuum("123invalid")
      end
    end

    it "raises ConnectionError for a non-existent queue" do
      assert_raises(PGMQ::Errors::ConnectionError) do
        @client.tune_autovacuum("nonexistent_queue_xyz")
      end
    end

    it "coerces string-like numeric input safely" do
      @client.tune_autovacuum(@queue_name, queue_settings: { autovacuum_vacuum_scale_factor: "0.03", fillfactor: "85" })

      opts = reloptions("q_#{@queue_name}")

      assert_in_delta 0.03, opts["autovacuum_vacuum_scale_factor"].to_f
      assert_equal "85", opts["fillfactor"]
    end

    it "rejects non-numeric values rather than injecting them" do
      assert_raises(ArgumentError) do
        @client.tune_autovacuum(@queue_name,
          queue_settings: { autovacuum_vacuum_scale_factor: "0.01); DROP TABLE pgmq.meta; --" })
      end
    end

    it "rejects unknown storage parameters rather than injecting them" do
      assert_raises(ArgumentError) do
        @client.tune_autovacuum(@queue_name, queue_settings: { "evil) ; DROP TABLE pgmq.meta; --" => 1 })
      end
    end

    it "tunes the correct table for a mixed-case queue name" do
      # PGMQ folds queue names to lower case for the backing tables, so tune_autovacuum must target the lower-cased
      # table (pgmq.q_<name>) rather than the mixed-case name verbatim.
      mixed = "Av_Mixed_#{SecureRandom.hex(4)}"
      @client.create(mixed)

      begin
        @client.tune_autovacuum(mixed)

        assert_in_delta 0.01, reloptions("q_#{mixed.downcase}")["autovacuum_vacuum_scale_factor"].to_f
      ensure
        @client.drop_queue(mixed)
      end
    end
  end

  describe "create with tune_autovacuum option" do
    it "tunes both tables when create is called with tune_autovacuum: true" do
      @client.create(@queue_name, tune_autovacuum: true)

      assert_equal "70", reloptions("q_#{@queue_name}")["fillfactor"]
      assert_in_delta 0.05, reloptions("a_#{@queue_name}")["autovacuum_vacuum_scale_factor"].to_f
    end

    it "leaves tables untouched when create is called with the default (false)" do
      @client.create(@queue_name)

      assert_empty reloptions("q_#{@queue_name}")
      assert_empty reloptions("a_#{@queue_name}")
    end

    it "forwards a Hash of options from create" do
      @client.create(@queue_name,
        tune_autovacuum: { queue_settings: { autovacuum_vacuum_scale_factor: 0.002 }, archive: false })

      assert_in_delta 0.002, reloptions("q_#{@queue_name}")["autovacuum_vacuum_scale_factor"].to_f
      assert_empty reloptions("a_#{@queue_name}")
    end

    it "tunes when create_unlogged is called with tune_autovacuum: true" do
      @client.create_unlogged(@queue_name, tune_autovacuum: true)

      assert_equal "70", reloptions("q_#{@queue_name}")["fillfactor"]
    end

    it "still returns the created/existed boolean when tuning" do
      assert @client.create(@queue_name, tune_autovacuum: true)
      refute @client.create(@queue_name, tune_autovacuum: true)
    end
  end
end
