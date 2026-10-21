# frozen_string_literal: true

describe PGMQ::Contracts::Consumer do
  let(:contract) { PGMQ::Contracts::Consumer.new }
  let(:config) do
    {
      queue_names: ["orders"],
      vt: 30,
      batch_size: 1,
      max_poll_seconds: 5,
      poll_interval_ms: 100,
      on_success: :delete,
      max_attempts: nil
    }
  end

  it "accepts a valid config" do
    assert_predicate contract.call(config), :success?
    assert_predicate contract.call(config.merge(on_success: :archive, max_attempts: 3)), :success?
  end

  it "rejects an empty, too long or duplicated list of queue names" do
    assert_equal(
      { queue_names: "needs at least one queue name" },
      contract.call(config.merge(queue_names: [])).errors
    )
    assert_equal(
      { queue_names: "cannot exceed 50 queues" },
      contract.call(config.merge(queue_names: Array.new(51) { |i| "q#{i}" })).errors
    )
    assert_equal(
      { queue_names: "must be unique (case-insensitive)" },
      contract.call(config.merge(queue_names: ["orders", :ORDERS])).errors
    )
  end

  it "leaves duplicated invalid queue names to PGMQ::QueueName" do
    assert_predicate contract.call(config.merge(queue_names: [nil, nil])), :success?
  end

  it "rejects an unknown on_success action" do
    assert_equal(
      { on_success: "must be :delete or :archive" },
      contract.call(config.merge(on_success: :purge)).errors
    )
  end

  %i[vt batch_size max_poll_seconds poll_interval_ms].each do |key|
    it "rejects a #{key} that is not a positive Integer" do
      [0, -1, "1", 1.5, nil].each do |value|
        assert_equal(
          { key => "must be a positive Integer" },
          contract.call(config.merge(key => value)).errors
        )
      end
    end
  end

  it "rejects a max_attempts that is not a positive Integer" do
    assert_equal(
      { max_attempts: "must be a positive Integer or nil" },
      contract.call(config.merge(max_attempts: 0)).errors
    )
  end

  it "rejects max_attempts with a batch_size above 1" do
    assert_equal(
      { max_attempts: "requires batch_size: 1" },
      contract.call(config.merge(batch_size: 3, max_attempts: 1)).errors
    )
  end

  it "reports missing options" do
    assert_equal({ vt: "needs to be present" }, contract.call(config.except(:vt)).errors)
  end
end
