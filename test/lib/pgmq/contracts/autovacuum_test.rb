# frozen_string_literal: true

describe PGMQ::Contracts::Autovacuum do
  let(:contract) { PGMQ::Contracts::Autovacuum.new }

  it "accepts known storage parameters" do
    assert_predicate contract.call({ queue_settings: {}, archive_settings: {} }), :success?
    assert_predicate(
      contract.call({ queue_settings: { fillfactor: 80 }, archive_settings: { autovacuum_vacuum_threshold: 10 } }),
      :success?
    )
  end

  it "rejects unknown storage parameters" do
    assert_equal(
      { queue_settings: "must contain only known storage parameters" },
      contract.call({ queue_settings: { evil: 1 }, archive_settings: {} }).errors
    )
    assert_equal(
      { archive_settings: "must contain only known storage parameters" },
      contract.call({ queue_settings: {}, archive_settings: { evil: 1 } }).errors
    )
  end

  it "accepts numeric values and numeric Strings" do
    assert_predicate(
      contract.call({ queue_settings: { autovacuum_vacuum_scale_factor: "0.03", fillfactor: "85" }, archive_settings: {} }),
      :success?
    )
  end

  it "rejects values that do not coerce to the parameter type" do
    assert_equal(
      { queue_settings: "must contain only numeric values" },
      contract.call({ queue_settings: { autovacuum_vacuum_scale_factor: "0.01); --" }, archive_settings: {} }).errors
    )
    assert_equal(
      { archive_settings: "must contain only numeric values" },
      contract.call({ queue_settings: {}, archive_settings: { autovacuum_vacuum_threshold: "1.5" } }).errors
    )
    assert_equal(
      { queue_settings: "must contain only numeric values" },
      contract.call({ queue_settings: { fillfactor: nil }, archive_settings: {} }).errors
    )
  end

  it "reports missing settings" do
    assert_equal({ archive_settings: "needs to be present" }, contract.call({ queue_settings: {} }).errors)
  end
end
