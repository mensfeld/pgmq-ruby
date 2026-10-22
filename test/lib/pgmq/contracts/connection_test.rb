# frozen_string_literal: true

describe PGMQ::Contracts::Connection do
  let(:contract) { PGMQ::Contracts::Connection.new }

  it "accepts a connection String, a non-empty Hash or a callable" do
    ["postgres://localhost/db", { host: "localhost" }, -> {}].each do |conn_params|
      assert_predicate contract.call({ conn_params: conn_params }), :success?
    end
  end

  it "rejects nil connection parameters" do
    assert_equal({ conn_params: "are required" }, contract.call({ conn_params: nil }).errors)
  end

  it "rejects connection parameters of another format" do
    [{}, 1, []].each do |conn_params|
      assert_equal(
        { conn_params: "must be a connection String, a non-empty Hash or a callable" },
        contract.call({ conn_params: conn_params }).errors
      )
    end
  end

  it "accepts Strings and Regexps as reconnectable error patterns" do
    assert_predicate contract.call({ reconnectable_error_patterns: ["lost", /gone/i] }), :success?
    assert_predicate contract.call({ reconnectable_error_patterns: nil }), :success?
  end

  it "rejects other reconnectable error patterns" do
    assert_equal(
      { reconnectable_error_patterns: "must contain Strings or Regexps" },
      contract.call({ reconnectable_error_patterns: ["lost", 1] }).errors
    )
  end

  it "accepts Exception subclasses as reconnectable error classes" do
    assert_predicate contract.call({ reconnectable_error_classes: [PG::ConnectionBad] }), :success?
    assert_predicate contract.call({ reconnectable_error_classes: IOError }), :success?
  end

  it "rejects other reconnectable error classes" do
    [[String], ["PG::ConnectionBad"]].each do |classes|
      assert_equal(
        { reconnectable_error_classes: "must contain Exception subclasses" },
        contract.call({ reconnectable_error_classes: classes }).errors
      )
    end
  end
end
