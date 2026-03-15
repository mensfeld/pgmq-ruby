# frozen_string_literal: true

describe "PGMQ::VERSION" do
  it "has a version number" do
    refute_nil PGMQ::VERSION
  end

  it "version is a string" do
    assert_kind_of String, PGMQ::VERSION
  end

  it "version follows semantic versioning format" do
    assert_match(/\A\d+\.\d+\.\d+/, PGMQ::VERSION)
  end
end
