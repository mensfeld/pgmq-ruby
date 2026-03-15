# frozen_string_literal: true

RSpec.describe "PGMQ::VERSION" do
  it "has a version number" do
    expect(PGMQ::VERSION).not_to be_nil
  end

  it "version is a string" do
    expect(PGMQ::VERSION).to be_a(String)
  end

  it "version follows semantic versioning format" do
    expect(PGMQ::VERSION).to match(/\A\d+\.\d+\.\d+/)
  end
end
