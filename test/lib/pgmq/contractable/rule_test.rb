# frozen_string_literal: true

describe PGMQ::Contractable::Rule do
  it "is a Struct with path, type and validator" do
    assert_operator PGMQ::Contractable::Rule, :<, Struct
    assert_equal %i[path type validator], PGMQ::Contractable::Rule.members
  end
end
