# frozen_string_literal: true

describe PGMQ do
  describe ".new" do
    it "creates a client via the convenience method with hash params" do
      client = PGMQ.new(TEST_DB_PARAMS)

      assert_kind_of PGMQ::Client, client
      assert_kind_of PGMQ::Connection, client.connection
    end

    it "supports connection string parameter" do
      conn_string = "postgres://postgres:postgres@localhost:5433/pgmq_test"
      client = PGMQ.new(conn_string)

      assert_kind_of PGMQ::Client, client
    end
  end
end
