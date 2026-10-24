defmodule SelectoDBMariaDB.ColumnCatalogIntegrationTest do
  # column_catalog/2 reports information_schema COLUMN_TYPE with the column
  # collation (query_enforced_scalar v2 verified metadata). SelectoUpdato's
  # in-process candidate check decides by it; MySQL-family stored-row
  # admission still re-checks every admitted insert.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  test "types carry length, precision, scale, fractional seconds and collation" do
    with_database(fn conn ->
      ddl!(
        conn,
        "CREATE TABLE cat (id BIGINT PRIMARY KEY, org_key VARCHAR(2), bin_key VARCHAR(10) " <>
          "COLLATE utf8mb4_bin, price DECIMAL(12,2), at DATETIME(3), n INT UNSIGNED, flag BOOLEAN)"
      )

      {:ok, columns} = Adapter.column_catalog(conn, "cat")

      assert columns["org_key"].type =~ ~r/\Avarchar\(2\) collate \S+\z/
      assert columns["bin_key"].type == "varchar(10) collate utf8mb4_bin"
      assert columns["price"].type == "decimal(12,2)"
      assert columns["at"].type == "datetime(3)"
      # MariaDB keeps display widths (int(10) unsigned, bigint(20)).
      assert columns["n"].type =~ ~r/\Aint(\(10\))? unsigned\z/
      assert columns["flag"].type == "tinyint(1)"
      assert columns["id"].type =~ ~r/\Abigint(\(20\))?\z/
      assert Enum.all?(Map.values(columns), &is_nil(&1.deterministic))

      assert {:ok, %{}} = Adapter.column_catalog(conn, "no_such_table")
    end)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_catalog_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    ddl!(admin, "CREATE DATABASE `#{database}`")

    try do
      {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

      try do
        fun.(conn)
      after
        Adapter.disconnect(conn)
      end
    after
      MyXQL.query(admin, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
      Adapter.disconnect(admin)
    end
  end

  defp ddl!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
