defmodule SelectoDBMariaDB.SessionStateIntegrationTest do
  # Session state a statement can change for the statements after it, and
  # host-owned transactions (brief items 42, 44, 59, 62, 62a, temporary
  # shadows on reads). A stored function a domain view calls, or a row
  # trigger of a write, can SET time_zone mid-statement; a trigger can
  # CREATE TEMPORARY TABLE; a host can hold an implicit transaction with
  # autocommit=0; MySQL generates a new AUTO_INCREMENT value for every
  # assigned zero spelling.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  defp domain(table, required_filters \\ []) do
    %{
      name: table,
      source: %{
        source_table: table,
        primary_key: :id,
        tenant_field: :org_key,
        fields: [:id, :org_key, :ts, :note],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          org_key: %{type: :string},
          ts: %{type: :utc_datetime},
          note: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      required_filters: required_filters
    }
  end

  defp read(conn, table, required_filters \\ [], select \\ ["id", "note"]) do
    domain(table, required_filters)
    |> update_in([:source, :fields], &(&1 ++ [:k]))
    |> put_in([:source, :columns, :k], %{type: :integer})
    |> Selecto.configure(conn, adapter: Adapter, validate: false)
    |> Selecto.with_tenant("t1")
    |> Selecto.select(select)
    |> Selecto.order_by(["id"])
    |> Selecto.execute()
  end

  defp rows({:ok, {rows, _columns, _aliases}}), do: rows
  defp rows({:error, %{details: %{reason: reason}}}), do: reason

  defp zone(conn), do: query!(conn, "SELECT @@session.time_zone")

  defp timestamp_table!(conn) do
    ddl!(
      conn,
      "CREATE TABLE tzb (id INT PRIMARY KEY, org_key VARCHAR(8) NOT NULL, ts TIMESTAMP NULL, " <>
        "note VARCHAR(16)) ENGINE=InnoDB"
    )

    ddl!(
      conn,
      "INSERT INTO tzb VALUES (1,'t1','2023-12-31 22:00:00','in'), " <>
        "(2,'t1','2024-01-01 03:00:00','OUT'), (3,'t1','2023-06-01 00:00:00','in3')"
    )
  end

  defp function_view!(conn, body) do
    ddl!(
      conn,
      "CREATE FUNCTION tzf(i INT) RETURNS INT DETERMINISTIC NO SQL BEGIN #{body} RETURN i; END"
    )

    ddl!(conn, "CREATE VIEW tzv AS SELECT id, org_key, ts, note, tzf(id) AS k FROM tzb")
  end

  @scope [{"ts", {:lt, "2024-01-01 00:00:00"}}]

  test "a view function that switches time_zone refuses the read and restores the session" do
    with_database(fn conn ->
      timestamp_table!(conn)
      function_view!(conn, "IF i = 1 THEN SET time_zone = '-05:00'; END IF;")
      before = zone(conn)

      # The function runs for the selected view column `k`.
      assert rows(read(conn, "tzv", @scope, ["id", "note", "k"])) == :session_state_changed
      assert zone(conn) == before
      assert rows(read(conn, "tzb", @scope)) == [[1, "in"], [3, "in3"]]
    end)
  end

  test "a time_zone switch and back inside one read cannot widen a TIMESTAMP scope" do
    with_database(fn conn ->
      timestamp_table!(conn)

      function_view!(
        conn,
        "IF i = 1 THEN SET @tz0 = @@session.time_zone; SET time_zone = '-05:00'; END IF; " <>
          "IF i = 3 THEN SET time_zone = @tz0; END IF;"
      )

      assert rows(read(conn, "tzv", @scope, ["id", "note", "k"])) == [[1, "in", 1], [3, "in3", 3]]
    end)
  end

  test "a trigger's time_zone switch refuses the write and restores the session" do
    with_database(fn conn ->
      timestamp_table!(conn)

      ddl!(
        conn,
        "CREATE TRIGGER tzt BEFORE UPDATE ON tzb FOR EACH ROW BEGIN SET time_zone = '-05:00'; END"
      )

      before = zone(conn)

      assert {:error, %Error{details: %{reason: :session_state_changed}}} =
               Adapter.execute_write_unsafe(conn, update("tzb", 1, "w"), [])

      assert zone(conn) == before
      assert query!(conn, "SELECT note FROM tzb WHERE id = 1") == [["in"]]
    end)
  end

  test "a TEMPORARY table shadowing a read relation is refused, for reads and streams" do
    with_database(fn conn ->
      timestamp_table!(conn)

      ddl!(
        conn,
        "CREATE TEMPORARY TABLE tzb (id INT PRIMARY KEY, org_key VARCHAR(8), ts TIMESTAMP NULL, " <>
          "note VARCHAR(16)) ENGINE=MEMORY"
      )

      assert rows(read(conn, "tzb")) == :temporary_shadow

      stream =
        domain("tzb")
        |> Selecto.configure(conn, adapter: Adapter, validate: false)
        |> Selecto.with_tenant("t1")
        |> Selecto.select(["id", "note"])
        |> Selecto.execute_stream()

      assert {:error, %{details: %{reason: :temporary_shadow}}} = stream

      # Unrelated temporary tables keep working (item 19).
      ddl!(conn, "DROP TEMPORARY TABLE tzb")
      ddl!(conn, "CREATE TEMPORARY TABLE scratch (id INT)")
      assert rows(read(conn, "tzb")) == [[1, "in"], [2, "OUT"], [3, "in3"]]
    end)
  end

  test "autocommit=0 is the host's transaction: a savepoint, or a refusal on a managed path" do
    with_database(fn conn ->
      timestamp_table!(conn)

      DBConnection.run(conn, fn checked_out ->
        query!(checked_out, "SET autocommit = 0")

        assert {:ok, _} = Adapter.execute_write_unsafe(checked_out, update("tzb", 1, "host"), [])

        query!(checked_out, "ROLLBACK")
        query!(checked_out, "SET autocommit = 1")
      end)

      assert query!(conn, "SELECT note FROM tzb WHERE id = 1") == [["in"]]

      # A pooled connection handed back with autocommit=0 is never written
      # into by a managed transaction.
      query!(conn, "SET autocommit = 0")

      assert {:error, %Error{details: %{code: :leaked_transaction}}} =
               Adapter.execute_write_unsafe(conn, update("tzb", 1, "managed"), [])

      query!(conn, "ROLLBACK")
      query!(conn, "SET autocommit = 1")
      assert query!(conn, "SELECT note FROM tzb WHERE id = 1") == [["in"]]
    end)
  end

  test "an AUTO_INCREMENT key assigned a zero spelling is not evaluable" do
    with_database(fn conn ->
      ddl!(
        conn,
        "CREATE TABLE ai (id INT AUTO_INCREMENT PRIMARY KEY, org_key VARCHAR(8) NOT NULL, " <>
          "note VARCHAR(16)) ENGINE=InnoDB"
      )

      ddl!(conn, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO'")
      ddl!(conn, "INSERT INTO ai VALUES (0, 't1', 'SECRET')")
      # Without NO_AUTO_VALUE_ON_ZERO every zero spelling generates a new id.
      ddl!(conn, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")

      for id <- [0, "0", "00", "+0"] do
        {:ok, command} =
          Command.new(%{
            operation: :insert,
            relation: "ai",
            assignments: [
              %{field: "id", value: {:literal, id}},
              %{field: "org_key", value: {:literal, "t1"}},
              %{field: "note", value: {:literal, "mine"}}
            ],
            metadata: %{
              insert_admission: %{
                predicate: {:eq, {:field, "org_key"}, {:literal, "t1"}},
                primary_key: "id"
              }
            }
          })

        assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
                 Adapter.execute_write_unsafe(conn, command, []),
               inspect(id)
      end

      assert query!(conn, "SELECT id, note FROM ai ORDER BY id") == [[0, "SECRET"]]
    end)
  end

  defp update(table, id, note) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: table,
        predicate: {:eq, {:field, "id"}, {:literal, id}},
        assignments: [%{field: "note", value: {:literal, note}}],
        expected_cardinality: {:exactly, 1}
      })

    command
  end

  defp query!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text).rows

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_session_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    ddl!(admin, "CREATE DATABASE `#{database}`")

    try do
      {:ok, conn} =
        Adapter.connect(
          connection_options()
          |> Keyword.put(:database, database)
          |> Keyword.put(:pool_size, 1)
        )

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
