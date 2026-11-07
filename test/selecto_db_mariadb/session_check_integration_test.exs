defmodule SelectoDBMariaDB.SessionCheckIntegrationTest do
  # Every read, stream and write verifies the session on the connection it
  # uses, on every call: a host that changes the session between calls is
  # caught on the next call.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  @unsafe [
    {"SET SESSION time_zone = '+01:00'", :time_zone},
    {"SET SESSION time_zone = '-00:30'", :time_zone},
    {"SET character_set_results = latin1", :character_set},
    {"SET character_set_client = latin1", :character_set},
    {"SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'", :sql_mode},
    {"SET @@system_versioning_asof = '2024-01-01 00:00:00'", :system_versioning_asof}
  ]

  test "a safe session reads, streams and writes" do
    with_database(fn conn ->
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, "SELECT id FROM items", [], [])
      assert {:ok, stream, ["id"]} = Adapter.stream(conn, "SELECT id FROM items", [], [])
      assert Enum.to_list(stream) == [[1]]
      assert {:ok, %{affected_rows: 1}} = Adapter.execute_write_unsafe(conn, update(), [])

      driver!(conn, "SET SESSION time_zone = '+00:00'")
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, "SELECT id FROM items", [], [])
    end)
  end

  for {statement, setting} <- @unsafe do
    test "#{statement} is refused before any read, stream or write runs" do
      with_database(fn conn ->
        driver!(conn, unquote(statement))

        assert {:error, %Selecto.Error{details: %{category: :invalid_session} = details}} =
                 Adapter.execute(conn, "SELECT id FROM items", [], [])

        assert details.setting == unquote(setting)

        assert {:error, %Selecto.Error{details: %{setting: unquote(setting)}}} =
                 Adapter.stream(conn, "SELECT id FROM items", [], [])

        assert {:error, %Error{type: :invalid_connection, details: write_details}} =
                 Adapter.execute_write_unsafe(conn, update(), [])

        assert write_details.setting == unquote(setting)

        driver!(conn, "SET SESSION time_zone = '+00:00', character_set_results = utf8mb4")
        driver!(conn, "SET character_set_client = utf8mb4, sql_mode = DEFAULT")
        driver!(conn, "SET @@system_versioning_asof = DEFAULT")
        assert {:ok, %{rows: [[name]]}} = MyXQL.query(conn, "SELECT name FROM items", [])
        assert name == "a"
      end)
    end
  end

  describe "SYSTEM VERSIONED tables under system_versioning_asof" do
    @past "2024-01-01 00:00:00"
    @scope {:eq, {:field, "org"}, {:literal, "t1"}}

    defp versioned!(conn) do
      driver!(conn, "SET @@timestamp = UNIX_TIMESTAMP('2023-01-01 00:00:00')")

      driver!(
        conn,
        "CREATE TABLE sv (id INT PRIMARY KEY, org VARCHAR(10), v INT) WITH SYSTEM VERSIONING"
      )

      driver!(conn, "INSERT INTO sv VALUES (1, 't1', 0), (2, 't1', 0)")
      driver!(conn, "SET @@timestamp = DEFAULT")
      driver!(conn, "UPDATE sv SET org = 't2' WHERE id = 2")
    end

    defp scoped_update(id, field, value) do
      {:ok, command} =
        Command.new(%{
          operation: :update,
          relation: "sv",
          predicate: {:and, [{:eq, {:field, "id"}, {:literal, id}}, @scope]},
          assignments: [%{field: field, value: {:literal, value}}],
          expected_cardinality: {:exactly, 1},
          metadata: %{post_image: %{predicate: @scope, key: ["id"]}}
        })

      command
    end

    defp current(conn),
      do: MyXQL.query!(conn, "SELECT id, org, v FROM sv ORDER BY id", [], query_type: :text).rows

    test "a set session refuses generated reads and writes; the guards never read the past" do
      with_database(fn conn ->
        versioned!(conn)
        before = current(conn)
        assert before == [[1, "t1", 0], [2, "t2", 0]]
        driver!(conn, "SET @@system_versioning_asof = '#{@past}'")

        for mark <- [:selecto_generated, :selecto_raw_sql] do
          read = [{:selecto_read, true}, {mark, true}]

          assert {:error, %Selecto.Error{details: %{setting: :system_versioning_asof}}} =
                   Adapter.execute(conn, "SELECT id FROM sv", [], read)

          assert {:error, %Selecto.Error{details: %{setting: :system_versioning_asof}}} =
                   Adapter.stream(conn, "SELECT id FROM sv", [], read)
        end

        # The post-image guard would read the old org and pass this move.
        assert {:error, %Error{type: :invalid_connection, details: details}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(1, "org", "t2"), [])

        assert details.setting == :system_versioning_asof

        assert {:error, %Error{type: :invalid_connection}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(2, "v", 9), [])

        driver!(conn, "SET @@system_versioning_asof = DEFAULT")
        assert current(conn) == before
      end)
    end

    test "text wholly the host's reads the past the host asked for" do
      with_database(fn conn ->
        versioned!(conn)
        driver!(conn, "SET @@system_versioning_asof = '#{@past}'")

        assert {:ok, %{rows: [[1, "t1"], [2, "t1"]]}} =
                 Adapter.execute(conn, "SELECT id, org FROM sv ORDER BY id", [],
                   selecto_read: true
                 )
      end)
    end

    test "at DEFAULT a versioned table reads and writes under its guards" do
      with_database(fn conn ->
        versioned!(conn)

        assert {:ok, %{affected_rows: 1}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(1, "v", 9), [])

        assert {:error, %Error{type: :cardinality_mismatch}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(2, "v", 9), [])

        assert {:error, %Error{details: %{code: :query_rule_violation}}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(1, "org", "t2"), [])

        assert current(conn) == [[1, "t1", 9], [2, "t2", 0]]
      end)
    end

    test "a write's trigger that sets it is undone and refused" do
      with_database(fn conn ->
        versioned!(conn)

        driver!(conn, """
        CREATE TRIGGER sv_past BEFORE UPDATE ON sv FOR EACH ROW
        SET @@session.system_versioning_asof = '#{@past}'
        """)

        assert {:error, %Error{details: %{reason: :session_state_changed}}} =
                 Adapter.execute_write_unsafe(conn, scoped_update(1, "v", 9), [])

        assert [["DEFAULT"]] =
                 MyXQL.query!(conn, "SELECT @@session.system_versioning_asof", [],
                   query_type: :text
                 ).rows

        assert current(conn) == [[1, "t1", 0], [2, "t2", 0]]
      end)
    end
  end

  describe "a SEQUENCE is no write target" do
    defp sequence_state(conn),
      do:
        MyXQL.query!(conn, "SELECT next_not_cached_value, increment FROM sq", [],
          query_type: :text
        ).rows

    test "insert, update, delete and upsert refuse and leave it as it was" do
      with_database(fn conn ->
        driver!(conn, "CREATE SEQUENCE sq")
        before = sequence_state(conn)

        columns = [
          {"next_not_cached_value", 900},
          {"minimum_value", 1},
          {"maximum_value", 9999},
          {"start_value", 1},
          {"increment", 1},
          {"cache_size", 0},
          {"cycle_option", 0},
          {"cycle_count", 0}
        ]

        assignments = Enum.map(columns, fn {f, v} -> %{field: f, value: {:literal, v}} end)
        predicate = {:eq, {:field, "increment"}, {:literal, 1}}

        for fields <- [
              %{operation: :insert, assignments: assignments},
              %{
                operation: :update,
                predicate: predicate,
                assignments: [%{field: "increment", value: {:literal, 5}}]
              },
              %{operation: :delete, predicate: predicate}
            ] do
          {:ok, command} = Command.new(Map.put(fields, :relation, "sq"))

          assert {:error,
                  %Error{
                    type: :query_enforcement,
                    details: %{code: :query_rule_not_evaluable, reason: :sequence_target}
                  }} = Adapter.execute_write_unsafe(conn, command, [])
        end

        assert sequence_state(conn) == before
      end)
    end

    test "a table whose comment reads like one still writes" do
      with_database(fn conn ->
        driver!(conn, "CREATE TABLE note (id INT PRIMARY KEY) COMMENT 'SEQUENCE=1'")

        {:ok, command} =
          Command.new(%{
            operation: :insert,
            relation: "note",
            assignments: [%{field: "id", value: {:literal, 1}}]
          })

        assert {:ok, %{affected_rows: 1}} = Adapter.execute_write_unsafe(conn, command, [])
      end)
    end
  end

  test "a session changed inside a host transaction is caught on the next call" do
    with_database(fn conn ->
      {:ok, result} =
        MyXQL.transaction(conn, fn tx ->
          assert {:ok, _} = Adapter.execute(tx, "SELECT id FROM items", [], [])
          MyXQL.query!(tx, "SET SESSION time_zone = '+02:00'", [], query_type: :text)
          Adapter.execute(tx, "SELECT id FROM items", [], [])
        end)

      assert {:error, %Selecto.Error{details: %{setting: :time_zone}}} = result
    end)
  end

  defp update do
    %Command{
      operation: :update,
      relation: "items",
      assignments: [%{field: "name", value: {:literal, "b"}}],
      predicate: {:eq, {:field, "id"}, {:literal, 1}},
      returning: :none,
      expected_cardinality: {:exactly, 1},
      metadata: %{}
    }
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_session_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    driver!(admin, "CREATE DATABASE `#{database}`")

    try do
      # One connection, so a session change applies to every later call.
      {:ok, conn} =
        Adapter.connect(Keyword.merge(connection_options(), database: database, pool_size: 1))

      try do
        driver!(conn, "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB")
        driver!(conn, "INSERT INTO items VALUES (1, 'a')")
        fun.(conn)
      after
        Adapter.disconnect(conn)
      end
    after
      MyXQL.query(admin, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
      Adapter.disconnect(admin)
    end
  end

  defp driver!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
