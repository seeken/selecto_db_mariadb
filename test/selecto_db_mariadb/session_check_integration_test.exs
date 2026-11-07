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
    {"SET @@system_versioning_asof = '2024-01-01 00:00:00'", :system_versioning_asof},
    {"SET SESSION sql_select_limit = 3", :sql_select_limit},
    {"SET SESSION div_precision_increment = 0", :div_precision_increment}
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
        driver!(conn, "SET sql_select_limit = DEFAULT, div_precision_increment = DEFAULT")
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

  describe "sql_select_limit (R74)" do
    defp limited!(conn) do
      driver!(conn, "CREATE TABLE lim (id INT PRIMARY KEY, org VARCHAR(10), v INT DEFAULT 0)")
      values = Enum.map_join(1..10, ",", &"(#{&1}, 't1', 0)")
      driver!(conn, "INSERT INTO lim VALUES #{values}")
    end

    defp scoped_all(field, value) do
      scope = {:eq, {:field, "org"}, {:literal, "t1"}}

      {:ok, command} =
        Command.new(%{
          operation: :update,
          relation: "lim",
          predicate: scope,
          assignments: [%{field: field, value: {:literal, value}}],
          expected_cardinality: {:exactly, 10},
          metadata: %{post_image: %{predicate: scope, key: ["id"]}}
        })

      command
    end

    test "a capture is not short: a trigger moving the last row commits nothing" do
      with_database(fn conn ->
        limited!(conn)

        driver!(conn, """
        CREATE TRIGGER mv BEFORE UPDATE ON lim FOR EACH ROW
        BEGIN IF NEW.id = 10 THEN SET NEW.org = 't2'; END IF; END
        """)

        driver!(conn, "SET SESSION sql_select_limit = 3")

        assert {:error, %Error{details: %{setting: :sql_select_limit}}} =
                 Adapter.execute_write_unsafe(conn, scoped_all("v", 7), [])

        driver!(conn, "SET SESSION sql_select_limit = DEFAULT")

        assert [["t1", 10, _sum]] =
                 MyXQL.query!(conn, "SELECT org, COUNT(*), SUM(v) FROM lim GROUP BY org", [],
                   query_type: :text
                 ).rows
      end)
    end

    test "a write's trigger that sets it is undone and refused" do
      with_database(fn conn ->
        limited!(conn)

        driver!(conn, """
        CREATE TRIGGER lim3 BEFORE UPDATE ON lim FOR EACH ROW SET @@session.sql_select_limit = 3
        """)

        assert {:error, %Error{details: %{reason: :session_state_changed}}} =
                 Adapter.execute_write_unsafe(conn, scoped_all("v", 7), [])

        assert [[18_446_744_073_709_551_615]] =
                 MyXQL.query!(conn, "SELECT @@session.sql_select_limit", [], query_type: :text).rows
      end)
    end

    test "text wholly the host's keeps the host's limit" do
      with_database(fn conn ->
        limited!(conn)
        driver!(conn, "SET SESSION sql_select_limit = 3")

        assert {:ok, %{rows: [_, _, _]}} =
                 Adapter.execute(conn, "SELECT id FROM lim", [], selecto_read: true)
      end)
    end
  end

  describe "a read the server would cut (R74)" do
    @built [selecto_read: true, selecto_generated: true]
    @mixed [selecto_read: true, selecto_raw_sql: true]

    defp walk(depth) do
      "WITH RECURSIVE w (a, selecto_depth) AS (SELECT 1, 1 UNION ALL " <>
        "SELECT w.a + 1, w.selecto_depth + 1 FROM w WHERE w.selecto_depth < #{depth}) " <>
        "SELECT COUNT(*) FROM w"
    end

    defp notes!(conn) do
      driver!(conn, "CREATE TABLE big (id INT PRIMARY KEY, note VARCHAR(200))")

      driver!(
        conn,
        "SET STATEMENT max_recursive_iterations = 8000 FOR INSERT INTO big " <>
          "WITH RECURSIVE s (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 8000) " <>
          "SELECT n, REPEAT('x', 190) FROM s"
      )
    end

    test "a declared depth past the server's default runs in full" do
      with_database(fn conn ->
        for opts <- [@built, @mixed], depth <- [999, 1000, 1001, 2500] do
          assert {:ok, %{rows: [[^depth]]}} = Adapter.execute(conn, walk(depth), [], opts)
        end

        assert {:ok, stream, _columns} = Adapter.stream(conn, walk(2500), [], @built)
        assert Enum.to_list(stream) == [[2500]]
      end)
    end

    test "a session setting does not cut a declared depth, and the pin leaves it as it was" do
      with_database(fn conn ->
        driver!(conn, "SET SESSION max_recursive_iterations = 10")
        assert {:ok, %{rows: [[50]]}} = Adapter.execute(conn, walk(50), [], @built)

        assert [[10]] =
                 MyXQL.query!(conn, "SELECT @@session.max_recursive_iterations", [],
                   query_type: :text
                 ).rows
      end)
    end

    test "a CTE with no declared depth keeps the bound and fails visibly" do
      with_database(fn conn ->
        loop =
          "WITH RECURSIVE w (a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 20000) " <>
            "SELECT COUNT(*) FROM w"

        refusal = %{code: :query_rule_not_evaluable, reason: :result_cut, adapter: :mariadb}

        for opts <- [@built, @mixed] do
          assert {:error, %Selecto.Error{type: :query_enforcement, details: ^refusal}} =
                   Adapter.execute(conn, loop, [], opts)

          assert {:error, %Selecto.Error{details: ^refusal}} = Adapter.stream(conn, loop, [], opts)
        end

        # The host's own text reads what its session gives.
        assert {:ok, %{rows: [[1001]]}} = Adapter.execute(conn, loop, [], selecto_read: true)
      end)
    end

    test "a declared depth does not lower the bound of a CTE beside it" do
      with_database(fn conn ->
        both =
          "WITH RECURSIVE d (a, selecto_depth) AS (SELECT 1, 1 UNION ALL " <>
            "SELECT d.a + 1, d.selecto_depth + 1 FROM d WHERE d.selecto_depth < 5), " <>
            "n (a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM n WHERE a < 20) " <>
            "SELECT (SELECT COUNT(*) FROM d), (SELECT COUNT(*) FROM n)"

        assert {:ok, %{rows: [[5, 20]]}} = Adapter.execute(conn, both, [], @built)
      end)
    end

    test "harmless warnings past the session's cap answer; a cut behind them does not" do
      with_database(fn conn ->
        driver!(conn, "CREATE TABLE w (id INT PRIMARY KEY, code VARCHAR(20), note VARCHAR(100))")
        values = Enum.map_join(1..100, ",", &"(#{&1}, '#{&1}-A', REPEAT('x', 90))")
        driver!(conn, "INSERT INTO w VALUES #{values}")
        driver!(conn, "SET SESSION max_error_count = 3")

        harmless = "SELECT SUM(code) FROM w"
        assert %{num_warnings: 100} = MyXQL.query!(conn, harmless, [])
        assert {:ok, %{rows: [[_sum]]}} = Adapter.execute(conn, harmless, [], @built)
        assert {:ok, stream, _columns} = Adapter.stream(conn, harmless, [], @built)
        assert [[_sum]] = Enum.to_list(stream)

        # A 10,000-iteration cut behind 100 harmless warnings, with the cap kept at 3.
        hidden =
          "WITH RECURSIVE r (a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM r WHERE a < 20000) " <>
            "SELECT (SELECT SUM(code) FROM w), COUNT(*) FROM r"

        assert {:error, %Selecto.Error{details: %{reason: :result_cut}}} =
                 Adapter.execute(conn, hidden, [], @built)

        assert [[3]] =
                 MyXQL.query!(conn, "SELECT @@session.max_error_count", [], query_type: :text).rows
      end)
    end

    test "an aggregate fits at the server's default and under a small session value" do
      with_database(fn conn ->
        notes!(conn)
        text = "SELECT JSON_LENGTH(JSON_ARRAYAGG(note)) FROM big"

        assert {:ok, %{rows: [[8000]]}} = Adapter.execute(conn, text, [], @built)
        driver!(conn, "SET SESSION group_concat_max_len = 1000")
        assert {:ok, %{rows: [[8000]]}} = Adapter.execute(conn, text, [], @built)
        assert {:ok, %{rows: [[8000]]}} = Adapter.execute(conn, text, [], @mixed)

        assert {:ok, stream, _columns} = Adapter.stream(conn, text, [], @built)
        assert Enum.to_list(stream) == [[8000]]

        assert {:ok, %{rows: [[6]]}} = Adapter.execute(conn, text, [], selecto_read: true)
      end)
    end

    test "a cut the pin could not stop is an error, and only a cut is" do
      with_database(fn conn ->
        notes!(conn)
        driver!(conn, "SET SESSION group_concat_max_len = 1000")

        cut = MyXQL.query!(conn, "SELECT LENGTH(GROUP_CONCAT(note)) FROM big", [])
        assert cut.num_warnings == 1

        assert {:error, %Selecto.Error{details: %{reason: :result_cut}}} =
                 SelectoDBMariaDB.Session.not_cut(conn, cut.num_warnings, @built)

        other = MyXQL.query!(conn, "SELECT CAST('1x' AS INT)", [])
        assert other.num_warnings == 1
        assert :ok = SelectoDBMariaDB.Session.not_cut(conn, other.num_warnings, @built)
      end)
    end

    test "over max_allowed_packet is an error, not an OK with NULL" do
      with_database(fn conn ->
        [[packet]] =
          MyXQL.query!(conn, "SELECT @@session.max_allowed_packet", [], query_type: :text).rows

        rows = div(packet, 190) + 1000
        driver!(conn, "CREATE TABLE huge (id INT PRIMARY KEY, note VARCHAR(200))")

        driver!(
          conn,
          "SET STATEMENT max_recursive_iterations = #{rows} FOR INSERT INTO huge " <>
            "WITH RECURSIVE s (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < #{rows}) " <>
            "SELECT n, REPEAT('x', 190) FROM s"
        )

        refusal = {:error, :result_cut}
        reason = fn {:error, %Selecto.Error{details: %{reason: reason}}} -> {:error, reason} end

        assert reason.(Adapter.execute(conn, "SELECT JSON_ARRAYAGG(note) FROM huge", [], @built)) ==
                 refusal

        assert reason.(Adapter.execute(conn, "SELECT REPEAT('x', #{packet + 1})", [], @built)) ==
                 refusal
      end)
    end

    test "the pins are valid: TRADITIONAL runs them and they leave no warning" do
      with_database(fn conn ->
        notes!(conn)
        driver!(conn, "SET SESSION sql_mode = 'TRADITIONAL'")
        text = "SELECT JSON_LENGTH(JSON_ARRAYAGG(note)) FROM big"

        assert {:ok, %{rows: [[8000]]}} = Adapter.execute(conn, text, [], @built)
        assert {:ok, %{rows: [[1500]]}} = Adapter.execute(conn, walk(1500), [], @built)
        assert {:ok, %{rows: [[10_000]]}} = Adapter.execute(conn, walk(10_000), [], @built)

        for sql <- [text, walk(10_000)] do
          pinned = Adapter.statement(sql, @built)
          assert %{num_warnings: 0} = MyXQL.query!(conn, pinned, [])
        end
      end)
    end

    test "a plain role can set the pins" do
      with_database(fn conn ->
        [[database]] = MyXQL.query!(conn, "SELECT DATABASE()", [], query_type: :text).rows
        user = "mp_pin_#{System.unique_integer([:positive])}"
        driver!(conn, "CREATE USER '#{user}'@'%' IDENTIFIED BY 'pw'")
        driver!(conn, "GRANT SELECT ON `#{database}`.* TO '#{user}'@'%'")

        try do
          options =
            connection_options()
            |> Keyword.merge(username: user, password: "pw", database: database)

          {:ok, plain} = Adapter.connect(options)
          assert {:ok, %{rows: [[1500]]}} = Adapter.execute(plain, walk(1500), [], @built)
          Adapter.disconnect(plain)
        after
          driver!(conn, "DROP USER '#{user}'@'%'")
        end
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

    test "NO_TABLE_OPTIONS cannot hide it" do
      with_database(fn conn ->
        driver!(conn, "CREATE SEQUENCE sq")
        before = sequence_state(conn)
        driver!(conn, "SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_TABLE_OPTIONS')")

        {:ok, command} =
          Command.new(%{
            operation: :delete,
            relation: "sq",
            predicate: {:eq, {:field, "increment"}, {:literal, 1}}
          })

        assert {:error, %Error{details: %{reason: :sequence_target}}} =
                 Adapter.execute_write_unsafe(conn, command, [])

        assert sequence_state(conn) == before
      end)
    end

    test "a view is no write target" do
      with_database(fn conn ->
        driver!(conn, "CREATE VIEW items_view AS SELECT id, name FROM items")

        {:ok, command} =
          Command.new(%{
            operation: :update,
            relation: "items_view",
            predicate: {:eq, {:field, "id"}, {:literal, 1}},
            assignments: [%{field: "name", value: {:literal, "v"}}]
          })

        assert {:error, %Error{details: %{reason: :unsupported_write_target}}} =
                 Adapter.execute_write_unsafe(conn, command, [])
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
