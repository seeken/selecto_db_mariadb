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

  defp scoped(table, columns, required_filters) do
    %{
      name: table,
      source: %{
        source_table: table,
        primary_key: :id,
        redact_fields: [],
        fields: Map.keys(columns),
        columns: columns,
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      required_filters: required_filters
    }
    |> Selecto.configure(Process.get(:conn), adapter: Adapter, validate: false)
    |> Selecto.select(["id"])
    |> Selecto.order_by(["id"])
    |> Selecto.execute()
  end

  test "identity text ranges and patterns compare bytes (R3)" do
    with_database(fn conn ->
      Process.put(:conn, conn)

      ddl!(
        conn,
        "CREATE TABLE idt (id INT PRIMARY KEY, org_key VARCHAR(8) NOT NULL) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO idt VALUES (1,'t1'), (2,'T1'), (3,'t1 '), (4,'t2')")
      columns = %{id: %{type: :integer}, org_key: %{type: :string}}

      for {filters, expected} <- [
            {[{"org_key", {:between, "t1", "t1"}}], [[1]]},
            {[{"org_key", {:gte, "t1"}}, {"org_key", {:lte, "t1"}}], [[1]]},
            {[{"org_key", {:starts_with, "t1"}}], [[1], [3]]},
            {[{"org_key", {:like, "t1%"}}], [[1], [3]]}
          ] do
        assert rows(scoped("idt", columns, filters)) == expected, inspect(filters)
      end
    end)
  end

  test "TIMESTAMP scope: patterns and lists refuse (R4), zero dates keep native semantics (R5)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")

      ddl!(
        conn,
        "CREATE TABLE tzr (id INT PRIMARY KEY, ts TIMESTAMP NULL, dt DATETIME NULL) ENGINE=InnoDB"
      )

      ddl!(
        conn,
        "INSERT INTO tzr VALUES (1,'2023-06-01 00:00:00','3500-01-01 00:00:00'), " <>
          "(2,'0000-00-00 00:00:00','1950-01-01 00:00:00'), (3,'2025-01-01 00:00:00','2023-01-01 00:00:00')"
      )

      ddl!(conn, "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
      columns = %{id: %{type: :integer}, ts: %{type: :utc_datetime}, dt: %{type: :naive_datetime}}

      for filter <- [{"ts", {:starts_with, "2023"}}, {"ts", {:like, "2023%"}}] do
        assert rows(scoped("tzr", columns, [filter])) == :timestamp_operator
      end

      assert rows(scoped("tzr", columns, [{"ts", {:in, ["2023-06-01 00:00:00"]}}])) == [[1]]

      for {filter, where} <- [
            {{"ts", {:lt, "1960-01-01 00:00:00"}}, "ts < '1960-01-01 00:00:00'"},
            {{"ts", {:gt, "1960-01-01 00:00:00"}}, "ts > '1960-01-01 00:00:00'"},
            {{:not, {"ts", {:gt, "2024-01-01 00:00:00"}}}, "NOT (ts > '2024-01-01 00:00:00')"},
            {{"dt", {:gt, "2024-01-01 00:00:00"}}, "dt > '2024-01-01 00:00:00'"}
          ] do
        assert rows(scoped("tzr", columns, [filter])) ==
                 query!(conn, "SELECT id FROM tzr WHERE #{where} ORDER BY id"),
               where
      end
    end)
  end

  test "computed columns over a TIMESTAMP stay zone-independent under CASE, = false and <> (R7)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE q1 (id INT PRIMARY KEY, ts TIMESTAMP NULL) ENGINE=InnoDB")

      ddl!(
        conn,
        "INSERT INTO q1 VALUES (1,'2024-01-01 03:00:00'), (2,'2023-12-31 20:00:00'), " <>
          "(3,'2023-12-31 21:00:00'), (4,'2024-01-02 00:00:00')"
      )

      ddl!(
        conn,
        "CREATE FUNCTION q1tz(i INT) RETURNS INT NOT DETERMINISTIC NO SQL BEGIN " <>
          "IF i = 1 THEN SET @tz0 = @@session.time_zone; SET time_zone = '+05:00'; END IF; " <>
          "IF i = 4 THEN SET time_zone = @tz0; END IF; RETURN 1; END"
      )

      ddl!(conn, "CREATE VIEW q1v AS SELECT * FROM q1 WHERE q1tz(id) = 1")

      columns = %{
        id: %{type: :integer},
        ts: %{type: :utc_datetime},
        flag: %{
          type: :string,
          computed: %{
            kind: :expression,
            expression: [
              "case",
              [["lt", "ts", "2024-01-01 00:00:00"], ["literal", "y"]],
              ["else", ["literal", "n"]]
            ]
          }
        },
        early: %{
          type: :boolean,
          computed: %{kind: :predicate, expression: ["lt", "ts", "2024-01-01 00:00:00"]}
        }
      }

      for filter <- [
            {"flag", "n"},
            {"flag", {:!=, "y"}},
            {"early", false},
            {"early", {:!=, true}}
          ] do
        assert rows(scoped("q1v", columns, [filter])) == [[1], [4]], inspect(filter)
      end
    end)
  end

  defp run_domain(domain) do
    domain
    |> Selecto.configure(Process.get(:conn), adapter: Adapter, validate: false)
    |> Selecto.select(["id"])
    |> Selecto.order_by(["id"])
    |> Selecto.execute()
  end

  defp relation(table, columns) do
    %{
      source_table: table,
      primary_key: :id,
      redact_fields: [],
      fields: Map.keys(columns),
      columns: columns,
      associations: %{}
    }
  end

  test "storage types are keyed by relation: a same-named column elsewhere does not decide (H1, R18)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE h1a (id INT PRIMARY KEY, ts TIMESTAMP NULL) ENGINE=InnoDB")

      ddl!(
        conn,
        "CREATE TABLE h1b (id INT PRIMARY KEY, a_id INT, ts DATETIME NULL) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO h1a VALUES (1, '2023-01-01 00:00:00')")

      ddl!(
        conn,
        "INSERT INTO h1b VALUES (1, 1, '2023-06-01 00:00:00'), (2, 1, '2025-01-01 00:00:00')"
      )

      domain = %{
        name: "h1b",
        source:
          relation("h1b", %{
            id: %{type: :integer},
            a_id: %{type: :integer},
            ts: %{type: :naive_datetime}
          })
          |> Map.put(:associations, %{
            a: %{queryable: :a, field: :a, owner_key: :a_id, related_key: :id, cardinality: :one}
          }),
        schemas: %{a: relation("h1a", %{id: %{type: :integer}, ts: %{type: :utc_datetime}})},
        joins: %{a: %{type: :left, name: "a"}},
        required_filters: [{"ts", {:lt, "2024-01-01 00:00:00"}}]
      }

      # h1b.ts is DATETIME: the native comparison, not a refusal for the
      # TIMESTAMP of h1a's same-named column.
      assert rows(run_domain(domain)) == [[1]]
    end)
  end

  test "an integer-declared scope column stored as VARCHAR compares bytes (R26)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE r26 (id INT PRIMARY KEY, org_n VARCHAR(8) NOT NULL) ENGINE=InnoDB")
      ddl!(conn, "INSERT INTO r26 VALUES (1, '1'), (2, '01'), (3, ' 1'), (4, '1x')")
      assert query!(conn, "SELECT id FROM r26 WHERE org_n = 1 ORDER BY id") |> length() > 1

      domain = %{
        name: "r26",
        source: relation("r26", %{id: %{type: :integer}, org_n: %{type: :integer}}),
        schemas: %{},
        joins: %{},
        required_filters: [{"org_n", 1}]
      }

      assert rows(run_domain(domain)) == [[1]]
    end)
  end

  test "relation names are matched exactly: rv_v and rv_V are two tables (R31)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE rv_v (id INT PRIMARY KEY, ts TIMESTAMP NULL) ENGINE=InnoDB")
      ddl!(conn, "CREATE TABLE rv_V (id INT PRIMARY KEY, ts DATETIME NULL) ENGINE=InnoDB")

      ddl!(
        conn,
        "INSERT INTO rv_v VALUES (1, '2023-12-31 22:00:00'), (2, '2024-01-01 03:00:00')"
      )

      ddl!(conn, "INSERT INTO rv_V VALUES (1, '2020-01-01 00:00:00')")

      domain = fn table, type ->
        %{
          name: table,
          source: relation(table, %{id: %{type: :integer}, ts: %{type: type}}),
          schemas: %{},
          joins: %{},
          required_filters: [{"ts", {:lt, "2024-01-01 00:00:00"}}]
        }
      end

      assert rows(run_domain(domain.("rv_v", :utc_datetime))) == [[1]]
      assert rows(run_domain(domain.("rv_V", :naive_datetime))) == [[1]]

      # Each read is keyed by its own table's types.
      selecto =
        domain.("rv_v", :utc_datetime)
        |> Selecto.configure(conn, adapter: Adapter, validate: false)

      assert {:ok, prepared} = Adapter.prepare_read(selecto, conn)
      assert Map.keys(prepared.set.storage_column_types) == ["rv_v"]
      assert prepared.set.storage_column_types["rv_v"]["ts"] == MapSet.new(["timestamp"])
    end)
  end

  test "required filters on INT and DECIMAL bind only exact literals (R3 clarification 2)" do
    with_database(fn conn ->
      Process.put(:conn, conn)

      ddl!(
        conn,
        "CREATE TABLE r3c (id INT PRIMARY KEY, n INT NOT NULL, d DECIMAL(10,2) NOT NULL) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO r3c VALUES (1, 1, 1.01), (2, 2, 2.50)")

      domain = fn filters ->
        %{
          name: "r3c",
          source:
            relation("r3c", %{id: %{type: :integer}, n: %{type: :string}, d: %{type: :string}}),
          schemas: %{},
          joins: %{},
          required_filters: filters
        }
      end

      assert rows(run_domain(domain.([{"n", "1"}]))) == [[1]]
      assert rows(run_domain(domain.([{"d", "1.01"}]))) == [[1]]

      for filter <- [{"n", "1.4"}, {"n", "1e0"}, {"d", "1.005"}] do
        assert rows(run_domain(domain.([filter]))) == :identity_value, inspect(filter)
      end
    end)
  end

  test "a view's temporal column needs a proven direct base mapping (R40)" do
    with_database(fn conn ->
      Process.put(:conn, conn)

      ddl!(
        conn,
        "CREATE TABLE r40 (id INT PRIMARY KEY, ts TIMESTAMP NULL, dt DATETIME NULL) ENGINE=InnoDB"
      )

      ddl!(
        conn,
        "INSERT INTO r40 VALUES (1, '2023-12-31 22:00:00', '2023-12-31 22:00:00'), " <>
          "(2, '2024-01-01 03:00:00', '2024-01-01 03:00:00')"
      )

      # Reported as DATETIME, but derived from a TIMESTAMP: zone-dependent.
      ddl!(conn, "CREATE VIEW r40_expr AS SELECT id, ts + INTERVAL 0 SECOND AS ts FROM r40")
      # A base DATETIME and a base TIMESTAMP taken as they are.
      ddl!(conn, "CREATE VIEW r40_direct AS SELECT id, dt, ts FROM r40")

      domain = fn table, column, type ->
        %{
          name: table,
          source: relation(table, %{:id => %{type: :integer}, column => %{type: type}}),
          schemas: %{},
          joins: %{},
          required_filters: [{to_string(column), {:lt, "2024-01-01 00:00:00"}}]
        }
      end

      assert rows(run_domain(domain.("r40_expr", :ts, :naive_datetime))) == :column_unresolved
      assert rows(run_domain(domain.("r40_direct", :dt, :naive_datetime))) == [[1]]
      assert rows(run_domain(domain.("r40_direct", :ts, :utc_datetime))) == [[1]]

      selecto =
        domain.("r40_direct", :ts, :utc_datetime)
        |> Selecto.configure(conn, adapter: Adapter, validate: false)

      assert {:ok, prepared} = Adapter.prepare_read(selecto, conn)
      assert prepared.set.storage_column_types["r40_direct"]["ts"] == MapSet.new(["timestamp"])
      assert prepared.set.storage_column_types["r40_direct"]["dt"] == MapSet.new(["datetime"])
    end)
  end

  test "any view column in a scope compare needs a passthrough proof in every branch (R40b, R40c)" do
    with_database(fn conn ->
      Process.put(:conn, conn)

      ddl!(
        conn,
        "CREATE TABLE r40b (id INT PRIMARY KEY, ts TIMESTAMP NULL, k VARCHAR(10)) ENGINE=InnoDB"
      )

      ddl!(
        conn,
        "INSERT INTO r40b VALUES (1, '2023-12-31 22:00:00', 'a'), (2, '2024-01-01 03:00:00', 'b')"
      )

      views = [
        # Reported VARCHAR, but zone-dependent.
        {"v_fmt", "SELECT id, DATE_FORMAT(ts, '%Y-%m-%d %H') AS k FROM r40b"},
        # A UNION whose first branch is an expression.
        {"v_union",
         "SELECT id, DATE_FORMAT(ts, '%Y') AS k FROM r40b WHERE id = 1 UNION ALL SELECT id, k FROM r40b WHERE id = 2"},
        # A set operator UNION splitting does not see (R40d).
        {"v_except",
         "SELECT id, k FROM r40b WHERE id > 0 EXCEPT SELECT id, DATE_FORMAT(ts, '%Y') FROM r40b"},
        {"v_intersect",
         "SELECT id, k FROM r40b WHERE id > 0 INTERSECT SELECT id, k FROM r40b WHERE id = 1"},
        # A derived table aliased like the base table.
        {"v_derived",
         "SELECT r40b.id, r40b.k FROM (SELECT id, DATE_FORMAT(ts, '%Y') AS k FROM r40b) r40b"},
        # A literal that looks like a passthrough item (R40c addition).
        {"v_literal",
         "SELECT id, CONCAT('`x`.`r40b`.`k` AS `k`', DATE_FORMAT(ts, '%Y')) AS k FROM r40b"},
        # A backslash-escaped quote hiding a fake item (R40d).
        {"v_backslash",
         "SELECT id, 'a\\', `r40b`.`k` AS `k`, \\'b' AS junk, DATE_FORMAT(ts, '%Y') AS k FROM r40b"}
      ]

      for {name, sql} <- views, do: ddl!(conn, "CREATE VIEW #{name} AS #{sql}")
      ddl!(conn, "CREATE VIEW v_plain AS SELECT b.id, b.k FROM r40b b")

      domain = fn table ->
        %{
          name: table,
          source: relation(table, %{id: %{type: :integer}, k: %{type: :string}}),
          schemas: %{},
          joins: %{},
          required_filters: [{"k", {:gte, "2"}}]
        }
      end

      for {name, _sql} <- views do
        assert rows(run_domain(domain.(name))) == :column_unresolved, name
      end

      # A proven passthrough keeps the base column's type.
      assert rows(run_domain(domain.("v_plain"))) == [[1], [2]]

      # The caller's own filters on such a column keep working.
      result =
        domain.("v_fmt")
        |> Map.put(:required_filters, [])
        |> Selecto.configure(conn, adapter: Adapter, validate: false)
        |> Selecto.select(["id"])
        |> Selecto.filter({"k", {:gte, "2"}})
        |> Selecto.order_by(["id"])
        |> Selecto.execute()

      assert {:ok, {_rows, _columns, _aliases}} = result
    end)
  end

  test "a computed value read from a TIMESTAMP scope column is refused (R15)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE r15 (id INT PRIMARY KEY, ts TIMESTAMP NULL) ENGINE=InnoDB")
      ddl!(conn, "INSERT INTO r15 VALUES (1, '2024-01-01 03:00:00')")

      for expression <- [
            ["cast", ["field", "ts"], "string"],
            ["concat", ["literal", ""], ["field", "ts"]]
          ] do
        domain = %{
          name: "r15",
          source:
            relation("r15", %{
              id: %{type: :integer},
              ts: %{type: :utc_datetime},
              tss: %{type: :string, computed: %{kind: :expression, expression: expression}}
            }),
          schemas: %{},
          joins: %{},
          required_filters: [{"tss", {:gte, "2024-01-01"}}]
        }

        assert rows(run_domain(domain)) == :timestamp_operator, inspect(expression)
      end

      domain = %{
        name: "r15",
        source: relation("r15", %{id: %{type: :integer}, ts: %{type: :utc_datetime}}),
        schemas: %{},
        joins: %{},
        required_filters: [{"ts", {:case_insensitive_like, "2024%"}}]
      }

      assert rows(run_domain(domain)) == :timestamp_operator
    end)
  end

  test "a query member's authored filter is host scope and compares bytes (R24)" do
    with_database(fn conn ->
      Process.put(:conn, conn)
      ddl!(conn, "CREATE TABLE mroot (id INT PRIMARY KEY) ENGINE=InnoDB")

      ddl!(
        conn,
        "CREATE TABLE mbr (id INT PRIMARY KEY, org_key VARCHAR(8) NOT NULL) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO mroot VALUES (1), (2)")
      ddl!(conn, "INSERT INTO mbr VALUES (1, 't1'), (2, 'T1')")

      domain = %{
        name: "mroot",
        source: relation("mroot", %{id: %{type: :integer}}),
        schemas: %{mbr: relation("mbr", %{id: %{type: :integer}, org_key: %{type: :string}})},
        joins: %{},
        query_members: %{
          ctes: %{
            m: %{
              source: "mbr",
              query: %{"select" => ["id"], "filter" => ["eq", "org_key", "t1"]},
              join: %{owner_key: "id", related_key: "id", type: "inner"}
            }
          }
        }
      }

      result =
        domain
        |> Selecto.configure(conn, adapter: Adapter, validate: false)
        |> Selecto.with_cte(:m)
        |> Selecto.select(["id"])
        |> Selecto.order_by(["id"])
        |> Selecto.execute()

      assert rows(result) == [[1]]
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
