defmodule SelectoDBMariaDB.PostImageIntegrationTest do
  # TW-09 post-image (user decision 2026-10-24): a guarded INSERT/UPDATE's
  # stored rows must stay inside the engine tenant and the trusted/host scope
  # (`metadata.post_image`, set by SelectoUpdato), whatever the update
  # assigns, a BEFORE trigger rewrites or a generated column derives. The
  # adapter locks the update's rows, re-reads them by key after the
  # statement and rolls back unless all still match. Ordinary triggers keep
  # working. Member GROUP BY and lateral correlations pair host-exactly.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  defp update(assignments, scope, opts \\ []) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: Keyword.get(opts, :relation, "tw"),
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        assignments: Enum.map(assignments, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        expected_cardinality: {:exactly, 1},
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    command
  end

  defp insert(values, guard) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: "tw",
        assignments: Enum.map(values, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        metadata: %{
          insert_admission: %{storage_backend: :mariadb, predicate: guard, primary_key: "id"},
          post_image: %{predicate: guard, key: ["id"]}
        }
      })

    command
  end

  defp site(value), do: {:eq, {:field, "site"}, {:literal, value}}

  defp stored(conn, table \\ "tw"),
    do:
      MyXQL.query!(conn, "SELECT id, org_key, site, name FROM #{table} ORDER BY id", [],
        query_type: :text
      ).rows

  defp table!(conn) do
    ddl!(conn, "DROP TABLE IF EXISTS tw")

    ddl!(
      conn,
      "CREATE TABLE tw (id INT PRIMARY KEY, org_key VARCHAR(16), site VARCHAR(16), name VARCHAR(16), touched INT DEFAULT 0)"
    )

    ddl!(conn, "INSERT INTO tw (id, org_key, site, name) VALUES (1, 'T1', 'S1', 'a')")
  end

  test "an update that assigns the scoped column or a rewriting trigger rolls back" do
    with_database(fn conn ->
      table!(conn)

      assert {:error, %Error{details: %{code: :query_rule_violation}}} =
               Adapter.execute_write_unsafe(conn, update([{"site", "S9"}], site("S1")), [])

      assert stored(conn) == [[1, "T1", "S1", "a"]]

      # Exact: 's1' is not 'S1' though the collation says so.
      assert {:error, %Error{details: %{code: :query_rule_violation}}} =
               Adapter.execute_write_unsafe(conn, update([{"site", "s1"}], site("S1")), [])

      ddl!(
        conn,
        "CREATE TRIGGER tw_bu BEFORE UPDATE ON tw FOR EACH ROW SET NEW.site = LOWER(NEW.site)"
      )

      assert {:error, %Error{details: %{code: :query_rule_violation}}} =
               Adapter.execute_write_unsafe(conn, update([{"name", "u"}], site("S1")), [])

      assert stored(conn) == [[1, "T1", "S1", "a"]]

      ddl!(
        conn,
        "CREATE TRIGGER tw_bi BEFORE INSERT ON tw FOR EACH ROW SET NEW.site = LOWER(NEW.site)"
      )

      # A rewritten insert candidate is not evaluable (typed_insert_admission
      # TA005); either way nothing is stored.
      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               Adapter.execute_write_unsafe(
                 conn,
                 insert(
                   [{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", "n"}],
                   site("S1")
                 ),
                 []
               )

      assert stored(conn) == [[1, "T1", "S1", "a"]]
    end)
  end

  test "a guarded insert or tenant upsert on a table with a BEFORE INSERT trigger is refused" do
    # w3exrev15: a BEFORE INSERT trigger can rewrite the key itself, so the
    # stored row is not found under the key the statement reported.
    with_database(fn conn ->
      table!(conn)

      ddl!(
        conn,
        "CREATE TRIGGER tw_pk BEFORE INSERT ON tw FOR EACH ROW BEGIN IF NEW.name = 'evil' THEN SET NEW.id = NEW.id + 100; SET NEW.org_key = 'T8'; SET NEW.site = 'S8'; END IF; END"
      )

      for name <- ["evil", "plain"] do
        assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
                 Adapter.execute_write_unsafe(
                   conn,
                   insert(
                     [{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", name}],
                     site("S1")
                   ),
                   []
                 )
      end

      ddl!(
        conn,
        "CREATE TABLE tu (id INT PRIMARY KEY, org_key VARCHAR(16) NOT NULL, code VARCHAR(16) NOT NULL, name VARCHAR(16), UNIQUE KEY tc (org_key, code)) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO tu VALUES (1, 'T1', 'a', 'x')")

      upsert = %Command{
        operation: :upsert,
        relation: "tu",
        assignments: [
          %{field: "id", value: {:literal, 2}},
          %{field: "org_key", value: {:literal, "T1"}},
          %{field: "code", value: {:literal, "a"}},
          %{field: "name", value: {:literal, "evil"}}
        ],
        predicate: nil,
        returning: :none,
        expected_cardinality: {:exactly, 1},
        metadata: %{
          conflict_target: ["org_key", "code"],
          declared_conflict_targets: [["org_key", "code"]],
          upsert_update_fields: ["name"],
          tenant_fields: ["org_key"],
          field_types: %{"org_key" => :string},
          post_image: %{predicate: {:eq, {:field, "org_key"}, {:literal, "T1"}}, key: ["id"]}
        }
      }

      assert {:ok, %{affected_rows: _}} = Adapter.execute_write_unsafe(conn, upsert, [])

      ddl!(
        conn,
        "CREATE TRIGGER tu_bu BEFORE UPDATE ON tu FOR EACH ROW SET NEW.org_key = LOWER(NEW.org_key)"
      )

      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               Adapter.execute_write_unsafe(conn, upsert, [])

      assert MyXQL.query!(conn, "SELECT id, org_key, code, name FROM tu ORDER BY id", [],
               query_type: :text
             ).rows ==
               [[1, "T1", "a", "evil"]]

      assert stored(conn) == [[1, "T1", "S1", "a"]]
    end)
  end

  test "a trigger created while the write waits for the metadata lock is seen" do
    # w3exrev16 item 11, three connections: C holds a shared metadata lock,
    # B's CREATE TRIGGER queues behind it, and the write's catalog checks
    # must wait behind B (they take the lock first) instead of reading the
    # catalog before the trigger exists.
    with_database(fn conn ->
      table!(conn)

      ddl!(
        conn,
        "CREATE TABLE tu (id INT PRIMARY KEY, org_key VARCHAR(16) NOT NULL, code VARCHAR(16) NOT NULL, name VARCHAR(16), UNIQUE KEY tc (org_key, code)) ENGINE=InnoDB"
      )

      ddl!(conn, "INSERT INTO tu VALUES (1, 'T1', 'a', 'x')")
      [[database]] = MyXQL.query!(conn, "SELECT DATABASE()", [], query_type: :text).rows
      options = Keyword.put(connection_options(), :database, database)
      {:ok, b} = MyXQL.start_link(options)
      {:ok, c} = MyXQL.start_link(options)

      race = fn table, trigger, write ->
        parent = self()

        {:ok, _} =
          MyXQL.transaction(
            c,
            fn tx ->
              MyXQL.query!(tx, "SELECT * FROM #{table} LIMIT 0", [], query_type: :text)
              tb = Task.async(fn -> MyXQL.query(b, trigger, [], query_type: :text) end)
              Process.sleep(500)
              ta = Task.async(fn -> Adapter.execute_write_unsafe(conn, write, []) end)
              Process.sleep(1_500)
              send(parent, {:tasks, ta, tb})
            end,
            timeout: 30_000
          )

        assert_receive {:tasks, ta, tb}
        assert {:ok, _} = Task.await(tb, 30_000)
        Task.await(ta, 30_000)
      end

      evil =
        "BEGIN IF NEW.name = 'evil' THEN SET NEW.id = NEW.id + 100; SET NEW.org_key = 'T8'; END IF; END"

      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               race.(
                 "tw",
                 "CREATE TRIGGER tw_race BEFORE INSERT ON tw FOR EACH ROW " <> evil,
                 insert(
                   [{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", "evil"}],
                   site("S1")
                 )
               )

      assert stored(conn) == [[1, "T1", "S1", "a"]]

      upsert = %Command{
        operation: :upsert,
        relation: "tu",
        assignments: [
          %{field: "id", value: {:literal, 2}},
          %{field: "org_key", value: {:literal, "T1"}},
          %{field: "code", value: {:literal, "b"}},
          %{field: "name", value: {:literal, "evil"}}
        ],
        predicate: nil,
        returning: :none,
        expected_cardinality: {:exactly, 1},
        metadata: %{
          conflict_target: ["org_key", "code"],
          declared_conflict_targets: [["org_key", "code"]],
          upsert_update_fields: ["name"],
          tenant_fields: ["org_key"],
          field_types: %{"org_key" => :string},
          post_image: %{predicate: {:eq, {:field, "org_key"}, {:literal, "T1"}}, key: ["id"]}
        }
      }

      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               race.(
                 "tu",
                 "CREATE TRIGGER tu_race BEFORE INSERT ON tu FOR EACH ROW " <> evil,
                 upsert
               )

      assert MyXQL.query!(conn, "SELECT id, org_key, code FROM tu ORDER BY id", [],
               query_type: :text
             ).rows ==
               [[1, "T1", "a"]]

      GenServer.stop(b)
      GenServer.stop(c)
    end)
  end

  test "inside the caller's transaction a refused write undoes its own DML" do
    # w3exrev16 item 18: the caller ignores the refusal and commits.
    with_database(fn conn ->
      table!(conn)

      assert {:ok, {refused, applied}} =
               MyXQL.transaction(conn, fn tx ->
                 refused =
                   Adapter.execute_write_unsafe(tx, update([{"site", "S9"}], site("S1")), [])

                 applied =
                   Adapter.execute_write_unsafe(tx, update([{"name", "u"}], site("S1")), [])

                 {refused, applied}
               end)

      assert {:error, %Error{details: %{code: :query_rule_violation}}} = refused
      assert {:ok, %{affected_rows: 1}} = applied
      assert stored(conn) == [[1, "T1", "S1", "u"]]
    end)
  end

  test "the write's savepoint never replaces or releases a host savepoint" do
    # w3exp5 item 20: MySQL replaces a same-named savepoint, so a counter-
    # named write savepoint could replace the host's and then release it.
    # The host holds savepoints over the next counter values; every one must
    # survive the writes.
    with_database(fn conn ->
      table!(conn)
      base = System.unique_integer([:positive])
      names = Enum.map(base..(base + 400), &"selecto_write_#{&1}")

      assert {:ok, :ok} =
               MyXQL.transaction(
                 conn,
                 fn tx ->
                   for name <- names,
                       do: MyXQL.query!(tx, "SAVEPOINT #{name}", [], query_type: :text)

                   assert {:error, %Error{details: %{code: :query_rule_violation}}} =
                            Adapter.execute_write_unsafe(
                              tx,
                              update([{"site", "S9"}], site("S1")),
                              []
                            )

                   assert {:ok, _} =
                            Adapter.execute_write_unsafe(
                              tx,
                              update([{"name", "u"}], site("S1")),
                              []
                            )

                   for name <- Enum.reverse(names),
                       do:
                         MyXQL.query!(tx, "ROLLBACK TO SAVEPOINT #{name}", [], query_type: :text)

                   :ok
                 end,
                 timeout: 120_000
               )

      assert stored(conn) == [[1, "T1", "S1", "a"]]
    end)
  end

  test "ordinary triggers keep working under a scope" do
    with_database(fn conn ->
      table!(conn)
      ddl!(conn, "CREATE TABLE tw_audit (id INT, at_name VARCHAR(16))")

      ddl!(
        conn,
        "CREATE TRIGGER tw_touch BEFORE UPDATE ON tw FOR EACH ROW SET NEW.touched = OLD.touched + 1"
      )

      ddl!(
        conn,
        "CREATE TRIGGER tw_audit_i AFTER INSERT ON tw FOR EACH ROW INSERT INTO tw_audit VALUES (NEW.id, NEW.name)"
      )

      assert {:ok, %{affected_rows: 1}} =
               Adapter.execute_write_unsafe(conn, update([{"name", "u"}], site("S1")), [])

      assert {:ok, %{affected_rows: 1}} =
               Adapter.execute_write_unsafe(
                 conn,
                 insert(
                   [{"id", 2}, {"org_key", "T1"}, {"site", "S1"}, {"name", "n"}],
                   site("S1")
                 ),
                 []
               )

      assert stored(conn) == [[1, "T1", "S1", "u"], [2, "T1", "S1", "n"]]

      assert MyXQL.query!(conn, "SELECT touched FROM tw WHERE id = 1", [], query_type: :text).rows ==
               [[1]]

      assert MyXQL.query!(conn, "SELECT * FROM tw_audit", [], query_type: :text).rows == [
               [2, "n"]
             ]
    end)
  end

  test "a generated scope column, a non-unique key and a view are covered or refused" do
    with_database(fn conn ->
      ddl!(
        conn,
        "CREATE TABLE gc (id INT PRIMARY KEY, site VARCHAR(16), g VARCHAR(16) GENERATED ALWAYS AS (UPPER(site)) STORED, name VARCHAR(16))"
      )

      ddl!(conn, "INSERT INTO gc (id, site, name) VALUES (1, 's1', 'a')")
      g = {:eq, {:field, "g"}, {:literal, "S1"}}

      assert {:error, %Error{details: %{code: :query_rule_violation}}} =
               Adapter.execute_write_unsafe(conn, update([{"site", "s9"}], g, relation: "gc"), [])

      ddl!(
        conn,
        "CREATE TABLE dk (id INT NOT NULL, site VARCHAR(16), name VARCHAR(16), KEY (id))"
      )

      ddl!(conn, "INSERT INTO dk VALUES (1, 'S1', 'x'), (1, 'S2', 'y')")

      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               Adapter.execute_write_unsafe(
                 conn,
                 update([{"name", "u"}], site("S1"), relation: "dk"),
                 []
               )

      table!(conn)
      ddl!(conn, "CREATE VIEW tw_v AS SELECT * FROM tw")

      assert {:error, %Error{details: %{code: :query_rule_not_evaluable}}} =
               Adapter.execute_write_unsafe(
                 conn,
                 update([{"name", "u"}], site("S1"), relation: "tw_v"),
                 []
               )

      assert stored(conn) == [[1, "T1", "S1", "a"]]
    end)
  end

  test "member GROUP BY keys pair host-exactly" do
    with_database(fn conn ->
      ddl!(conn, "CREATE TABLE mg_root (id INT PRIMARY KEY, org_key VARCHAR(16))")
      ddl!(conn, "INSERT INTO mg_root VALUES (1, 't1')")
      ddl!(conn, "CREATE TABLE mg_use (id INT PRIMARY KEY, org_key VARCHAR(16), amount INT)")
      ddl!(conn, "INSERT INTO mg_use VALUES (1,'t1',1),(2,'T1',10),(3,'t1 ',100),(4,'tı',1000)")

      for {root_tf, required, tenant} <- [{:org_key, [], "t1"}, {nil, [{"org_key", "t1"}], nil}],
          {label, use} <- members() do
        base =
          member_domain(root_tf, required)
          |> Selecto.configure(conn, adapter: Adapter, validate: false)

        base = if tenant, do: Selecto.with_tenant(base, tenant), else: base

        assert rows(use.(base)) |> Enum.map(fn [id, s] -> [id, Decimal.new(s)] end) ==
                 [[1, Decimal.new(1)]],
               "#{label} #{inspect(root_tf)}"
      end
    end)
  end

  defp members do
    [
      {"cte", &(&1 |> Selecto.with_cte(:tot) |> Selecto.select(["id", "tot.s"]))},
      # MariaDB has no LATERAL and refuses lateral members.
      {"aliased cte", &(&1 |> Selecto.with_cte(:ali) |> Selecto.select(["id", "ali.s"]))}
    ]
  end

  defp member_domain(root_tf, required) do
    rel = fn table, cols, tf ->
      %{
        source_table: table,
        primary_key: :id,
        fields: cols |> Map.keys() |> Enum.sort(),
        redact_fields: [],
        columns: Map.new(cols, fn {n, t} -> {n, %{type: t}} end),
        associations: %{}
      }
      |> then(&if(tf, do: Map.put(&1, :tenant_field, tf), else: &1))
    end

    sum = %{"as" => "s", "aggregate" => "sum", "field" => "amount"}

    %{
      name: "MG",
      source: rel.("mg_root", %{id: :integer, org_key: :string}, root_tf),
      schemas: %{use: rel.("mg_use", %{id: :integer, org_key: :string, amount: :integer}, nil)},
      joins: %{},
      required_filters: required,
      query_members: %{
        ctes: %{
          tot: %{
            source: "use",
            query: %{"select" => ["org_key", sum], "group_by" => ["org_key"]},
            join: %{owner_key: "org_key", related_key: "org_key", type: "left"}
          },
          ali: %{
            source: "use",
            query: %{
              "select" => [%{"as" => "k", "value" => ["field", "org_key"]}, sum],
              "group_by" => ["org_key"]
            },
            join: %{owner_key: "org_key", related_key: "k", type: "left"}
          }
        },
        laterals: %{
          lat: %{
            source: "use",
            query: %{"select" => [sum]},
            correlations: %{org_key: "org_key"},
            join_type: "left"
          }
        }
      }
    }
  end

  defp ids(query), do: query |> rows() |> Enum.map(&hd/1) |> Enum.sort()

  defp rows(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_postimg_" <>
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
