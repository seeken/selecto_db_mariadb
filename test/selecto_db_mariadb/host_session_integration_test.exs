defmodule SelectoDBMariaDB.HostSessionIntegrationTest do
  # What a host's session keeps when Selecto runs on it: completion_type,
  # autocommit=0, an open transaction, table locks, and a read that writes
  # (tenth review H1, H2, H3, M1, M3).
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter, as: Adapter
  alias SelectoDBMariaDB.Session, as: Session
  alias Selecto.Write.{Batch, Command}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  defp opts(extra \\ []) do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "localhost"),
      port: String.to_integer(System.get_env("SELECTO_MARIADB_PORT", "3306")),
      username:
        System.get_env("SELECTO_MARIADB_USERNAME") ||
          System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.get_env("SELECTO_MARIADB_PASSWORD", ""),
      database: System.get_env("SELECTO_MARIADB_DATABASE", "selecto_test"),
      pool_size: 1
    ]
    |> Keyword.merge(extra)
  end

  defp open(extra \\ []) do
    {:ok, conn} = Adapter.connect(opts(extra))
    Process.unlink(conn)
    conn
  end

  defp sql(conn, statement) do
    case MyXQL.query(conn, statement, [], query_type: :text) do
      {:ok, %MyXQL.Result{rows: nil, num_rows: count}} -> count
      {:ok, %MyXQL.Result{rows: rows}} -> rows
      {:error, %MyXQL.Error{mysql: %{code: code}}} -> {:error, code}
    end
  end

  defp id(conn), do: conn |> sql("SELECT CONNECTION_ID()") |> hd() |> hd()
  defp close(conn), do: GenServer.stop(conn, :normal, 5_000)

  setup do
    other = open()

    for statement <- [
          "DROP VIEW IF EXISTS hs_writing",
          "DROP VIEW IF EXISTS hs_slow",
          "DROP FUNCTION IF EXISTS hs_write",
          "DROP FUNCTION IF EXISTS hs_sleep",
          "DROP TABLE IF EXISTS hs_t",
          "DROP TABLE IF EXISTS hs_log",
          "CREATE TABLE hs_t (id INT PRIMARY KEY, org VARCHAR(20) NOT NULL, v INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
          "CREATE TABLE hs_log (hit VARCHAR(40)) ENGINE=InnoDB",
          "INSERT INTO hs_t VALUES (1,'t1',0),(2,'t1',0),(3,'t2',0)",
          "CREATE FUNCTION hs_write() RETURNS INT MODIFIES SQL DATA DETERMINISTIC " <>
            "BEGIN INSERT INTO hs_log VALUES ('view'); RETURN 1; END",
          "CREATE FUNCTION hs_sleep() RETURNS INT DETERMINISTIC NO SQL BEGIN DO SLEEP(1); RETURN 1; END",
          "CREATE VIEW hs_writing AS SELECT id, org, v FROM hs_t WHERE hs_write() = 1",
          "CREATE VIEW hs_slow AS SELECT id, org, v FROM hs_t WHERE id = 1 AND hs_sleep() = 1"
        ] do
      sql(other, statement)
    end

    on_exit(fn ->
      cleanup = open()

      for statement <- [
            "DROP VIEW IF EXISTS hs_writing",
            "DROP VIEW IF EXISTS hs_slow",
            "DROP FUNCTION IF EXISTS hs_write",
            "DROP FUNCTION IF EXISTS hs_sleep",
            "DROP TABLE IF EXISTS hs_t",
            "DROP TABLE IF EXISTS hs_log"
          ] do
        sql(cleanup, statement)
      end

      close(cleanup)
    end)

    {:ok, other: other}
  end

  defp domain(table \\ "hs_t") do
    %{
      name: table,
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :org, :v],
        redact_fields: [],
        columns: %{id: %{type: :integer}, org: %{type: :string}, v: %{type: :integer}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp selecto(conn, table \\ "hs_t") do
    domain(table)
    |> Selecto.configure(conn, adapter: Adapter, validate: false)
    |> Selecto.select(["id"])
    |> Selecto.order_by(["id"])
  end

  defp read(conn, table \\ "hs_t"), do: Selecto.execute(selecto(conn, table))

  defp stream(conn, table \\ "hs_t") do
    case Selecto.execute_stream(selecto(conn, table)) do
      {:ok, rows} -> {:ok, Enum.map(rows, &elem(&1, 0))}
      other -> other
    end
  end

  defp command(operation, fields) do
    {:ok, command} =
      Command.new(
        Map.merge(
          %{
            operation: operation,
            relation: :hs_t,
            assignments: [],
            predicate: nil,
            expected_cardinality: {:exactly, 1},
            returning: :none,
            metadata: %{}
          },
          Map.new(fields)
        )
      )

    command
  end

  defp insert(id) do
    command(:insert,
      assignments: [
        %{field: :id, value: {:literal, id}},
        %{field: :org, value: {:literal, "t1"}},
        %{field: :v, value: {:literal, 7}}
      ]
    )
  end

  defp update(id, value) do
    command(:update,
      assignments: [%{field: :v, value: {:literal, value}}],
      predicate: {:eq, {:field, :id}, {:literal, id}}
    )
  end

  defp batch(commands) do
    {:ok, batch} = Batch.new(commands)
    batch
  end

  defp endings do
    [
      write: &Adapter.execute_write_unsafe(&1, insert(10), []),
      batch: &Adapter.execute_write_unsafe(&1, batch([insert(11), update(2, 6)]), []),
      refused_write: &Adapter.execute_write_unsafe(&1, update(999, 5), []),
      refused_batch: &Adapter.execute_write_unsafe(&1, batch([insert(12), update(999, 6)]), []),
      transaction_ok: fn conn ->
        Adapter.transaction(
          conn,
          fn tx ->
            sql(tx, "INSERT INTO hs_log VALUES ('tx')")
            {:ok, 1}
          end,
          []
        )
      end,
      transaction_error: fn conn ->
        Adapter.transaction(
          conn,
          fn tx ->
            sql(tx, "INSERT INTO hs_log VALUES ('tx')")
            {:error, :stop}
          end,
          []
        )
      end,
      selecto_transaction: fn conn ->
        Selecto.transaction(selecto(conn), fn scoped -> Selecto.execute(scoped) end)
      end,
      read: &read/1,
      stream: &stream/1
    ]
  end

  for completion_type <- [0, 1, 2] do
    test "every ending leaves completion_type #{completion_type} out of it", %{other: other} do
      for {name, ending} <- endings() do
        sql(other, "DELETE FROM hs_log")
        conn = open()
        sql(conn, "SET SESSION completion_type = #{unquote(completion_type)}")
        before = id(conn)

        _ = ending.(conn)

        assert id(conn) == before, "#{name} closed the connection"
        assert sql(conn, "INSERT INTO hs_log VALUES ('host')") == 1
        assert id(conn) == before
        close(conn)

        assert sql(other, "SELECT COUNT(*) FROM hs_log WHERE hit = 'host'") == [[1]],
               "#{name} left the host's next write in a transaction that closing lost"
      end
    end
  end

  test "a transaction a pooled connection inherits has no stale snapshot", %{other: other} do
    conn = open()
    sql(conn, "SET SESSION completion_type = 1")

    assert {:ok, _} = Adapter.execute_write_unsafe(conn, update(1, 11), [])
    assert {:ok, {[[1], [2], [3]], _, _}} = read(conn)

    sql(other, "INSERT INTO hs_t VALUES (4,'t1',0)")

    assert {:ok, {[[1], [2], [3], [4]], _, _}} = read(conn)
    assert {:ok, _} = Adapter.execute_write_unsafe(conn, update(1, 12), [])
    assert DBConnection.run(conn, &DBConnection.status/1) == :idle
    close(conn)
    assert sql(other, "SELECT v FROM hs_t WHERE id = 1") == [[12]]
  end

  test "reads at autocommit=0 keep the connection and its variables" do
    conn = open()
    sql(conn, "SET SESSION autocommit = 0")
    sql(conn, "SET @hs_host = 42")
    before = id(conn)

    assert {:ok, {[[1], [2], [3]], _, _}} = read(conn)
    assert {:ok, [[1], [2], [3]]} = stream(conn)
    assert id(conn) == before
    assert sql(conn, "SELECT @hs_host, @@session.autocommit") == [[42, 0]]
    close(conn)
  end

  test "hand-built text on a read path must be a read (R68)", %{other: other} do
    conn = open()
    read = &Adapter.execute(&1, &2, [], selecto_read: true)

    for text <- [
          "COMMIT",
          "ROLLBACK",
          "DELETE FROM hs_t",
          "SET @x = 1",
          "/* SELECT */ COMMIT",
          "/*! DELETE FROM hs_t */",
          "-- SELECT\nCOMMIT",
          "# SELECT\nCOMMIT",
          "(COMMIT)",
          "SELECTED",
          ""
        ] do
      assert {:error, %{details: %{reason: :hand_built_not_a_read}}} = read.(conn, text), text
    end

    assert {:ok, %{rows: [[1]]}} = read.(conn, "/* a */ -- b\n ((SELECT 1))")
    assert {:ok, %{rows: [["delete"]]}} = read.(conn, "SELECT 'delete'")

    # In a host transaction nothing stops a write, and the read stays as written.
    sql(conn, "START TRANSACTION")
    sql(conn, "INSERT INTO hs_log VALUES ('host')")
    assert {:error, %{details: %{reason: :hand_built_not_a_read}}} = read.(conn, "COMMIT")

    assert {:error, %{details: %{reason: :hand_built_write}}} =
             read.(conn, "WITH x AS (SELECT 1 AS id) DELETE hs_t FROM hs_t JOIN x USING (id)")

    assert {:error, %{details: %{reason: :hand_built_row_lock}}} =
             read.(conn, "WITH x AS (SELECT 1 AS id) SELECT id FROM x FOR UPDATE")

    assert {:ok, %{rows: [["update"]]}} =
             read.(conn, "WITH x AS (SELECT 'update' AS id) SELECT id FROM x")

    sql(conn, "ROLLBACK")
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
    assert sql(other, "SELECT COUNT(*) FROM hs_t") == [[3]]
    close(conn)
  end

  test "a raw stream is classified before the statement prefix (M5)" do
    conn = open()

    domain = %{
      name: "D",
      source: %{
        source_table: "hs_t",
        primary_key: :id,
        fields: [:id],
        redact_fields: [],
        columns: %{id: %{type: :integer}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    assert {:ok, stream} =
             domain
             |> Selecto.configure(conn, adapter: Adapter, validate: false)
             |> Selecto.select(["id"])
             |> Selecto.filter({:raw_sql_filter, "selecto_root.id > 0"})
             |> Selecto.execute_stream()

    assert length(Enum.to_list(stream)) == 3
    close(conn)
  end

  test "the server says whether hand-built WITH writes: no lexer decides (R72)", %{other: other} do
    for {setup, text} <- [
          {"SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')",
           "WITH c AS (SELECT 'a\\' AS x) DELETE FROM hs_t WHERE 'b' = 'b'"},
          {"SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')",
           "WITH c AS (SELECT 1 AS \"a'\") DELETE FROM hs_t WHERE 1 = 1 OR 'x' = 'y'"},
          {"DO 0", "WITH c AS (SELECT 1 AS `a\\`) DELETE FROM hs_t"},
          {"DO 0", "WITH c AS (SELECT 1 AS a) /*!80000 DELETE FROM hs_t */"},
          {"DO 0", "WITH c AS (SELECT 1 AS a) /*!50000 UPDATE hs_t SET v = 1 */"}
        ] do
      conn = open()
      sql(conn, setup)
      sql(conn, "START TRANSACTION")

      assert {:error, %{details: %{reason: :hand_built_write}}} =
               Adapter.execute(conn, text, [], selecto_read: true),
             text

      sql(conn, "COMMIT AND NO CHAIN NO RELEASE")
      close(conn)
      assert sql(other, "SELECT COUNT(*), SUM(v) FROM hs_t") in [[[3, 0]], [[3, Decimal.new(0)]]]
    end
  end

  test "what only reports answers as the driver does (R71)" do
    conn = open()

    for text <- [
          "SHOW VARIABLES LIKE 'autocommit'",
          "EXPLAIN SELECT 1",
          "DESCRIBE hs_t",
          "DESC hs_t",
          "VALUES ROW(1)"
        ],
        text != "VALUES ROW(1)" or String.contains?(inspect(Adapter), "MySQL") do
      assert {:ok, %{rows: [_ | _]}} = Adapter.execute(conn, text, [], selecto_read: true), text
    end

    sql(conn, "START TRANSACTION")

    assert {:ok, %{rows: [_ | _]}} =
             Adapter.execute(conn, "EXPLAIN SELECT id FROM hs_t", [], selecto_read: true)

    assert {:ok, %{rows: [_ | _]}} =
             Adapter.execute(conn, "ANALYZE SELECT id FROM hs_t", [], selecto_read: true)

    assert {:error, %{details: %{reason: reason}}} =
             Adapter.execute(conn, "ANALYZE DELETE FROM hs_t", [], selecto_read: true)

    assert reason in [:hand_built_write, :hand_built_not_a_read]

    sql(conn, "ROLLBACK AND NO CHAIN NO RELEASE")
    close(conn)
  end

  test "hand-built row locks refuse, however they are written (R72)" do
    conn = open()

    for text <- [
          "SELECT id FROM hs_t FOR UPDATE",
          "SELECT id FROM hs_t FOR SHARE",
          "SELECT id FROM hs_t LOCK IN SHARE MODE",
          "SELECT id FROM hs_t /*!50000 FOR UPDATE */",
          "SELECT id FROM hs_t FOR/* */UPDATE"
        ],
        setup <- ["DO 0", "START TRANSACTION"] do
      sql(conn, setup)

      assert {:error, %{details: %{reason: :hand_built_row_lock}}} =
               Adapter.execute(conn, text, [], selecto_read: true),
             text

      sql(conn, "ROLLBACK AND NO CHAIN NO RELEASE")
    end

    close(conn)
  end

  test "held locks stay when no probe of a name can tell (no default database)", %{other: other} do
    {:ok, conn} = Adapter.connect(opts() |> Keyword.delete(:database))
    Process.unlink(conn)
    [[database]] = sql(other, "SELECT DATABASE()")
    sql(conn, "LOCK TABLES #{database}.hs_t READ")

    assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, "SELECT 1", [], selecto_read: true)

    assert {:error, %{details: %{reason: :host_table_locks}}} =
             Adapter.transaction(conn, fn _ -> {:ok, 1} end, [])

    sql(other, "SET SESSION lock_wait_timeout = 1")
    assert sql(other, "INSERT INTO hs_t VALUES (77, 't1', 0)") == {:error, 1205}
    sql(conn, "UNLOCK TABLES")
    close(conn)
  end

  test "a failed undo in the host's own transaction closes, and sends no ROLLBACK (R72)",
       %{other: other} do
    conn = open()

    DBConnection.run(conn, fn held ->
      assert {:ok, _} = MyXQL.query(held, "START TRANSACTION", [], query_type: :text)

      assert {:ok, _} =
               MyXQL.query(held, "INSERT INTO hs_log VALUES ('host')", [], query_type: :text)

      assert :ok = Session.fail_transaction(held)

      assert {:error, %DBConnection.ConnectionError{}} =
               MyXQL.query(held, "COMMIT AND NO CHAIN NO RELEASE", [], query_type: :text)
    end)

    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
    close(conn)
  end

  test "a scope compare on a numeric view column answers without a proof (R40d)", %{other: other} do
    for statement <- [
          "DROP VIEW IF EXISTS hs_un, hs_nested, hs_cast, hs_comp",
          "DROP TABLE IF EXISTS hs_b",
          "CREATE TABLE hs_b (id INT PRIMARY KEY, org INT NOT NULL) ENGINE=InnoDB",
          "INSERT INTO hs_b VALUES (1, 7), (2, 8)",
          "CREATE VIEW hs_comp AS SELECT id, org + 0 AS tid FROM hs_b",
          "CREATE VIEW hs_cast AS SELECT id, CAST(org AS SIGNED) AS tid FROM hs_b",
          "CREATE VIEW hs_nested AS SELECT id, tid FROM hs_comp",
          "CREATE VIEW hs_un AS SELECT id, org AS tid FROM hs_b UNION ALL SELECT id, org FROM hs_b"
        ],
        do: sql(other, statement)

    conn = open()

    read = fn view ->
      %{
        name: "D",
        source: %{
          source_table: view,
          primary_key: :id,
          tenant_field: :tid,
          fields: [:id, :tid],
          redact_fields: [],
          columns: %{id: %{type: :integer}, tid: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      |> Selecto.configure(conn, adapter: Adapter, validate: false)
      |> Selecto.with_tenant(7)
      |> Selecto.select(["id"])
      |> Selecto.execute()
    end

    # MariaDB's derived column types prove nothing: these refuse, by design.
    for view <- ~w(hs_comp hs_cast hs_nested hs_un) do
      assert {:error, %{details: %{reason: :column_unresolved}}} = read.(view), view
    end

    close(conn)
    sql(other, "DROP VIEW hs_un, hs_nested, hs_cast, hs_comp")
    sql(other, "DROP TABLE hs_b")
  end

  test "a read at autocommit=0 keeps its snapshot until the host ends it", %{other: other} do
    conn = open()
    sql(conn, "SET SESSION autocommit = 0")

    assert {:ok, {[[1], [2], [3]], _, _}} = read(conn)
    sql(other, "INSERT INTO hs_t VALUES (9, 't1', 0)")
    assert {:ok, {[[1], [2], [3]], _, _}} = read(conn)
    assert {:ok, [[1], [2], [3]]} = stream(conn)

    sql(conn, "COMMIT")
    assert {:ok, {[[1], [2], [3], [9]], _, _}} = read(conn)
    close(conn)
  end

  test "a raw statement at autocommit=0 keeps the connection and the host's transaction",
       %{other: other} do
    conn = open()
    sql(conn, "SET SESSION autocommit = 0")
    before = id(conn)

    assert {:ok, _} = Adapter.execute(conn, "INSERT INTO hs_log VALUES ('host')", [], [])
    assert id(conn) == before
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
    sql(conn, "COMMIT")
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[1]]
    close(conn)
  end

  test "a write on a pid in a host transaction refuses and leaves it alone", %{other: other} do
    for setup <- ["START TRANSACTION", "SET SESSION autocommit = 0"] do
      sql(other, "DELETE FROM hs_log")
      conn = open()
      sql(conn, setup)
      sql(conn, "INSERT INTO hs_log VALUES ('a')")
      before = id(conn)

      for write <- [update(1, 5), batch([update(1, 5), update(2, 5)])] do
        assert {:error, %{details: %{reason: :unowned_transaction}}} =
                 Adapter.execute_write_unsafe(conn, write, [])
      end

      assert {:error, %{details: %{reason: :unowned_transaction}}} =
               Adapter.transaction(conn, fn _tx -> {:ok, 1} end, [])

      assert id(conn) == before
      sql(conn, "INSERT INTO hs_log VALUES ('b')")
      sql(conn, "COMMIT")
      close(conn)

      assert sql(other, "SELECT hit FROM hs_log ORDER BY hit") == [["a"], ["b"]]
      assert sql(other, "SELECT v FROM hs_t WHERE id = 1") == [[0]]
    end
  end

  test "an idle read or stream of a writing view refuses and commits nothing", %{other: other} do
    conn = open()

    assert {:error, %{details: %{reason: :read_only_read}}} = read(conn, "hs_writing")
    assert {:error, %{details: %{reason: :read_only_read}}} = stream(conn, "hs_writing")
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
    assert DBConnection.run(conn, &DBConnection.status/1) == :idle
    assert sql(conn, "INSERT INTO hs_log VALUES ('host')") == 1
    close(conn)
    assert sql(other, "SELECT hit FROM hs_log") == [["host"]]
  end

  test "a failed or table-less read leaves no read-only state behind" do
    conn = open()

    assert {:error, _} = read(conn, "hs_absent")
    assert sql(conn, "INSERT INTO hs_log VALUES ('host')") == 1
    assert {:ok, _} = Adapter.execute_write_unsafe(conn, update(1, 5), [])
    close(conn)
  end

  test "inside a host transaction a read runs as written and the host undoes it",
       %{other: other} do
    conn = open()
    sql(conn, "START TRANSACTION")

    assert {:ok, {[[1], [2], [3]], _, _}} = read(conn, "hs_writing")
    assert sql(conn, "SELECT COUNT(*) FROM hs_log") == [[1]]
    sql(conn, "ROLLBACK")
    close(conn)
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
  end

  test "reads keep LOCK TABLES and FLUSH TABLES t WITH READ LOCK", %{other: other} do
    sql(other, "SET SESSION lock_wait_timeout = 1")

    for autocommit <- [1, 0] do
      conn = open()
      sql(conn, "SET SESSION autocommit = #{autocommit}")
      sql(conn, "LOCK TABLES hs_t WRITE, hs_t AS selecto_root WRITE")
      before = id(conn)

      assert {:ok, {[[1], [2], [3]], _, _}} = read(conn)
      assert {:ok, [[1], [2], [3]]} = stream(conn)
      assert id(conn) == before
      assert sql(other, "LOCK TABLES hs_t WRITE") == {:error, 1205}
      sql(conn, "UNLOCK TABLES")
      close(conn)

      conn = open()
      sql(conn, "SET SESSION autocommit = #{autocommit}")
      sql(conn, "FLUSH TABLES hs_t WITH READ LOCK")
      before = id(conn)

      assert {:error, _} = read(conn)
      assert id(conn) == before
      assert sql(other, "INSERT INTO hs_t VALUES (77,'x',0)") == {:error, 1205}
      sql(conn, "UNLOCK TABLES")
      close(conn)
    end
  end

  test "a write or transaction under table locks refuses and keeps them", %{other: other} do
    sql(other, "SET SESSION lock_wait_timeout = 1")
    conn = open()
    sql(conn, "LOCK TABLES hs_t WRITE")

    assert {:error, %{details: %{reason: :host_table_locks}}} =
             Adapter.execute_write_unsafe(conn, update(1, 5), [])

    assert {:error, %{details: %{reason: :host_table_locks}}} =
             Adapter.transaction(conn, fn _tx -> {:ok, 1} end, [])

    assert sql(other, "LOCK TABLES hs_t WRITE") == {:error, 1205}
    sql(conn, "UNLOCK TABLES")
    close(conn)
  end

  test "the lock probe runs no code of a view", %{other: other} do
    conn = open()

    assert Session.host_locks(conn, ["`hs_writing`"]) == :none
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[0]]
    close(conn)
  end

  test "a row-locking read gets a transaction it can run in, ended by ROLLBACK" do
    conn = open()

    assert {:ok, %{rows: [[1]]}} =
             Adapter.execute(conn, "SELECT id FROM hs_t WHERE id = 1 FOR UPDATE", [],
               selecto_read: true,
               selecto_generated: true
             )

    assert DBConnection.run(conn, &DBConnection.status/1) == :idle
    close(conn)
  end

  test "KILL QUERY on a read reaches the host as a cancellation", %{other: other} do
    conn = open()
    victim = id(conn)
    task = Task.async(fn -> Selecto.execute(selecto(conn, "hs_slow"), timeout: 8_000) end)
    Process.sleep(300)
    sql(other, "KILL QUERY #{victim}")

    assert {:error, %{details: %{category: :query_canceled, code: 1317}}} =
             Task.await(task, 10_000)

    assert id(conn) == victim
    close(conn)
  end

  test "a session check that fails is not a session change", %{other: other} do
    conn = open()
    {:ok, session} = Session.state(conn)
    victim = id(conn)

    result =
      try do
        DBConnection.run(conn, fn checked_out ->
          sql(other, "KILL CONNECTION #{victim}")
          Process.sleep(100)
          throw({:checked, Session.unchanged(checked_out, session)})
        end)
      catch
        :throw, {:checked, value} -> value
      end

    assert {:error, %Selecto.Error{type: :connection_error}} = result
    close(conn)
  end

  test "a kill storm reaches the host as what it is and leaves no transaction",
       %{other: other} do
    conn = open()
    victim = id(conn)
    stop = :atomics.new(1, [])

    killer =
      Task.async(fn ->
        Stream.repeatedly(fn ->
          sql(other, "KILL QUERY #{victim}")
          Process.sleep(Enum.random(1..5))
        end)
        |> Enum.find(fn _ -> :atomics.get(stop, 1) == 1 end)
      end)

    deadline = System.monotonic_time(:millisecond) + 1_200

    results =
      Stream.repeatedly(fn -> read(conn) end)
      |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)

    :atomics.put(stop, 1, 1)
    Task.await(killer, 5_000)

    refute Enum.any?(
             results,
             &match?({:error, %{details: %{reason: :session_state_changed}}}, &1)
           )

    for result <- results do
      assert match?({:ok, _}, result) or
               match?({:error, %{details: %{category: :query_canceled}}}, result)
    end

    Process.sleep(50)
    sql(conn, "DO 0")
    assert sql(conn, "INSERT INTO hs_log VALUES ('host')") == 1
    close(conn)
    assert sql(other, "SELECT COUNT(*) FROM hs_log") == [[1]]
  end

  # A proxy that answers the first command naming `needle` with ERR 1317, as a
  # KILL QUERY landing on it would, without passing it on.
  defp killing_proxy(needle) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, ip: {127, 0, 0, 1}, nodelay: true])

    {:ok, port} = :inet.port(listener)
    armed = :atomics.new(1, [])
    target = Keyword.fetch!(opts(), :port)
    spawn_link(fn -> proxy_accept(listener, target, armed, needle) end)
    {port, armed}
  end

  defp proxy_accept(listener, target, armed, needle) do
    with {:ok, client} <- :gen_tcp.accept(listener) do
      {:ok, server} =
        :gen_tcp.connect({127, 0, 0, 1}, target, [:binary, packet: :raw, active: false])

      spawn(fn -> proxy_replies(server, client) end)
      spawn(fn -> proxy_commands(client, server, armed, needle) end)
      proxy_accept(listener, target, armed, needle)
    end
  end

  defp proxy_replies(server, client) do
    with {:ok, data} <- :gen_tcp.recv(server, 0), :ok <- :gen_tcp.send(client, data) do
      proxy_replies(server, client)
    else
      _closed -> :gen_tcp.close(client)
    end
  end

  defp proxy_commands(client, server, armed, needle) do
    with {:ok, <<size::little-24, sequence>> = header} <- :gen_tcp.recv(client, 4),
         {:ok, body} <- :gen_tcp.recv(client, size) do
      if :atomics.get(armed, 1) == 1 and :binary.match(body, needle) != :nomatch do
        :atomics.put(armed, 1, 0)
        error = <<0xFF, 1317::little-16, "#70100", "Query execution was interrupted">>
        :gen_tcp.send(client, <<byte_size(error)::little-24, sequence + 1>> <> error)
      else
        :gen_tcp.send(server, header <> body)
      end

      proxy_commands(client, server, armed, needle)
    else
      _closed -> :gen_tcp.close(server)
    end
  end

  test "a killed shadow check does not let a TEMPORARY table answer" do
    {port, armed} = killing_proxy("SHOW CREATE TABLE")
    conn = open(hostname: "127.0.0.1", port: port)
    sql(conn, "CREATE TEMPORARY TABLE hs_t (id INT, org VARCHAR(20), v INT)")
    sql(conn, "INSERT INTO hs_t VALUES (777, 'shadow', 0)")

    :atomics.put(armed, 1, 1)
    assert {:error, %{details: %{code: 1317}}} = read(conn)
    close(conn)
  end

  test "a write whose RELEASE SAVEPOINT failed is not committed by the host", %{other: other} do
    {port, armed} = killing_proxy("RELEASE SAVEPOINT")
    conn = open(hostname: "127.0.0.1", port: port)

    result =
      try do
        DBConnection.run(conn, fn held ->
          {:ok, _} = MyXQL.query(held, "START TRANSACTION", [], query_type: :text)
          :atomics.put(armed, 1, 1)
          written = Adapter.execute_write_unsafe(held, insert(60), [])
          {:ok, _} = MyXQL.query(held, "COMMIT", [], query_type: :text)
          throw({:written, written})
        end)
      catch
        :throw, {:written, written} -> written
      end

    assert {:error, _} = result
    assert sql(other, "SELECT COUNT(*) FROM hs_t WHERE id = 60") == [[0]]
    close(conn)
  end

  test "a connection that cannot be returned to idle closes, keeping the original result" do
    conn = open()
    before = id(conn)

    assert Session.checkout(conn, fn checked_out ->
             {:ok, _} = MyXQL.query(checked_out, "START TRANSACTION", [], query_type: :text)
             {:selecto_session_close, {:error, :original}}
           end) == {:error, :original}

    assert Session.checkout(conn, fn checked_out -> DBConnection.status(checked_out) end) == :idle
    assert id(conn) != before
    close(conn)
  end

  test "a held connection that cannot be returned to idle closes under its holder" do
    conn = open()
    before = id(conn)

    result =
      try do
        DBConnection.run(conn, fn held ->
          {:ok, _} = MyXQL.query(held, "START TRANSACTION", [], query_type: :text)

          value =
            Session.checkout(held, fn checked_out ->
              {:selecto_session_close, {:error, :original}}
              |> then(fn closed -> if checked_out == held, do: closed end)
            end)

          throw({:held, value, MyXQL.query(held, "SELECT 1", [], query_type: :text)})
        end)
      catch
        :throw, {:held, value, next} -> {value, next}
      end

    assert {{:error, :original}, {:error, %DBConnection.ConnectionError{}}} = result
    assert id(conn) != before
    close(conn)
  end
end
