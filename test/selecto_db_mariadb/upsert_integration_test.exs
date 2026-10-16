defmodule SelectoDBMariaDB.UpsertIntegrationTest do
  # Upserts act on the declared conflict target only, as PostgreSQL's
  # ON CONFLICT (target) does. ON DUPLICATE KEY UPDATE fired on any unique
  # key: an assigned id of another row overwrote that row.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 180_000

  @retryable [:deadlock_detected, :lock_timeout]

  test "a primary-key collision never updates the row that owns the key" do
    with_database(fn conn, _database ->
      result = Adapter.execute_write_unsafe(conn, upsert(1, "zzz", "overwritten"), [])

      assert {:error, %Error{type: :execution_failed, details: %{reason: :unique_violation}}} =
               result

      assert rows(conn) == [[1, "a", "victim"], [2, "b", "other"]]
    end)
  end

  test "an existing target row is updated and a new target row inserted" do
    with_database(fn conn, _database ->
      assert {:ok, %{affected_rows: 1}} =
               Adapter.execute_write_unsafe(conn, upsert(9, "a", "renamed"), [])

      assert {:ok, %{affected_rows: 1}} =
               Adapter.execute_write_unsafe(conn, upsert(3, "c", "new"), [])

      assert rows(conn) == [[1, "a", "renamed"], [2, "b", "other"], [3, "c", "new"]]
    end)
  end

  test "a target that is not exactly one whole-column unique index is refused" do
    with_database(fn conn, _database ->
      driver!(
        conn,
        "ALTER TABLE items ADD COLUMN label VARCHAR(20), ADD UNIQUE KEY pre (label(2))"
      )

      driver!(conn, "ALTER TABLE items ADD UNIQUE KEY pair (code, name)")

      for target <- [[:name], [:label], [:code, :id, :name]] do
        command = put_in(upsert(5, "e", "x").metadata.conflict_target, target)
        command = put_in(command.metadata.declared_conflict_targets, [target])

        assert {:error,
                %Error{
                  type: :write_capability_missing,
                  details: %{code: :unsupported_upsert_conflict_target}
                }} = Adapter.execute_write_unsafe(conn, command, [])
      end

      assert rows(conn) == [[1, "a", "victim"], [2, "b", "other"]]
    end)
  end

  test "a tenant column in the target compares exactly" do
    with_database(fn conn, _database ->
      driver!(conn, """
      CREATE TABLE scoped (id INT PRIMARY KEY, tenant_id VARCHAR(20) NOT NULL,
        code VARCHAR(20) NOT NULL, name VARCHAR(20), UNIQUE KEY tc (tenant_id, code)) ENGINE=InnoDB
      """)

      driver!(conn, "INSERT INTO scoped VALUES (1, 'T1', 'a', 'upper')")

      command = %Command{
        operation: :upsert,
        relation: "scoped",
        assignments: [
          %{field: "id", value: {:literal, 2}},
          %{field: "tenant_id", value: {:literal, "t1"}},
          %{field: "code", value: {:literal, "a"}},
          %{field: "name", value: {:literal, "t1 row"}}
        ],
        predicate: nil,
        returning: :none,
        expected_cardinality: {:exactly, 1},
        metadata: %{
          conflict_target: ["tenant_id", "code"],
          declared_conflict_targets: [["tenant_id", "code"]],
          upsert_update_fields: ["name"],
          tenant_fields: ["tenant_id"],
          field_types: %{"tenant_id" => :string}
        }
      }

      # The case-insensitive unique index refuses 't1' beside 'T1'; the exact
      # target finds no 't1' row, so the unique violation stands.
      assert {:error, %Error{details: %{reason: :unique_violation}}} =
               Adapter.execute_write_unsafe(conn, command, [])

      assert column(conn, "SELECT CONCAT(tenant_id, ':', name) FROM scoped") == ["T1:upper"]
    end)
  end

  test "a deadlock on the savepointed insert keeps its typed retryable error" do
    with_database(fn conn, database ->
      for id <- 10..19, do: driver!(conn, "INSERT INTO items VALUES (#{id}, 'x#{id}', 'pad')")
      {:ok, other} = Adapter.connect(Keyword.put(connection_options(), :database, database))
      parent = self()

      try do
        # The other session inserts the new key, then waits on the host's row;
        # it changes more rows, so InnoDB picks the host as the victim.
        blocker =
          Task.async(fn ->
            MyXQL.transaction(other, fn tx ->
              MyXQL.query!(tx, "UPDATE items SET name = 'o' WHERE id >= 10", [])
              MyXQL.query!(tx, "INSERT INTO items VALUES (50, 'n', 'other session')", [])
              send(parent, :inserted)

              receive do
                :go -> MyXQL.query(tx, "UPDATE items SET name = 'o' WHERE code = 'b'", [])
              end
            end)
          end)

        MyXQL.transaction(conn, fn tx ->
          MyXQL.query!(tx, "UPDATE items SET name = 'host' WHERE code = 'b'", [])
          assert_receive :inserted, 5_000
          send(blocker.pid, :go)
          Process.sleep(300)

          written = Adapter.execute_write_unsafe(tx, upsert(51, "n", "host"), [])
          send(parent, {:written, written})
          written
        end)

        assert_received {:written, written}

        assert {:error, %Error{type: :execution_failed, details: %{reason: :deadlock_detected}}} =
                 written

        Task.await(blocker, 10_000)
      after
        Adapter.disconnect(other)
      end
    end)
  end

  for sessions <- [2, 3, 5], key <- [:existing, :new] do
    test "#{sessions} sessions racing on #{key} key with a deleter stay on the target" do
      with_database(fn _conn, database ->
        code = if unquote(key) == :existing, do: "a", else: "n"
        parent = self()

        workers =
          for index <- 1..unquote(sessions) do
            Task.async(fn ->
              {:ok, conn} =
                Adapter.connect(Keyword.put(connection_options(), :database, database))

              try do
                receive do
                  :go -> :ok
                end

                for round <- 1..5 do
                  Adapter.execute_write_unsafe(
                    conn,
                    upsert(100 + index * 10 + round, code, "s#{index}"),
                    []
                  )
                end
              after
                Adapter.disconnect(conn)
              end
            end)
          end

        deleter =
          Task.async(fn ->
            {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

            try do
              receive do
                :go -> :ok
              end

              for _round <- 1..5 do
                MyXQL.query(conn, "DELETE FROM items WHERE code = ?", [code])
              end
            after
              Adapter.disconnect(conn)
            end
          end)

        for task <- [deleter | workers], do: send(task.pid, :go)
        _ = parent
        Task.await(deleter, 60_000)

        results = workers |> Enum.map(&Task.await(&1, 60_000)) |> List.flatten()

        for result <- results do
          case result do
            {:ok, %{affected_rows: 1}} ->
              :ok

            {:error, %Error{type: :execution_failed, details: %{reason: reason}}} ->
              assert reason in @retryable, inspect(result)
          end
        end

        {:ok, check} = Adapter.connect(Keyword.put(connection_options(), :database, database))

        try do
          # The other row is never touched, and the target holds one row.
          assert column(check, "SELECT name FROM items WHERE code = 'b'") == ["other"]
          assert length(column(check, "SELECT id FROM items WHERE code = '#{code}'")) <= 1
        after
          Adapter.disconnect(check)
        end
      end)
    end
  end

  defp upsert(id, code, name) do
    %Command{
      operation: :upsert,
      relation: "items",
      assignments: [
        %{field: "id", value: {:literal, id}},
        %{field: "code", value: {:literal, code}},
        %{field: "name", value: {:literal, name}}
      ],
      predicate: nil,
      returning: :none,
      expected_cardinality: {:exactly, 1},
      metadata: %{
        conflict_target: ["code"],
        declared_conflict_targets: [["code"]],
        upsert_update_fields: ["name"]
      }
    }
  end

  defp rows(conn) do
    {:ok, %{rows: rows}} =
      MyXQL.query(conn, "SELECT id, code, name FROM items ORDER BY id", [])

    rows
  end

  defp column(conn, sql) do
    {:ok, %{rows: rows}} = MyXQL.query(conn, sql, [])
    Enum.map(rows, &hd/1)
  end

  defp driver!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_upsert_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    driver!(admin, "CREATE DATABASE `#{database}`")

    try do
      {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

      try do
        driver!(conn, """
        CREATE TABLE items (id INT PRIMARY KEY, code VARCHAR(20) NOT NULL UNIQUE,
          name VARCHAR(20)) ENGINE=InnoDB
        """)

        driver!(conn, "INSERT INTO items VALUES (1, 'a', 'victim'), (2, 'b', 'other')")
        fun.(conn, database)
      after
        Adapter.disconnect(conn)
      end
    after
      MyXQL.query(admin, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
      Adapter.disconnect(admin)
    end
  end

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
