defmodule SelectoDBMariaDB.WriteErrorIntegrationTest do
  # Strict-mode value errors are typed data violations, and a deadlock victim
  # keeps its typed retryable category also inside a host transaction. No
  # error carries the value, the statement or a key name.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  test "values a strict-mode column cannot hold are data violations" do
    with_database(fn conn ->
      for {field, value, code} <- [
            {"code", "secret-too-long", 1406},
            {"small", 1000, 1264},
            {"small", "secret-number", 1366},
            {"day", "2024-02-30", 1292}
          ] do
        assert {:error, %Error{type: :execution_failed, details: details} = error} =
                 Adapter.execute_write_unsafe(conn, insert(%{field => value}), [])

        assert details.reason == :data_violation, "errno #{code}"
        refute inspect(error) =~ "secret"
        refute inspect(error) =~ "INSERT"
      end

      assert {:error, %Error{details: %{reason: :not_null_violation}}} =
               Adapter.execute_write_unsafe(conn, insert(%{}, ["name"]), [])

      assert column(conn, "SELECT COUNT(*) FROM items WHERE id = 5") == [0]
    end)
  end

  test "zero and out-of-range stored dates fail reads and streams as typed data errors" do
    with_database(fn conn ->
      MyXQL.query!(conn, "CREATE TABLE days (id INT PRIMARY KEY, d DATE, dt DATETIME)", [],
        query_type: :text
      )

      MyXQL.query!(conn, "SET SESSION sql_mode = ''", [], query_type: :text)

      MyXQL.query!(
        conn,
        "INSERT INTO days VALUES (1, '0000-00-00', '2024-01-01'), (2, '2024-00-10', NULL), " <>
          "(3, NULL, '2024-02-00 10:00:00'), (4, '0000-01-01', NULL), (5, '0001-01-01', NULL)",
        [],
        query_type: :text
      )

      MyXQL.query!(conn, "SET SESSION sql_mode = DEFAULT", [], query_type: :text)

      for id <- 1..4 do
        assert {:error, %Selecto.Error{details: details}} =
                 Adapter.execute(conn, "SELECT d, dt FROM days WHERE id = ?", [id], [])

        assert details.category == :datetime_field_overflow
        assert details.sqlstate == "22008"

        assert {:error, %Selecto.Error{details: %{category: :datetime_field_overflow}}} =
                 Adapter.stream(conn, "SELECT d, dt FROM days WHERE id = ?", [id], [])
      end

      assert {:ok, %{rows: [[~D[0001-01-01], nil]]}} =
               Adapter.execute(conn, "SELECT d, dt FROM days WHERE id = 5", [], [])
    end)
  end

  test "a deadlock victim inside a host transaction keeps its typed retryable category" do
    with_database(fn conn ->
      {:ok, other} = Adapter.connect(Keyword.put(connection_options(), :database, database(conn)))

      try do
        parent = self()

        # The other session modifies more rows, so InnoDB picks the host
        # transaction (fewer changes) as the victim.
        blocker =
          Task.async(fn ->
            MyXQL.transaction(other, fn tx ->
              MyXQL.query!(tx, "UPDATE items SET code = 'o' WHERE id >= 2", [])
              send(parent, :other_locked)

              receive do
                :go -> MyXQL.query(tx, "UPDATE items SET code = 'o' WHERE id = 1", [])
              end
            end)
          end)

        result =
          MyXQL.transaction(conn, fn tx ->
            MyXQL.query!(tx, "UPDATE items SET code = 'h' WHERE id = 1", [])
            assert_receive :other_locked, 5_000
            send(blocker.pid, :go)
            # Let the other session queue on row 1 before closing the cycle.
            Process.sleep(300)

            written = Adapter.execute_write_unsafe(tx, update(2, "x"), [])
            send(parent, {:written, written})
            written
          end)

        assert_received {:written, written}

        assert {:error, %Error{type: :execution_failed, details: details} = error} = written
        assert details.reason == :deadlock_detected
        refute inspect(error) =~ "UPDATE"

        # The host transaction cannot commit, and the other session finished.
        assert {:error, _} = result
        assert {:ok, {:ok, _}} = Task.await(blocker, 10_000)
        assert column(conn, "SELECT code FROM items WHERE id = 1") == ["o"]
      after
        Adapter.disconnect(other)
      end
    end)
  end

  defp insert(values, omit \\ []) do
    base = %{"id" => 5, "code" => "ok", "small" => 1, "day" => "2024-02-01", "name" => "n"}
    row = base |> Map.merge(values) |> Map.drop(omit)

    %Command{
      operation: :insert,
      relation: "items",
      assignments:
        Enum.map(row, fn {field, value} -> %{field: field, value: {:literal, value}} end),
      predicate: nil,
      returning: :none,
      expected_cardinality: {:exactly, 1},
      metadata: %{}
    }
  end

  defp update(id, code) do
    %Command{
      operation: :update,
      relation: "items",
      assignments: [%{field: "code", value: {:literal, code}}],
      predicate: {:eq, {:field, "id"}, {:literal, id}},
      returning: :none,
      expected_cardinality: {:exactly, 1},
      metadata: %{}
    }
  end

  defp column(conn, sql) do
    {:ok, %{rows: rows}} = Adapter.execute(conn, sql, [], [])
    Enum.map(rows, &hd/1)
  end

  defp database(conn) do
    {:ok, %{rows: [[name]]}} = MyXQL.query(conn, "SELECT DATABASE()", [])
    name
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_werr_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{database}`", [], query_type: :text)

    try do
      {:ok, conn} =
        Adapter.connect(Keyword.merge(connection_options(), database: database, pool_size: 1))

      try do
        MyXQL.query!(
          conn,
          "CREATE TABLE items (id INT PRIMARY KEY, code VARCHAR(3) NOT NULL, " <>
            "small TINYINT, day DATE, name VARCHAR(20) NOT NULL) ENGINE=InnoDB",
          [],
          query_type: :text
        )

        for id <- 1..6 do
          MyXQL.query!(conn, "INSERT INTO items VALUES (#{id * 10}, 'a', 1, NULL, 'n')", [],
            query_type: :text
          )
        end

        MyXQL.query!(conn, "UPDATE items SET id = 1 WHERE id = 10", [], query_type: :text)
        MyXQL.query!(conn, "UPDATE items SET id = 2 WHERE id = 20", [], query_type: :text)
        fun.(conn)
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
