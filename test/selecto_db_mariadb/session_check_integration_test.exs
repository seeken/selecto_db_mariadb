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
    {"SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'", :sql_mode}
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
        assert {:ok, %{rows: [[name]]}} = MyXQL.query(conn, "SELECT name FROM items", [])
        assert name == "a"
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
