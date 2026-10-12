defmodule SelectoDBMariaDB.RowStreamIntegrationTest do
  @moduledoc """
  The read-only cursor stream: full and early-closed enumeration, an empty
  result, and connection ownership. Runs against the existing database named
  by `SELECTO_MARIADB_DATABASE`.
  """
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter, as: MySQL

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  @table "selecto_stream_rows"

  setup do
    {:ok, conn} = MySQL.connect(connection_options())
    execute!(conn, "DROP TABLE IF EXISTS #{@table}")
    execute!(conn, "CREATE TABLE #{@table} (id INT PRIMARY KEY, name VARCHAR(40))")

    for id <- 1..7 do
      {:ok, _} =
        MySQL.execute(conn, "INSERT INTO #{@table} VALUES (?, ?)", [id, "row #{id}"], [])
    end

    on_exit(fn ->
      {:ok, cleanup} = MySQL.connect(connection_options())
      execute!(cleanup, "DROP TABLE IF EXISTS #{@table}")
      MySQL.disconnect(cleanup)
    end)

    {:ok, conn: conn}
  end

  test "streams every row in order across several fetches", %{conn: conn} do
    assert {:ok, stream, ["id", "name"]} =
             MySQL.stream(conn, "SELECT id, name FROM #{@table} WHERE id > ? ORDER BY id", [0],
               max_rows: 2
             )

    assert Enum.map(stream, &hd/1) == Enum.to_list(1..7)
  end

  test "an early-closed stream releases the connection for the next query", %{conn: conn} do
    {:ok, stream, _columns} =
      MySQL.stream(conn, "SELECT id FROM #{@table} ORDER BY id", [], max_rows: 2)

    assert Enum.take(stream, 3) == [[1], [2], [3]]
    assert {:ok, %{rows: [[7]]}} = MySQL.execute(conn, "SELECT COUNT(*) FROM #{@table}", [], [])

    {:ok, again, _columns} =
      MySQL.stream(conn, "SELECT id FROM #{@table} WHERE id > ? ORDER BY id", [5], [])

    assert Enum.to_list(again) == [[6], [7]]
  end

  test "an empty result reports its columns and no rows", %{conn: conn} do
    assert {:ok, stream, ["id"]} =
             MySQL.stream(conn, "SELECT id FROM #{@table} WHERE id < ?", [0], [])

    assert Enum.to_list(stream) == []
  end

  test "a stream never joins or ends a host transaction on its connection", %{conn: conn} do
    result =
      MyXQL.transaction(conn, fn tx ->
        {:ok, _} = MyXQL.query(tx, "UPDATE #{@table} SET name = ? WHERE id = ?", ["host", 1])
        # The cursor needs the connection the host holds, so it cannot open.
        opened = MySQL.stream(conn, "SELECT id FROM #{@table}", [], receive_timeout: 1_000)
        {:ok, %{rows: [["host"]]}} = MyXQL.query(tx, "SELECT name FROM #{@table} WHERE id = 1")
        opened
      end)

    assert {:ok, {:error, _reason}} = result

    assert {:ok, %{rows: [["host"]]}} =
             MySQL.execute(conn, "SELECT name FROM #{@table} WHERE id = ?", [1], [])
  end

  test "a failing statement is an error, not an empty stream", %{conn: conn} do
    assert {:error, _reason} = MySQL.stream(conn, "SELECT missing FROM #{@table}", [], [])
  end

  defp execute!(conn, sql) do
    assert {:ok, _result} = MyXQL.query(conn, sql, [], query_type: :text)
  end

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD"),
      database: System.fetch_env!("SELECTO_MARIADB_DATABASE")
    ]
  end
end
