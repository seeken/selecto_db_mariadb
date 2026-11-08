defmodule SelectoDBMariaDB.ClosureNameIntegrationTest do
  # Two closure joins with one id over different closure tables, in the root and
  # in a CTE body, a union member or a subquery: each generated CTE is named by
  # its definition, so each reads its own closure table. A list named like the
  # closure table a join reads refuses.
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  @adapter SelectoDBMariaDB.Adapter

  defp opts do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end

  setup do
    {:ok, admin} = MyXQL.start_link(opts())

    db =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_closure_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, ddl} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    for sql <- [
          "CREATE TABLE cat (id INT PRIMARY KEY, name VARCHAR(20))",
          "CREATE TABLE cat_closure (ancestor_id INT, descendant_id INT, depth INT)",
          "CREATE TABLE cc2 (ancestor_id INT, descendant_id INT, depth INT)",
          "CREATE TABLE ci (id INT PRIMARY KEY, cat_id INT)",
          "INSERT INTO cat VALUES (1, 'c1'), (2, 'c2')",
          "INSERT INTO cat_closure VALUES (1, 1, 0), (1, 2, 1), (2, 2, 0)",
          "INSERT INTO cc2 VALUES (1, 1, 10), (1, 2, 11), (2, 2, 10)",
          "INSERT INTO ci VALUES (1, 2)"
        ],
        do: MyXQL.query!(ddl, sql, [], query_type: :text)

    GenServer.stop(ddl)
    {:ok, conn} = @adapter.connect(Keyword.put(opts(), :database, db))

    on_exit(fn ->
      {:ok, a} = MyXQL.start_link(opts())
      MyXQL.query!(a, "DROP DATABASE IF EXISTS `#{db}`", [], query_type: :text)
    end)

    %{conn: conn}
  end

  defp closure_domain(closure) do
    %{
      name: "ci",
      source: %{
        source_table: "ci",
        primary_key: :id,
        fields: [:id, :cat_id],
        redact_fields: [],
        columns: %{id: %{type: :integer}, cat_id: %{type: :integer}},
        associations: %{
          catj: %{queryable: :catq, field: :catj, owner_key: :cat_id, related_key: :id}
        }
      },
      schemas: %{
        catq: %{
          source_table: "cat",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        }
      },
      joins: %{
        catj: %{
          type: :hierarchical,
          name: "catj",
          hierarchy_type: :closure_table,
          closure_table: closure
        }
      }
    }
  end

  defp q(conn, closure),
    do:
      Selecto.configure(closure_domain(closure), conn, adapter: @adapter, validate: false)
      |> Selecto.select(["id", "catj_depth"])

  defp rows(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _, _}} ->
        Enum.sort(rows)

      {:error, %Selecto.Error{details: details}} ->
        {:error, details[:reason], details[:relations]}
    end
  end

  test "a CTE body's closure join with the root's id reads its own closure table",
       %{conn: conn} do
    query =
      q(conn, "cat_closure")
      |> Selecto.with_cte("cb", fn -> q(conn, "cc2") end,
        columns: ["rid", "cdepth"],
        join: [owner_key: :id, related_key: :rid, fields: %{"cdepth" => %{type: :integer}}]
      )
      |> Selecto.select(["id", "catj_depth", "cb.cdepth"])

    assert rows(query) == [[1, 0, 10], [1, 0, 11], [1, 1, 10], [1, 1, 11]]
  end

  test "union members' closure joins fail on MariaDB (a WITH in a set operand)",
       %{conn: conn} do
    union = Selecto.union(q(conn, "cat_closure"), q(conn, "cc2"), all: true)
    # Known (LOW): MariaDB does not parse a WITH inside a set operand.
    assert {:error, %Selecto.Error{type: :query_error}} = Selecto.execute(union)
  end

  test "a list named like the closure table a join reads refuses", %{conn: conn} do
    query =
      q(conn, "cat_closure")
      |> Selecto.with_values([[1, 1, 10]],
        columns: ["ancestor_id", "descendant_id", "depth"],
        as: "cat_closure"
      )

    assert rows(query) == {:error, :cte_name_collision, ["cat_closure"]}
  end
end
