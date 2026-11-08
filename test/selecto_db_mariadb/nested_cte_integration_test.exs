defmodule SelectoDBMariaDB.NestedCteIntegrationTest do
  # MariaDB lets a name from an enclosing WITH win over a CTE a body defines in a
  # WITH of its own; the body is refused rather than read the outer one.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  defp opts do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end

  defp domain(table, fields) do
    %{
      name: table,
      schemas: %{},
      joins: %{},
      source: %{
        source_table: table,
        primary_key: :id,
        fields: Keyword.keys(fields),
        redact_fields: [],
        columns: Map.new(fields, fn {k, v} -> {k, %{type: v}} end),
        associations: %{}
      }
    }
  end

  setup do
    {:ok, admin} = MyXQL.start_link(opts())

    db =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_nested_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, ddl} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    for sql <- [
          "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(20))",
          "CREATE TABLE r (id INT PRIMARY KEY, product_id INT, title VARCHAR(20))",
          "INSERT INTO p VALUES (1, 'a'), (2, 'b')",
          "INSERT INTO r VALUES (1, 1, 'r1'), (2, 2, 'r2')"
        ],
        do: MyXQL.query!(ddl, sql, [], query_type: :text)

    GenServer.stop(ddl)
    {:ok, conn} = Adapter.connect(Keyword.put(opts(), :database, db))

    on_exit(fn ->
      {:ok, a} = MyXQL.start_link(opts())
      MyXQL.query!(a, "DROP DATABASE IF EXISTS `#{db}`", [], query_type: :text)
    end)

    %{conn: conn}
  end

  defp query(conn, own_name) do
    pdom = domain("p", id: :integer, name: :string)
    rdom = domain("r", id: :integer, product_id: :integer, title: :string)

    all = fn ->
      Selecto.configure(pdom, conn, adapter: Adapter) |> Selecto.select(["id", "name"])
    end

    body = fn ->
      Selecto.configure(rdom, conn, adapter: Adapter)
      |> Selecto.with_cte(
        own_name,
        fn ->
          Selecto.configure(pdom, conn, adapter: Adapter)
          |> Selecto.select(["id", "name"])
          |> Selecto.filter({"id", 1})
        end,
        columns: ["pid", "label"]
      )
      |> Selecto.join(:oj,
        source: own_name,
        type: :inner,
        owner_key: :product_id,
        related_key: :pid,
        fields: %{label: %{type: :string}}
      )
      |> Selecto.select(["id", "oj.label"])
    end

    Selecto.configure(rdom, conn, adapter: Adapter)
    |> Selecto.with_cte("allp", all, columns: ["pid", "label"])
    |> Selecto.with_cte("cb", body,
      columns: ["rid", "clab"],
      join: [owner_key: :id, related_key: :rid, fields: %{clab: %{type: :string}}]
    )
    |> Selecto.select(["title", "cb.clab"])
    |> Selecto.order_by("title")
  end

  test "a body's own CTE named like an enclosing one refuses; another name answers", %{conn: conn} do
    assert {:error,
            %Selecto.Error{details: %{reason: :nested_cte_shadowing, relations: ["allp"]}}} =
             Selecto.execute(query(conn, "allp"))

    assert {:ok, {[["r1", "a"], ["r2", nil]], _, _}} = Selecto.execute(query(conn, "onlya"))
  end
end
