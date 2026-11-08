defmodule SelectoDBMariaDB.NestedWithIntegrationTest do
  # Nested WITH on the MySQL family: a body's copy of a statement CTE emitted after
  # it is the body's own (MySQL shows no body a later CTE, even under WITH
  # RECURSIVE); a values-backed schema in a body reads its own rows; a VALUES list
  # carried from the root into a body is the statement's.
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
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_nwith_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, ddl} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    for sql <- [
          "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(20), tenant_id INT)",
          "CREATE TABLE r (id INT PRIMARY KEY, product_id INT, title VARCHAR(20))",
          "CREATE TABLE c2 (product_id INT, label VARCHAR(20))",
          "CREATE TABLE it (id INT PRIMARY KEY, sid VARCHAR(5))",
          "CREATE TABLE sh (id INT PRIMARY KEY, sid VARCHAR(5))",
          "INSERT INTO p VALUES (1, 'a', 1), (2, 'b', 2)",
          "INSERT INTO r VALUES (1, 1, 'pub-t1'), (2, 1, 'pub-t2'), (3, 2, 'pub-low')",
          "INSERT INTO c2 VALUES (1, 'TBL-C2'), (2, 'TBL-C2b')",
          "INSERT INTO it VALUES (1, 'A'), (2, 'B')",
          "INSERT INTO sh VALUES (1, 'A'), (2, 'B')",
          "CREATE TABLE lk (product_id INT, label VARCHAR(20))",
          "INSERT INTO lk VALUES (1, 'TBL-LK')"
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

  defp domain(table, fields, extra \\ %{}) do
    %{
      name: table,
      schemas: %{},
      joins: %{},
      source:
        Map.merge(
          %{
            source_table: table,
            primary_key: :id,
            fields: Keyword.keys(fields),
            redact_fields: [],
            columns: Map.new(fields, fn {k, v} -> {k, %{type: v}} end),
            associations: %{}
          },
          extra
        )
    }
  end

  defp p, do: domain("p", id: :integer, name: :string, tenant_id: :integer)
  defp r, do: domain("r", id: :integer, product_id: :integer, title: :string)
  defp q(conn, domain), do: Selecto.configure(domain, conn, adapter: @adapter, validate: false)

  defp rows(query) do
    {:ok, {rows, _, _}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  defp walk(query, conn) do
    Selecto.with_recursive_cte(query, "walk",
      base_query: fn -> q(conn, p()) |> Selecto.select(["id"]) |> Selecto.filter({"id", 1}) end,
      recursive_query: fn _ref ->
        q(conn, p())
        |> Selecto.join(:w,
          source: "walk",
          type: :inner,
          owner_key: :id,
          related_key: :wid,
          fields: %{"wid" => %{type: :integer}}
        )
        |> Selecto.select(["id"])
        |> Selecto.filter({"id", 999})
      end,
      columns: ["wid"],
      join: [
        owner_key: :id,
        related_key: :wid,
        type: :left,
        fields: %{"wid" => %{type: :integer}}
      ]
    )
  end

  defp values_domain(table, prefix, id \\ :status) do
    %{
      name: table,
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :sid],
        redact_fields: [],
        columns: %{id: %{type: :integer}, sid: %{type: :string}},
        associations: %{
          id => %{queryable: id, field: id, owner_key: :sid, related_key: :id}
        }
      },
      schemas: %{
        id => %{
          primary_key: :id,
          fields: [:id, :name],
          columns: %{id: %{type: :string}, name: %{type: :string}},
          values: [%{id: "A", name: prefix <> "-A"}, %{id: "B", name: prefix <> "-B"}],
          associations: %{}
        }
      },
      joins: %{id => %{type: :left}}
    }
  end

  test "under WITH RECURSIVE a body's copy of a later CTE is its own", %{conn: conn} do
    label = %{"label" => %{type: :string}}

    spec =
      Selecto.Advanced.CTE.create_cte(
        "c2",
        fn ->
          q(conn, p()) |> Selecto.filter({"tenant_id", 1}) |> Selecto.select(["id", "name"])
        end,
        columns: ["product_id", "label"]
      )

    body = fn ->
      q(conn, r())
      |> Selecto.with_ctes([spec])
      |> Selecto.join(:c2j,
        source: "c2",
        source_kind: :cte,
        type: :inner,
        owner_key: :product_id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.select(["id", "c2j.label"])
    end

    query =
      q(conn, r())
      |> Selecto.with_cte("cb", body,
        columns: ["rid", "clab"],
        join: [owner_key: :id, related_key: :rid, fields: %{"clab" => %{type: :string}}]
      )
      |> Selecto.with_ctes([spec])
      |> Selecto.join(:c2m,
        source: "c2",
        source_kind: :cte,
        type: :left,
        owner_key: :product_id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.with_recursive_cte("walk",
        base_query: fn -> q(conn, p()) |> Selecto.select(["id"]) |> Selecto.filter({"id", 1}) end,
        recursive_query: fn _ref ->
          q(conn, p())
          |> Selecto.join(:w,
            source: "walk",
            type: :inner,
            owner_key: :id,
            related_key: :wid,
            fields: %{"wid" => %{type: :integer}}
          )
          |> Selecto.select(["id"])
          |> Selecto.filter({"id", 999})
        end,
        columns: ["wid"],
        join: [
          owner_key: :product_id,
          related_key: :wid,
          type: :inner,
          fields: %{"wid" => %{type: :integer}}
        ]
      )
      |> Selecto.select(["title", "cb.clab", "c2m.label"])

    assert rows(query) == [["pub-t1", "a", "a"], ["pub-t2", "a", "a"]]
  end

  test "a values-backed schema in a body reads its own rows", %{conn: conn} do
    body = fn -> q(conn, values_domain("sh", "Ship")) |> Selecto.select(["id", "status.name"]) end

    query =
      q(conn, values_domain("it", "Item"))
      |> Selecto.with_cte("sc", body,
        columns: ["shid", "sname"],
        join: [
          owner_key: :id,
          related_key: :shid,
          type: :left,
          fields: %{"sname" => %{type: :string}}
        ]
      )
      |> Selecto.select(["id", "status.name", "sc.sname"])

    assert rows(query) == [[1, "Item-A", "Ship-A"], [2, "Item-B", "Ship-B"]]
  end

  test "a VALUES list carried from the root into a body is the statement's", %{conn: conn} do
    root =
      q(conn, p())
      |> Selecto.with_values([[1, "OUTER-V"]], columns: ["product_id", "label"], as: "lk")

    body = fn ->
      root
      |> Selecto.join(:lkj,
        source: "lk",
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: %{"label" => %{type: :string}}
      )
      |> Selecto.select(["id", "lkj.label"])
    end

    query =
      root
      |> Selecto.with_cte("cv", body,
        columns: ["pid", "lab"],
        join: [owner_key: :id, related_key: :pid, fields: %{"lab" => %{type: :string}}]
      )
      |> Selecto.select(["name", "cv.lab"])

    assert rows(query) == [["a", "OUTER-V"], ["b", nil]]
  end

  test "a values-backed schema with a long id answers", %{conn: conn} do
    id = String.to_atom("status_" <> String.duplicate("y", 39))
    query = q(conn, values_domain("it", "Item", id)) |> Selecto.select(["id", "#{id}.name"])

    assert rows(query) == [[1, "Item-A"], [2, "Item-B"]]
  end

  test "under WITH RECURSIVE a body reading a table named like a later CTE refuses",
       %{conn: conn} do
    label = %{"label" => %{type: :string}}

    lk =
      Selecto.Advanced.CTE.create_cte(
        "lk",
        fn -> q(conn, p()) |> Selecto.select(["id", "name"]) end,
        columns: ["product_id", "label"]
      )

    body = fn ->
      q(conn, p())
      |> Selecto.join(:lkj,
        source: "lk",
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.select(["id", "lkj.label"])
    end

    query =
      q(conn, p())
      |> Selecto.with_cte("cb", body,
        columns: ["rid", "clab"],
        join: [owner_key: :id, related_key: :rid, fields: %{"clab" => %{type: :string}}]
      )
      |> Selecto.with_ctes([lk])
      |> Selecto.join(:lkm,
        source: "lk",
        source_kind: :cte,
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.select(["name", "cb.clab", "lkm.label"])

    assert rows(query) == [["a", "TBL-LK", "a"], ["b", nil, "b"]]

    assert {:error, %Selecto.Error{details: %{reason: :cte_name_collision, relations: ["lk"]}}} =
             Selecto.execute(walk(query, conn))
  end

  test "a depth-3 body's own CTE named like a root CTE reads its own", %{conn: conn} do
    label = %{"label" => %{type: :string}}

    own =
      Selecto.Advanced.CTE.create_cte(
        "c2",
        fn ->
          q(conn, p()) |> Selecto.filter({"tenant_id", 1}) |> Selecto.select(["id", "name"])
        end,
        columns: ["product_id", "label"]
      )

    all =
      Selecto.Advanced.CTE.create_cte(
        "c2",
        fn -> q(conn, p()) |> Selecto.select(["id", "name"]) end,
        columns: ["product_id", "label"]
      )

    dd = fn ->
      q(conn, r())
      |> Selecto.with_ctes([own])
      |> Selecto.join(:c2j,
        source: "c2",
        source_kind: :cte,
        type: :inner,
        owner_key: :product_id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.select(["id", "c2j.label"])
    end

    cb = fn ->
      q(conn, r())
      |> Selecto.with_cte("dd", dd,
        columns: ["did", "dl"],
        join: [
          owner_key: :id,
          related_key: :did,
          type: :inner,
          fields: %{"dl" => %{type: :string}}
        ]
      )
      |> Selecto.select(["id", "dd.dl"])
    end

    query =
      q(conn, r())
      |> Selecto.with_ctes([all])
      |> Selecto.with_cte("cb", cb,
        columns: ["rid", "clab"],
        join: [owner_key: :id, related_key: :rid, fields: %{"clab" => %{type: :string}}]
      )
      |> Selecto.join(:c2m,
        source: "c2",
        source_kind: :cte,
        type: :left,
        owner_key: :product_id,
        related_key: :product_id,
        fields: label
      )
      |> Selecto.select(["title", "cb.clab", "c2m.label"])

    assert rows(query) == [["pub-low", nil, "b"], ["pub-t1", "a", "a"], ["pub-t2", "a", "a"]]
  end
end
