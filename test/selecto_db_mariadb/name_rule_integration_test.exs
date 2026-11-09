defmodule SelectoDBMariaDB.NameRuleIntegrationTest do
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
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_names_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, ddl} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    for sql <- [
          "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(20), tenant_id INT)",
          "CREATE TABLE r (id INT PRIMARY KEY, product_id INT, rating INT, title VARCHAR(20), tenant_id INT)",
          "CREATE TABLE lk (product_id INT, label VARCHAR(20))",
          "INSERT INTO p VALUES (1, 'a', 1), (2, 'b', 2)",
          "INSERT INTO r VALUES (1, 1, 5, 'pub-t1', 1), (2, 1, 5, 'pub-t2', 2), (3, 2, 1, 'pub-low', 2)",
          "INSERT INTO lk VALUES (1, 'TBL-LK')"
        ],
        do: MyXQL.query!(ddl, sql, [], query_type: :text)

    GenServer.stop(ddl)
    {:ok, conn} = @adapter.connect(Keyword.put(opts(), :database, db))

    on_exit(fn ->
      {:ok, a} = MyXQL.start_link(opts())
      MyXQL.query!(a, "DROP DATABASE IF EXISTS `#{db}`", [], query_type: :text)
    end)

    %{conn: conn, t: %{p: "p", r: "r", lk: "lk"}}
  end

  # Shared shapes: filter subqueries, declared non-association joins, domains
  # rooted on a CTE, and lists private to union members.

  defp dom(table, fields, extra \\ %{}) do
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

  @pf [id: :integer, name: :string, tenant_id: :integer]
  @rf [id: :integer, product_id: :integer, rating: :integer, title: :string, tenant_id: :integer]

  defp p(ctx), do: dom(ctx.t.p, @pf)
  defp r(ctx), do: dom(ctx.t.r, @rf)
  defp q(ctx, domain), do: Selecto.configure(domain, ctx.conn, adapter: @adapter, validate: false)

  defp outcome(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _, _}} -> Enum.sort(rows)
      {:error, %Selecto.Error{type: type, details: details}} -> {:error, details[:reason] || type}
    end
  end

  defp refusal(query) do
    case Selecto.execute(query) do
      {:error, %Selecto.Error{details: %{reason: reason, relations: names}}} -> {reason, names}
      other -> other
    end
  end

  # Products with a review rated 4 or more: only product 1.
  defp rated(ctx),
    do: q(ctx, r(ctx)) |> Selecto.select(["product_id"]) |> Selecto.filter({"rating", {:>=, 4}})

  defp values_r(query, ctx),
    do:
      Selecto.with_values(query, [[2, 5, 99, "VALS", 1]],
        columns: ["product_id", "rating", "id", "title", "tenant_id"],
        as: ctx.t.r
      )

  test "a filter subquery reads its table; a list named like it refuses", ctx do
    in_filter = q(ctx, p(ctx)) |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
    assert outcome(Selecto.select(in_filter, ["name"])) == [["a"]]

    assert refusal(in_filter |> values_r(ctx) |> Selecto.select(["name"])) ==
             {:cte_name_collision, [ctx.t.r]}

    exists =
      q(ctx, r(ctx))
      |> Selecto.select(["id"])
      |> Selecto.filter({"product_id", 2})
      |> Selecto.filter({"rating", {:>=, 4}})

    exists_query = q(ctx, p(ctx)) |> Selecto.filter({:exists, exists})
    assert outcome(Selecto.select(exists_query, ["name"])) == []

    assert refusal(exists_query |> values_r(ctx) |> Selecto.select(["name"])) ==
             {:cte_name_collision, [ctx.t.r]}

    any =
      q(ctx, p(ctx))
      |> values_r(ctx)
      |> Selecto.filter({"id", "=", {:subquery, :any, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(any) == {:cte_name_collision, [ctx.t.r]}
  end

  test "a declared non-association join reads its table; a list named like it refuses", ctx do
    domain =
      put_in(p(ctx), [:joins], %{
        lkd: %{
          non_assoc: true,
          source: ctx.t.lk,
          name: "lkd",
          type: :left,
          owner_key: :id,
          related_key: :product_id,
          fields: %{"label" => %{type: :string}}
        }
      })

    query = q(ctx, domain) |> Selecto.select(["name", "lkd.label"])
    assert outcome(query) == [["a", "TBL-LK"], ["b", nil]]

    named =
      Selecto.with_values(query, [[1, "OUTER-V"]], columns: ["product_id", "label"], as: ctx.t.lk)

    assert refusal(named) == {:cte_name_collision, [ctx.t.lk]}
  end

  test "a domain rooted on a CTE reads the CTE", ctx do
    one = fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) |> Selecto.filter({"id", 1}) end
    c1 = dom("cte_one", id: :integer, name: :string)
    c2 = dom("cte_two", id: :integer, name: :string)

    rooted = q(ctx, c1) |> Selecto.with_cte("cte_one", one, columns: ["id", "name"])
    assert outcome(Selecto.select(rooted, ["name"])) == [["a"]]

    chained =
      q(ctx, c2)
      |> Selecto.with_cte("cte_one", one, columns: ["id", "name"])
      |> Selecto.with_cte("cte_two", fn -> q(ctx, c1) |> Selecto.select(["id", "name"]) end,
        columns: ["id", "name"],
        dependencies: ["cte_one"]
      )

    assert outcome(Selecto.select(chained, ["name"])) == [["a"]]

    # The same name read as a table elsewhere in the statement still refuses.
    as_table = dom(ctx.t.lk, product_id: :integer, label: :string)
    lk_rows = fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end

    mixed =
      q(ctx, dom(ctx.t.lk, id: :integer, name: :string))
      |> Selecto.with_cte(
        "cb",
        fn -> q(ctx, as_table) |> Selecto.select(["product_id", "label"]) end,
        columns: ["pid", "plabel"]
      )
      |> Selecto.with_cte(ctx.t.lk, lk_rows, columns: ["id", "name"])
      |> Selecto.select(["name"])

    assert refusal(mixed) == {:cte_name_collision, [ctx.t.lk]}
  end

  defp labelled(ctx, label) do
    q(ctx, p(ctx))
    |> Selecto.with_values([[1, label]], columns: ["product_id", "label"], as: "vv")
    |> Selecto.join(:vj,
      source: "vv",
      type: :left,
      owner_key: :id,
      related_key: :product_id,
      fields: %{"label" => %{type: :string}}
    )
    |> Selecto.select(["name", "vj.label"])
  end

  test "different lists of one name in two union members are private to each", ctx do
    union = Selecto.union(labelled(ctx, "L"), labelled(ctx, "R"), all: true)
    # Known (LOW): MariaDB does not parse a WITH inside a set operand.
    assert {:error, %Selecto.Error{type: :query_error}} = Selecto.execute(union)

    # A member's list and a different statement list of that name refuse.
    body = fn -> Selecto.union(labelled(ctx, "L"), labelled(ctx, "L"), all: true) end

    statement =
      q(ctx, p(ctx))
      |> Selecto.with_values([[2, "MAIN"]], columns: ["product_id", "label"], as: "vv")
      |> Selecto.with_cte("cb", body, columns: ["cname", "clab"])
      |> Selecto.select(["name"])

    assert refusal(statement) == {:cte_name_redefined, ["vv"]}
  end

  # A CTE named like the table p, its body over r with p's columns.
  defp fake_p(query, ctx) do
    Selecto.with_cte(
      query,
      ctx.t.p,
      fn -> q(ctx, r(ctx)) |> Selecto.select(["product_id", "title", "tenant_id"]) end,
      columns: ["id", "name", "tenant_id"]
    )
  end

  defp reviews_with_product(ctx, extra \\ %{}) do
    product = Map.merge(p(ctx).source, extra)

    r(ctx)
    |> put_in([:source, :associations], %{
      prod: %{queryable: :prod, field: :prod, owner_key: :product_id, related_key: :id}
    })
    |> Map.put(:schemas, %{prod: product})
    |> Map.put(:joins, %{prod: %{type: :left, name: "prod"}})
  end

  test "only an undeclared root reads a list of its name; joined and declared tables stay tables",
       ctx do
    # A joined schema over table p, and a CTE named p: refused.
    joined =
      q(ctx, reviews_with_product(ctx)) |> fake_p(ctx) |> Selecto.select(["title", "prod.name"])

    assert refusal(joined) == {:cte_name_collision, [ctx.t.p]}

    # A root declaring its schema is a table: refused.
    declared =
      q(ctx, put_in(p(ctx), [:source, :source_schema], "public"))
      |> fake_p(ctx)
      |> Selecto.select(["name"])

    assert refusal(declared) == {:cte_name_collision, [ctx.t.p]}

    # A body rooted on a declared table named like an earlier CTE: refused.
    body = dom("cte_cb", id: :integer, name: :string)

    nested =
      q(ctx, body)
      |> fake_p(ctx)
      |> Selecto.with_cte(
        "cte_cb",
        fn ->
          q(ctx, put_in(p(ctx), [:source, :source_schema], "public"))
          |> Selecto.select(["id", "name"])
        end,
        columns: ["id", "name"],
        dependencies: [ctx.t.p]
      )
      |> Selecto.select(["name"])

    assert refusal(nested) == {:cte_name_collision, [ctx.t.p]}

    # An undeclared root named like its own CTE.
    undeclared = q(ctx, p(ctx)) |> fake_p(ctx) |> Selecto.select(["name"])
    # It reads the list (README): no adapter here can tell a table of that name.
    assert outcome(undeclared) == [["pub-low"], ["pub-t1"], ["pub-t2"]]
  end

  defp tier(query, condition),
    do: Selecto.case_when_select(query, [{[condition], "HIGH"}], else: "LOW", as: "tier")

  test "a CASE subquery reads its table; a list named like it refuses", ctx do
    exists =
      q(ctx, r(ctx))
      |> Selecto.select(["id"])
      |> Selecto.filter({"product_id", 2})
      |> Selecto.filter({"rating", {:>=, 4}})

    for condition <- [
          {"id", {:subquery, :in, rated(ctx)}},
          {"id", "=", {:subquery, :any, rated(ctx)}},
          {:exists, exists}
        ] do
      query = q(ctx, p(ctx)) |> values_r(ctx) |> Selecto.select(["name"]) |> tier(condition)
      assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
    end

    control =
      q(ctx, p(ctx)) |> Selecto.select(["name"]) |> tier({"id", {:subquery, :in, rated(ctx)}})

    assert outcome(control) == [["a", "HIGH"], ["b", "LOW"]]
  end

  test "raw CTEs: unreferenced, joined, and named like a table read", ctx do
    plain = q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, "raw_one AS (SELECT 1 AS v)", []}])
    assert outcome(Selecto.select(plain, ["name"])) == [["a"], ["b"]]

    joined =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, "raw_two (rid, v) AS (SELECT 1, 7)", []}])
      |> Selecto.join(:rw,
        source: "raw_two",
        source_kind: :cte,
        type: :left,
        owner_key: :id,
        related_key: :rid,
        fields: %{"v" => %{type: :integer}}
      )
      |> Selecto.select(["name", "rw.v"])

    assert outcome(joined) == [["a", 7], ["b", nil]]

    # A raw CTE named like a table a filter subquery reads refuses.
    named =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, "#{ctx.t.r} AS (SELECT 2 AS product_id)", []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(named) == {:cte_name_collision, [ctx.t.r]}
  end

  test "an embedded query missing its required tenant refuses typed", ctx do
    inner =
      q(ctx, Map.put(dom(ctx.t.r, @rf, %{tenant_field: :tenant_id}), :tenant_required, true))
      |> Selecto.select(["product_id"])

    query =
      q(ctx, p(ctx))
      |> Selecto.filter({"id", {:subquery, :in, inner}})
      |> Selecto.select(["name"])

    assert {:error, %Selecto.Error{details: %{code: :tenant_scope_missing}}} =
             Selecto.execute(query)
  end

  defp raw_r(ctx),
    do: "SELECT 2 AS id, 2 AS product_id, 5 AS rating, 'RAW' AS title, 1 AS tenant_id"

  defp raw_p(_ctx), do: "SELECT 1 AS id, 'RAWP' AS name, 1 AS tenant_id"

  defp raw_texts(name, body) do
    [
      "/* c */ #{name} AS (#{body})",
      "-- c\n #{name} AS (#{body})",
      "x13 AS (SELECT 1 AS v), #{name} AS (#{body})",
      "`#{name}` AS (#{body})"
    ]
  end

  test "every name a raw CTE's text defines counts; unreadable text refuses", ctx do
    for text <- raw_texts(ctx.t.r, raw_r(ctx)) do
      query =
        q(ctx, p(ctx))
        |> Selecto.with_ctes([{:raw_cte, text, []}])
        |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
        |> Selecto.select(["name"])

      assert refusal(query) == {:cte_name_collision, [ctx.t.r]}, text
    end

    for text <- raw_texts(ctx.t.p, raw_p(ctx)) do
      query =
        q(ctx, reviews_with_product(ctx))
        |> Selecto.with_ctes([{:raw_cte, text, []}])
        |> Selecto.select(["title", "prod.name"])

      assert refusal(query) == {:cte_name_collision, [ctx.t.p]}, text
    end

    two = "x13 AS (SELECT 1 AS v), #{ctx.t.r} AS (#{raw_r(ctx)})"

    rooted =
      q(ctx, r(ctx)) |> Selecto.with_ctes([{:raw_cte, two, []}]) |> Selecto.select(["title"])

    # The root reads its own raw list (README).
    assert outcome(rooted) == [["RAW"]]

    unreadable = q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, "SELECT 1", []}])

    assert {:error, %Selecto.Error{details: %{reason: :raw_cte_unparsed}}} =
             Selecto.execute(Selecto.select(unreadable, ["name"]))
  end

  test "a query member the read does not use is not checked", ctx do
    member =
      q(ctx, Map.put(dom(ctx.t.r, @rf, %{tenant_field: :tenant_id}), :tenant_required, true))
      |> Selecto.select(["product_id"])

    domain = Map.put(p(ctx), :query_members, %{unused: %{query: member}})
    assert outcome(q(ctx, domain) |> Selecto.select(["name"])) == [["a"], ["b"]]
  end

  test "a mixed-case list root reads its list; a name an identifier cannot hold refuses", ctx do
    mixed =
      q(ctx, dom("MixedList", id: :integer, name: :string))
      |> Selecto.with_values([[1, "M"]], columns: ["id", "name"], as: "MixedList")
      |> Selecto.select(["name"])

    assert outcome(mixed) == [["M"]]

    cte =
      q(ctx, dom("MixedCte", id: :integer, name: :string))
      |> Selecto.with_cte("MixedCte", fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end,
        columns: ["id", "name"]
      )
      |> Selecto.select(["name"])

    assert outcome(cte) == [["a"], ["b"]]

    bad = ~S[bad"name]

    quoted =
      q(ctx, dom(bad, id: :integer, name: :string))
      |> Selecto.with_values([[1, "M"]], columns: ["id", "name"], as: bad)
      |> Selecto.select(["name"])

    assert refusal(quoted) == {:invalid_identifier, [bad]}
  end

  test "a raw CTE's iodata text with parameters is read and runs", ctx do
    raw = {:raw_cte, ["x14 AS (SELECT ", {:param, 1}, " AS pid)"], []}

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([raw])
      |> Selecto.join(:xj,
        source: "x14",
        source_kind: :cte,
        type: :left,
        owner_key: :id,
        related_key: :pid,
        fields: %{"pid" => %{type: :integer}}
      )
      |> Selecto.select(["name", "xj.pid"])

    assert outcome(query) == [["a", 1], ["b", nil]]
  end

  test "a mixed-case list named unquoted in host SQL reads the list", ctx do
    list = "MixF"

    query =
      q(ctx, p(ctx))
      |> Selecto.with_values([[1]], columns: ["pid"], as: list)
      |> Selecto.filter({:raw_sql_filter, ["selecto_root.id IN (SELECT pid FROM #{list})"]})
      |> Selecto.select(["name"])

    assert outcome(query) == [["a"]]

    # A raw CTE's unquoted mixed-case name and an ad-hoc join spelled the same
    # read one list.
    raw = {:raw_cte, "MixR AS (SELECT 1 AS id, 'RAWR' AS v)", []}

    joined =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([raw])
      |> Selecto.join(:mj,
        source: "MixR",
        type: :left,
        owner_key: :id,
        related_key: :id,
        fields: %{"v" => %{type: :string}}
      )
      |> Selecto.select(["name", "mj.v"])

    assert outcome(joined) == [["a", "RAWR"], ["b", nil]]
  end

  test "a $ inside a raw CTE's identifier does not hide the next definition", ctx do
    text = "x AS (SELECT 1 AS a$q$), #{ctx.t.r} AS (#{raw_r(ctx)})"

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, text, []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
  end

  test "an ad-hoc join on a list whose name an identifier cannot hold refuses typed", ctx do
    query =
      q(ctx, p(ctx))
      |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: "v$x")
      |> Selecto.join(:vj,
        source: "v$x",
        type: :left,
        owner_key: :id,
        related_key: :id,
        fields: %{"v" => %{type: :string}}
      )
      |> Selecto.select(["name", "vj.v"])

    assert refusal(query) == {:invalid_identifier, ["v$x"]}
  end

  test "raw text MySQL would read differently refuses", ctx do
    for text <- [
          ~S[r2 AS (SELECT "x" AS v)],
          "r2 AS (SELECT 1 AS v) # c",
          "r2 AS (SELECT 1 --c\n AS v)",
          "/*! r2 AS */ r3 AS (SELECT 1 AS v)",
          ~S[r2 AS (SELECT $t$ x $t$ AS v)]
        ] do
      query =
        q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, text, []}]) |> Selecto.select(["name"])

      assert {:error, %Selecto.Error{details: %{reason: :raw_cte_unparsed}}} =
               Selecto.execute(query),
             text
    end
  end

  test "a list named with a word any supported dialect reserves reads its rows", ctx do
    for name <- ~w(With Desc Window To Lateral Rank Rows Groups) do
      joined =
        q(ctx, p(ctx))
        |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: name)
        |> Selecto.join(:vj,
          source: name,
          type: :left,
          owner_key: :id,
          related_key: :id,
          fields: %{"v" => %{type: :string}}
        )
        |> Selecto.select(["name", "vj.v"])

      assert outcome(joined) == [["a", "V"], ["b", nil]], name

      rooted =
        q(ctx, dom(name, id: :integer, v: :string))
        |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: name)
        |> Selecto.select(["v"])

      assert outcome(rooted) == [["V"]], name

      cte =
        q(ctx, p(ctx))
        |> Selecto.with_cte(name, fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end,
          columns: ["id", "v"]
        )
        |> Selecto.join(:cj,
          source: name,
          type: :left,
          owner_key: :id,
          related_key: :id,
          fields: %{"v" => %{type: :string}}
        )
        |> Selecto.select(["name", "cj.v"])

      assert outcome(cte) == [["a", "a"], ["b", "b"]], name
    end
  end

  test "a non-ASCII identifier before $$ does not hide the next definition", ctx do
    text = "x AS (SELECT 1 AS ä$$), #{ctx.t.r} AS (#{raw_r(ctx)}), y AS (SELECT 2 AS ö$$)"

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, text, []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
  end

  test "a MariaDB executable comment in a raw CTE's text refuses", ctx do
    for open <- ["/*M!", "/*M!100100"] do
      text = "x AS (SELECT 1 AS v) #{open} , #{ctx.t.r} AS (#{raw_r(ctx)}) */"

      query =
        q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, text, []}]) |> Selecto.select(["name"])

      assert {:error, %Selecto.Error{details: %{reason: :raw_cte_unparsed}}} =
               Selecto.execute(query),
             open
    end
  end
end
