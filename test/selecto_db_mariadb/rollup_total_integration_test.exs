defmodule SelectoDBMariaDB.RollupTotalIntegrationTest do
  # WITH ROLLUP returns no row on empty input; PostgreSQL's ROLLUP returns the
  # grand total (COUNT 0, SUM NULL, every GROUPING bit 1). Selecto adds it.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  # MariaDB has no GROUPING().
  @grouping false

  test "an empty input returns the grand total row, a non-empty one is unchanged" do
    with_database(fn conn ->
      query = fn filters, order ->
        conn
        |> configure()
        |> Selecto.select(
          ["state", {:count, "*"}, {:sum, "total"}] ++
            if(@grouping, do: [{:grouping, ["state", "person_id"]}], else: [])
        )
        |> Selecto.group_by([{:rollup, ["state", "person_id"]}])
        |> Selecto.filter(filters)
        |> Selecto.order_by(order)
      end

      marker = fn bits -> if @grouping, do: [bits], else: [] end

      assert rows(query.([{"state", "none"}], [])) == [[nil, 0, nil] ++ marker.(3)]

      expected = [
        [nil, 3, Decimal.new(6)] ++ marker.(3),
        ["a", 1, Decimal.new(1)] ++ marker.(1),
        ["a", 1, Decimal.new(1)] ++ marker.(0),
        ["b", 2, Decimal.new(5)] ++ marker.(1),
        ["b", 2, Decimal.new(5)] ++ marker.(0)
      ]

      assert Enum.sort(rows(query.([], []))) == Enum.sort(expected)

      # Selecto's rollup order puts the NULL (rolled-up) key first ascending.
      ordered = rows(query.([], ["state"]))
      assert Enum.map(ordered, &hd/1) == [nil, "a", "a", "b", "b"]
      assert Enum.map(rows(query.([], [{"state", :desc}])), &hd/1) == [nil, "b", "b", "a", "a"]

      paged =
        query.([{"state", "none"}], [{"state", :desc}]) |> Selecto.limit(1) |> Selecto.offset(1)

      assert rows(paged) == []
    end)
  end

  test "explicit directions place NULL keys as PostgreSQL does, also on a page" do
    with_database(fn conn ->
      MyXQL.query!(conn, "INSERT INTO orders VALUES (4, NULL, 3, 4)", [], query_type: :text)

      query = fn order ->
        conn
        |> configure()
        |> Selecto.select(["state", {:count, "*"}])
        |> Selecto.group_by([{:rollup, ["state"]}])
        |> Selecto.order_by(order)
      end

      # Groups: a 1, b 2, NULL 1 (a real NULL state) and the grand total 4.
      # PostgreSQL: explicit ASC puts NULLs last, DESC and a bare term first.
      assert rows(query.([{"state", :asc}, {{:count, "*"}, :asc}])) ==
               [["a", 1], ["b", 2], [nil, 1], [nil, 4]]

      assert rows(query.([{"state", :desc}, {{:count, "*"}, :asc}])) ==
               [[nil, 1], [nil, 4], ["b", 2], ["a", 1]]

      assert rows(query.([{"state", :asc_nulls_first}, {{:count, "*"}, :asc}])) ==
               [[nil, 1], [nil, 4], ["a", 1], ["b", 2]]

      assert rows(query.([{"state", :desc_nulls_last}, {{:count, "*"}, :asc}])) ==
               [["b", 2], ["a", 1], [nil, 1], [nil, 4]]

      assert rows(query.(["state", {{:count, "*"}, :asc}])) ==
               [[nil, 1], [nil, 4], ["a", 1], ["b", 2]]

      page =
        query.([{"state", :asc}, {{:count, "*"}, :asc}])
        |> Selecto.limit(2)
        |> Selecto.offset(1)

      assert rows(page) == [["b", 2], [nil, 1]]
    end)
  end

  defp rows(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    rows
  end

  defp configure(conn) do
    domain = %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :state, :person_id, :total],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          state: %{type: :string},
          person_id: %{type: :integer},
          total: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, conn, adapter: Adapter, validate: false)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_rollup_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{database}`", [], query_type: :text)

    try do
      {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

      try do
        for sql <- [
              "CREATE TABLE orders (id INT PRIMARY KEY, state VARCHAR(5), person_id INT, total INT)",
              "INSERT INTO orders VALUES (1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'b', 2, 3)"
            ],
            do: MyXQL.query!(conn, sql, [], query_type: :text)

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
