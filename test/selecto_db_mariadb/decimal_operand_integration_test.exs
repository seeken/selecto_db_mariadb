defmodule SelectoDBMariaDB.DecimalOperandIntegrationTest do
  @moduledoc """
  Decimal filter values compare exactly, per operator, on indexed and plain
  columns of every numeric and text type (measured differential: Selecto's
  answer against the exact one computed here), and an indexed column keeps
  its index.
  """
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 180_000

  @adapter SelectoDBMariaDB.Adapter

  defp opts do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end

  @rows for id <- 1..200, do: id
  defp i_of(id), do: rem(id, 5)
  defp d_of(id), do: Decimal.mult(Decimal.new(rem(id, 7) - 3), Decimal.new("0.333333"))

  setup_all do
    {:ok, admin} = MyXQL.start_link(opts())

    db =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_decop_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, raw} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    MyXQL.query!(
      raw,
      "CREATE TABLE dfx (id INT PRIMARY KEY, i INT, ni INT, bu BIGINT UNSIGNED, y YEAR, b BIT(8), " <>
        "d DECIMAL(12,6), nd DECIMAL(12,6), x DOUBLE, v VARCHAR(20), " <>
        "KEY ki (i), KEY kbu (bu), KEY ky (y), KEY kb (b), KEY kd (d), KEY kx (x), KEY kv (v))",
      [],
      query_type: :text
    )

    values =
      Enum.map_join(@rows, ", ", fn id ->
        i = i_of(id)
        d = Decimal.to_string(d_of(id), :normal)
        "(#{id}, #{i}, #{i}, #{i}, #{2000 + i}, #{i}, #{d}, #{d}, #{d}, '#{d}')"
      end)

    MyXQL.query!(raw, "INSERT INTO dfx VALUES " <> values, [], query_type: :text)
    MyXQL.query!(raw, "ANALYZE TABLE dfx", [], query_type: :text)
    {:ok, conn} = @adapter.connect(Keyword.put(opts(), :database, db))

    on_exit(fn ->
      {:ok, a} = MyXQL.start_link(opts())
      MyXQL.query!(a, "DROP DATABASE IF EXISTS `#{db}`", [], query_type: :text)
    end)

    %{conn: conn, raw: raw}
  end

  @fields [
    i: :integer,
    ni: :integer,
    bu: :integer,
    y: :integer,
    b: :integer,
    d: :decimal,
    nd: :decimal,
    x: :float,
    v: :string
  ]

  defp domain do
    %{
      name: "dfx",
      schemas: %{},
      joins: %{},
      source: %{
        source_table: "dfx",
        primary_key: :id,
        fields: [:id | Keyword.keys(@fields)],
        redact_fields: [],
        columns: Map.new([{:id, :integer} | @fields], fn {k, t} -> {k, %{type: t}} end),
        associations: %{}
      }
    }
  end

  # The stored value of a column, as the server compares it with a Decimal:
  # integers and DECIMALs exactly, DOUBLE and VARCHAR as doubles.
  defp stored(:i, id), do: {:exact, Decimal.new(i_of(id))}
  defp stored(:ni, id), do: {:exact, Decimal.new(i_of(id))}
  defp stored(:bu, id), do: {:exact, Decimal.new(i_of(id))}
  defp stored(:b, id), do: {:exact, Decimal.new(i_of(id))}
  defp stored(:y, id), do: {:exact, Decimal.new(2000 + i_of(id))}
  defp stored(:d, id), do: {:exact, d_of(id)}
  defp stored(:nd, id), do: {:exact, d_of(id)}
  defp stored(:x, id), do: {:double, Decimal.to_float(d_of(id))}
  defp stored(:v, id), do: {:double, Decimal.to_float(d_of(id))}

  defp cmp({:exact, s}, v), do: Decimal.compare(s, v)

  defp cmp({:double, s}, v) do
    f = Decimal.to_float(v)

    cond do
      s < f -> :lt
      s > f -> :gt
      true -> :eq
    end
  end

  defp holds?(stored, {op, v}) when op in [:=, :!=, :<, :<=, :>, :>=] do
    c = cmp(stored, v)

    case op do
      := -> c == :eq
      :!= -> c != :eq
      :< -> c == :lt
      :<= -> c != :gt
      :> -> c == :gt
      :>= -> c != :lt
    end
  end

  defp holds?(stored, {:in, vs}), do: Enum.any?(vs, &(cmp(stored, &1) == :eq))
  defp holds?(stored, {:not_in, vs}), do: Enum.all?(vs, &(cmp(stored, &1) != :eq))

  defp holds?(stored, {:between, lo, hi}),
    do: cmp(stored, lo) != :lt and cmp(stored, hi) != :gt

  defp filter(field, {op, v}) when op in [:=, :!=, :<, :<=, :>, :>=],
    do: {to_string(field), {op, v}}

  defp filter(field, {:in, vs}), do: {to_string(field), {:in, vs}}
  defp filter(field, {:not_in, vs}), do: {to_string(field), {:not_in, vs}}
  defp filter(field, {:between, lo, hi}), do: {to_string(field), {:between, lo, hi}}

  @values [
    "1.5",
    "2",
    "2.4999",
    "-0.0000005",
    "0.333333",
    "0.3333330000000001",
    "-0.999999",
    "2001",
    "2000.5"
  ]

  defp conditions do
    vs =
      Enum.map(@values, &Decimal.new/1) ++ [Decimal.div(1, 3), Decimal.new(3), Decimal.new(2003)]

    for v <- vs,
        cond <- [
          {:=, v},
          {:!=, v},
          {:<, v},
          {:<=, v},
          {:>, v},
          {:>=, v},
          {:in, [v]},
          {:in, [v, Decimal.new("0.666666")]},
          {:not_in, [v]},
          {:between, v, Decimal.add(v, 1)}
        ],
        do: cond
  end

  # YEAR converts a value to a year before comparing (2 is the year 2002,
  # 1.5 rounds to it, 2000.5 is a year next to 2000) under any rendering, as
  # a literal does; it is checked with four-digit integral years. MySQL
  # compares a BIT column = a fractional Decimal rounded under any rendering,
  # as main did; BIT ranges are exact.
  defp checked?(:y, cond), do: integral?(cond) and four_digit?(cond)
  defp checked?(:b, {op, _v} = cond) when op in [:=, :!=], do: true or integral?(cond)
  defp checked?(:b, {op, _vs} = cond) when op in [:in, :not_in], do: true or integral?(cond)
  defp checked?(_field, _cond), do: true

  defp four_digit?(cond), do: Enum.all?(cond_values(cond), &(Decimal.compare(&1, 1901) != :lt))

  defp cond_values({_op, %Decimal{} = v}), do: [v]
  defp cond_values({_op, vs}) when is_list(vs), do: vs
  defp cond_values({:between, lo, hi}), do: [lo, hi]

  defp integral?({_op, %Decimal{} = v}), do: Decimal.integer?(v)
  defp integral?({_op, vs}) when is_list(vs), do: Enum.all?(vs, &Decimal.integer?/1)
  defp integral?({:between, lo, hi}), do: Decimal.integer?(lo) and Decimal.integer?(hi)

  test "every operator answers exactly on every column type, indexed or not", ctx do
    q = Selecto.configure(domain(), ctx.conn, adapter: @adapter, validate: false)

    wrong =
      for {field, _type} <- @fields, cond <- conditions(), checked?(field, cond), reduce: [] do
        acc ->
          want = for id <- @rows, holds?(stored(field, id), cond), do: id

          {:ok, {rows, _, _}} =
            q
            |> Selecto.filter(filter(field, cond))
            |> Selecto.select(["id"])
            |> Selecto.execute()

          got = rows |> List.flatten() |> Enum.sort()
          if got == want, do: acc, else: [{field, cond, length(got), length(want)} | acc]
      end

    assert wrong == []
  end

  test "a Decimal in host SQL text binds as the host wrote it", ctx do
    q = Selecto.configure(domain(), ctx.conn, adapter: @adapter, validate: false)
    v = Decimal.new("0.333333")
    want = for id <- @rows, rem(id, 7) == 4, do: id

    run = fn query ->
      {:ok, {rows, _, _}} = query |> Selecto.select(["id"]) |> Selecto.execute()
      rows |> List.flatten() |> Enum.sort()
    end

    # A host comparison as text: a rewritten operand would compare its own
    # text form (0.333333000000000000000000000000) instead.
    assert run.(q |> Selecto.filter({:raw_sql_filter, ["selecto_root.v LIKE ", {:param, v}]})) ==
             want

    assert run.(
             q
             |> Selecto.filter(
               {:raw_sql_filter, ["CONCAT(", {:param, v}, ", '') = selecto_root.v"]}
             )
           ) == want

    assert run.(
             q
             |> Selecto.filter(
               {:exists, "SELECT 1 FROM dfx z WHERE z.id = selecto_root.id AND z.v LIKE ?", [v]}
             )
           ) == want

    assert run.(
             q
             |> Selecto.with_ctes([
               {:raw_cte, "hc (rid) AS (SELECT id FROM dfx WHERE v LIKE ?)", [v]}
             ])
             |> Selecto.filter({:raw_sql_filter, "selecto_root.id IN (SELECT rid FROM hc)"})
           ) == want
  end

  test "an indexed column keeps its index for = and range comparisons", ctx do
    q = Selecto.configure(domain(), ctx.conn, adapter: @adapter, validate: false)

    for field <- [:i, :d],
        {op, v} <- [{:=, Decimal.new(2)}, {:=, Decimal.new("1.5")}, {:>=, Decimal.new("1.5")}] do
      {sql, params} =
        q
        |> Selecto.filter({to_string(field), {op, v}})
        |> Selecto.select(["id"])
        |> Selecto.to_sql()

      %MyXQL.Result{columns: columns, rows: [row | _]} =
        MyXQL.query!(ctx.raw, "EXPLAIN " <> sql, params)

      access = row |> Enum.at(Enum.find_index(columns, &(&1 == "type")))

      # An impossible equality (an integer column = 1.5) needs no access at all.
      allowed =
        if op == := and not Decimal.integer?(v),
          do: [nil, "ref", "range"],
          else: ["ref", "range"]

      assert access in allowed, "#{field} #{op} #{v}: #{inspect(access)}"
    end
  end
end
