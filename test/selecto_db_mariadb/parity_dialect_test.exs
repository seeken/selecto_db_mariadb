defmodule SelectoDBMariaDB.ParityDialectTest do
  @moduledoc "Renderers added for MySQL/MariaDB certification parity (MariaDB)."
  use ExUnit.Case, async: true

  alias Selecto.Dialect.Collection.Operation, as: CollectionOperation
  alias Selecto.Dialect.ComputedValue
  alias Selecto.Dialect.Json.Operation, as: JsonOperation
  alias Selecto.Dialect.TableFunction.Join
  alias Selecto.Dialect.TextSearch.Predicate
  alias SelectoDBMariaDB.Dialect

  defp sql(iodata), do: iodata |> List.wrap() |> List.flatten() |> render()

  defp render(parts),
    do:
      Enum.map_join(parts, fn
        {:param, value} -> "<#{value}>"
        part -> to_string(part)
      end)

  test "computed casts have explicit MariaDB targets and no boolean cast" do
    cast = fn type -> %ComputedValue{operation: :cast, expression: "x", type: type} end

    assert {:ok, decimal} = Dialect.render_computed_value(cast.("decimal"), %{})
    assert sql(decimal) == "CAST(x AS DECIMAL(65, 10))"

    assert {:ok, string} = Dialect.render_computed_value(cast.("string"), %{})
    assert sql(string) == "CAST(x AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_bin"

    assert {:error, _} = Dialect.render_computed_value(cast.("boolean"), %{})
  end

  test "json_text binds every path segment and maps JSON null to NULL" do
    fragment = %ComputedValue{
      operation: :json_text,
      expression: "doc",
      path: [{:param, "items"}, {:param, "0"}]
    }

    assert {:ok, rendered} = Dialect.render_computed_value(fragment, %{})
    text = sql(rendered)

    assert text =~
             "JSON_TYPE(JSON_EXTRACT(doc, CONCAT('$', '.\"', <items>, '\"', '[', <0>, ']'))) = 'NULL'"

    unsafe = %{fragment | path: [{:param, "a'b"}]}
    assert {:error, _} = Dialect.render_computed_value(unsafe, %{})
  end

  test "array predicates bind one JSON document and never match non-arrays" do
    op = %CollectionOperation{
      operation: :array_contained,
      clause: :filter,
      column: "tags",
      value: ["a", "z"]
    }

    assert {:ok, rendered} = Dialect.render_collection_operation(op, %{})

    assert sql(rendered) ==
             "(CASE WHEN tags IS NULL THEN NULL WHEN JSON_TYPE(tags) = 'ARRAY' THEN " <>
               "JSON_CONTAINS(<[\"a\",\"z\"]>, tags) ELSE 0 END)"

    nested = %{op | value: [%{"k" => 1}]}
    assert {:error, _} = Dialect.render_collection_operation(nested, %{})
  end

  test "unnest joins JSON_TABLE with the requested ordinality column" do
    assert {:ok, source} =
             Dialect.render_collection_operation(
               %CollectionOperation{operation: :unnest, clause: :table, column: "tags"},
               %{}
             )

    join = %Join{join_type: :cross, source_sql: source, alias: "t", ordinality_alias: "pos"}
    assert {:ok, rendered} = Dialect.render_table_function_join(join, %{})

    assert sql(rendered) ==
             "CROSS JOIN JSON_TABLE(tags, '$[*]' COLUMNS (`pos` FOR ORDINALITY, value LONGTEXT PATH '$')) AS `t`"
  end

  test "MariaDB has no LATERAL derived tables" do
    join = %Join{join_type: :left, source_sql: ["(q)"], alias: "m", source_kind: :subquery}
    assert {:error, _} = Dialect.render_table_function_join(join, %{})
  end

  test "JSON rowsets inline only plain member and index paths" do
    join = %Join{
      join_type: :left,
      source_sql: [],
      alias: "items",
      source_kind: :json_rowset,
      options: %{function: :json_each, source: "selecto_root.payload", path: "$.items"}
    }

    assert {:ok, rendered} = Dialect.render_table_function_join(join, %{})
    assert sql(rendered) =~ "JSON_TABLE(selecto_root.payload, '$.items[*]' COLUMNS"

    for path <- ["$.a'b", "$.a\\'", "$..a", "lax $.a"] do
      hostile = put_in(join.options.path, path)
      assert {:error, _} = Dialect.render_table_function_join(hostile, %{})
    end
  end

  test "ordered and limited collection aggregates use JSON_ARRAYAGG clauses" do
    op = %JsonOperation{
      operation: :json_agg,
      clause: :select,
      options: %{column_sql: "v", order_by_sql: ["t.id ASC"], limit: 3}
    }

    assert {:ok, rendered} = Dialect.render_json_operation(op, %{})
    assert sql(rendered) == "JSON_ARRAYAGG(v ORDER BY t.id ASC LIMIT 3)"

    assert {:error, _} =
             Dialect.render_json_operation(%{op | options: %{op.options | limit: 0}}, %{})
  end

  test "prefix lookups bind normalized words and refuse boolean operators" do
    predicate = %Predicate{
      fields: ["name"],
      selectors: ["name"],
      query: "ada:* & lov:*",
      mode: :prefix
    }

    assert {:ok, rendered} = Dialect.render_text_search_predicate(predicate, %{})

    assert sql(rendered) =~
             "AGAINST (CONCAT('+', REPLACE(REPLACE(<ada:* & lov:*>, ':*', '*'), ' & ', ' +')) IN BOOLEAN MODE)"

    for query <- ["ada*", "-ada:*", "ada:* | b:*", "\"ada\":*"] do
      assert {:error, _} =
               Dialect.render_text_search_predicate(%{predicate | query: query}, %{})
    end
  end
end
