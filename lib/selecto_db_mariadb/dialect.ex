defmodule SelectoDBMariaDB.Dialect do
  @moduledoc false

  @behaviour Selecto.DB.Dialect

  alias Selecto.Dialect.Collection.Operation, as: CollectionOperation
  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Predicate.Comparison
  alias Selecto.Dialect.Window.FrameBoundary
  alias Selecto.Dialect.TextSearch.{Predicate, Rank}
  alias Selecto.Dialect.TableFunction.Join, as: TableFunctionJoin

  @impl true
  def render_datetime_operation(%DateTimeOperation{operation: :format} = operation, _selecto) do
    if temporal_conversion_requested?(operation.options) do
      unsupported_datetime(operation)
    else
      {:ok,
       SelectoDBMariaDB.Adapter.format_datetime(
         operation.expression,
         Map.fetch!(operation.options, :format)
       )}
    end
  end

  def render_datetime_operation(%DateTimeOperation{} = operation, _selecto),
    do: unsupported_datetime(operation)

  @impl true
  def render_comparison(%Comparison{} = comparison, _selecto) do
    operator = if comparison.operation == :case_insensitive_not_like, do: "NOT LIKE", else: "LIKE"
    {:ok, ["LOWER(", comparison.left, ") ", operator, " LOWER(", comparison.right, ")"]}
  end

  alias Selecto.Dialect.Json.{
    ArrayContains,
    ArrayContainsAll,
    Contains,
    Extraction,
    KeyExists,
    Operation
  }

  @impl true
  def render_json_extraction(%Extraction{} = fragment, _selecto) do
    extraction = ["JSON_EXTRACT(", column_ref(fragment), ", '", json_path(fragment.path), "')"]
    extraction = if fragment.as_text, do: ["JSON_UNQUOTE(", extraction, ")"], else: extraction
    {:ok, cast_json(extraction, fragment.cast)}
  end

  @impl true
  def render_json_contains(%Contains{} = fragment, _selecto) do
    candidate = fragment.value |> Jason.encode!() |> string_literal()
    {:ok, ["JSON_CONTAINS(", column_ref(fragment), ", ", candidate, ")"]}
  end

  @impl true
  def render_json_key_exists(%KeyExists{} = fragment, _selecto) do
    {:ok,
     [
       "JSON_CONTAINS_PATH(",
       column_ref(fragment),
       ", 'one', '",
       json_path(fragment.path),
       "')"
     ]}
  end

  @impl true
  def render_json_array_contains(%ArrayContains{value: values} = fragment, selecto)
      when is_list(values) do
    combine(values, " OR ", fn value ->
      render_json_array_contains(%{fragment | value: value}, selecto)
    end)
  end

  def render_json_array_contains(%ArrayContains{} = fragment, _selecto) do
    candidate = fragment.value |> Jason.encode!() |> string_literal()

    {:ok,
     [
       "JSON_CONTAINS(",
       column_ref(fragment),
       ", ",
       candidate,
       ", '",
       json_path(fragment.path),
       "')"
     ]}
  end

  @impl true
  def render_json_array_contains_all(%ArrayContainsAll{} = fragment, selecto) do
    combine(fragment.values, " AND ", fn value ->
      render_json_array_contains(
        %ArrayContains{
          column: fragment.column,
          path: fragment.path,
          value: value,
          table_alias: fragment.table_alias
        },
        selecto
      )
    end)
  end

  @impl true
  def render_json_operation(%Operation{} = operation, selecto) do
    case operation.operation do
      :json_extract ->
        operation_extraction(operation, false, selecto)

      :json_extract_text ->
        operation_extraction(operation, true, selecto)

      :json_contains ->
        render_json_contains(operation_contains(operation), selecto)

      :json_contained ->
        unsupported_json(operation)

      kind when kind in [:json_exists, :json_path_exists] ->
        render_json_key_exists(operation_key_exists(operation), selecto)

      :json_agg ->
        json_array_aggregate(operation)

      :json_object_agg ->
        {:ok,
         [
           "JSON_OBJECTAGG(",
           operation_field(operation, :key_sql, operation.key_field),
           ", ",
           operation_field(operation, :value_sql, operation.value_field),
           ")"
         ]}

      :json_build_object ->
        {:ok, ["JSON_OBJECT(", operation_pairs(operation), ")"]}

      :json_build_array ->
        {:ok, ["JSON_ARRAY(", json_values(operation.value), ")"]}

      :json_empty_array ->
        {:ok, "JSON_ARRAY()"}

      :json_set ->
        {:ok, json_mutation("JSON_SET", operation)}

      :json_remove ->
        {:ok,
         [
           "JSON_REMOVE(",
           operation_column(operation),
           ", '",
           operation.path |> parse_operation_path() |> json_path(),
           "')"
         ]}

      :json_typeof ->
        {:ok, ["JSON_TYPE(", operation_column(operation), ")"]}

      :json_array_length ->
        {:ok, json_path_function("JSON_LENGTH", operation)}
    end
  end

  # MariaDB's JSON_ARRAYAGG takes ORDER BY and LIMIT, so ordered and
  # per-parent-limited collections aggregate directly (`:json_agg_limit`).
  defp json_array_aggregate(operation) do
    options = operation.options || %{}

    order_by =
      case Map.get(options, :order_by_sql) do
        order_by when order_by in [nil, []] -> []
        order_by -> [" ORDER BY ", order_by]
      end

    case Map.get(options, :limit) do
      nil ->
        {:ok, ["JSON_ARRAYAGG(", operation_column(operation), order_by, ")"]}

      limit when is_integer(limit) and limit > 0 ->
        {:ok,
         [
           "JSON_ARRAYAGG(",
           operation_column(operation),
           order_by,
           " LIMIT ",
           Integer.to_string(limit),
           ")"
         ]}

      _limit ->
        unsupported_json(operation)
    end
  end

  @impl true
  def render_collection_operation(%CollectionOperation{} = operation, _selecto) do
    case operation.operation do
      :array_agg when not operation.distinct and operation.order_by in [nil, []] ->
        {:ok, ["JSON_ARRAYAGG(", operation.column, ")"]}

      :string_agg ->
        delimiter = Map.get(operation.options, :delimiter, ",")
        distinct = if operation.distinct, do: "DISTINCT ", else: ""

        {:ok,
         {[
            "GROUP_CONCAT(",
            distinct,
            operation.column,
            collection_order_by(operation.order_by),
            " SEPARATOR ",
            {:param, delimiter},
            ")"
          ], [delimiter]}}

      kind when kind in [:array_contains, :array_overlap, :array_contained] ->
        array_predicate(kind, operation.column, operation.value)

      :unnest ->
        {:ok, json_table_source(operation.column)}

      unsupported ->
        unsupported_collection(unsupported)
    end
  end

  defp unsupported_collection(operation) do
    {:error,
     Selecto.Error.validation_error("MariaDB does not support this collection operation", %{
       operation: operation,
       unsupported_feature: :collection_operation
     })}
  end

  # Arrays are JSON arrays. NULL stays unknown and a non-array document never
  # matches. The bound values travel as one JSON document parameter, so element
  # types compare as JSON values ("1" is not 1).
  @array_scalar_types [:string, :integer, :float, :boolean]

  defp array_predicate(kind, column, values) when is_list(values) do
    with true <- Enum.all?(values, &array_scalar?/1),
         {:ok, encoded} <- Jason.encode(values) do
      bound = {:param, encoded}

      predicate =
        case kind do
          :array_contains -> ["JSON_CONTAINS(", column, ", ", bound, ")"]
          :array_overlap -> ["JSON_OVERLAPS(", column, ", ", bound, ")"]
          :array_contained -> ["JSON_CONTAINS(", bound, ", ", column, ")"]
        end

      {:ok,
       [
         "(CASE WHEN ",
         column,
         " IS NULL THEN NULL WHEN JSON_TYPE(",
         column,
         ") = 'ARRAY' THEN ",
         predicate,
         " ELSE 0 END)"
       ]}
    else
      _ -> unsupported_collection(kind)
    end
  end

  defp array_predicate(kind, _column, _values), do: unsupported_collection(kind)

  defp array_scalar?(value),
    do: Enum.any?(@array_scalar_types, &array_scalar_type?(&1, value))

  defp array_scalar_type?(:string, value), do: is_binary(value)
  defp array_scalar_type?(:integer, value), do: is_integer(value)
  defp array_scalar_type?(:float, value), do: is_float(value)
  defp array_scalar_type?(:boolean, value), do: is_boolean(value)

  # JSON_TABLE expands a JSON array in element order; render_table_function_join
  # adds the requested 1-based ordinality column to exactly this shape. MariaDB
  # has no JSON value type to decode, so elements are their unquoted scalar
  # text (strings exact; numbers and booleans as text; documents NULL).
  defp json_table_source(column),
    do: ["JSON_TABLE(", column, ", '$[*]' COLUMNS (", "value LONGTEXT PATH '$'))"]

  # Canonical computed-value casts. A decimal keeps 10 fractional digits so
  # decimal division keeps its fraction (division adds 4 more), while results
  # stay within the 34 significant digits MyXQL's decimal decoding accepts;
  # strings compare by code point, as the fixture tables' binary collation does. MariaDB has no boolean cast target,
  # so a boolean cast fails closed rather than reinterpreting text as 0.
  @computed_casts %{
    "string" => "CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_bin",
    "integer" => "SIGNED)",
    "decimal" => "DECIMAL(65, 10))",
    "date" => "DATE)",
    "utc_datetime" => "DATETIME(6))"
  }

  @impl true
  def render_computed_value(
        %{
          __struct__: Selecto.Dialect.ComputedValue,
          operation: :cast,
          expression: expression,
          type: type,
          path: []
        } = fragment,
        _selecto
      )
      when map_size(fragment) == 5 do
    with {:ok, target} <- Map.fetch(@computed_casts, type),
         true <- computed_expression?(expression) do
      {:ok, ["CAST(", expression, " AS ", target]}
    else
      _ -> invalid_computed_value(:cast)
    end
  end

  # JSON null and an absent path are SQL NULL; strings are unquoted, other
  # scalars use their JSON spelling. Each bound path segment stays a marker.
  def render_computed_value(
        %{
          __struct__: Selecto.Dialect.ComputedValue,
          operation: :json_text,
          expression: expression,
          type: nil,
          path: [_ | _] = path
        } = fragment,
        _selecto
      )
      when map_size(fragment) == 5 do
    if computed_expression?(expression) and Enum.all?(path, &computed_path_marker?/1) do
      path_sql = [
        "CONCAT('$', ",
        path |> Enum.map(&path_segment/1) |> Enum.intersperse(", "),
        ")"
      ]

      extracted = ["JSON_EXTRACT(", expression, ", ", path_sql, ")"]

      {:ok,
       [
         "(CASE WHEN JSON_TYPE(",
         extracted,
         ") = 'NULL' THEN NULL ELSE JSON_UNQUOTE(",
         extracted,
         ") END)"
       ]}
    else
      invalid_computed_value(:json_text)
    end
  end

  def render_computed_value(_fragment, _selecto), do: invalid_computed_value(:unknown)

  defp path_segment({:param, value} = marker) do
    if Regex.match?(~r/\A[0-9]+\z/, value),
      do: ["'[', ", marker, ", ']'"],
      else: ["'.\"', ", marker, ", '\"'"]
  end

  defp computed_expression?(expression),
    do: computed_sql?(expression) and computed_sql_present?(expression)

  defp computed_sql?(value) when is_binary(value), do: true
  defp computed_sql?(value) when is_integer(value), do: value in 0..255
  defp computed_sql?({:param, _value}), do: true
  defp computed_sql?(value) when is_list(value), do: Enum.all?(value, &computed_sql?/1)
  defp computed_sql?(_value), do: false

  defp computed_sql_present?(value) when is_binary(value), do: byte_size(value) > 0

  defp computed_sql_present?(value) when is_list(value),
    do: Enum.any?(value, &computed_sql_present?/1)

  defp computed_sql_present?(_value), do: true

  defp computed_path_marker?({:param, segment}) when is_binary(segment),
    do: Regex.match?(~r/\A[A-Za-z0-9_]+\z/, segment)

  defp computed_path_marker?(_marker), do: false

  defp invalid_computed_value(operation) do
    {:error,
     Selecto.Error.validation_error("Invalid MariaDB computed-value fragment", %{
       operation: operation,
       unsupported_feature: :computed_value
     })}
  end

  # MariaDB has no LATERAL derived tables; JSON_TABLE may still read columns
  # of the tables before it.
  @impl true
  def render_table_function_join(
        %TableFunctionJoin{
          join_type: join_type,
          source_sql: ["JSON_TABLE(", column, ", '$[*]' COLUMNS (", "value LONGTEXT PATH '$'))"]
        } = join,
        _selecto
      )
      when join_type in [:cross, :inner, :left] do
    columns =
      case join.ordinality_alias do
        nil -> []
        ordinality -> [quote_field(ordinality), " FOR ORDINALITY, "]
      end

    source = [
      "JSON_TABLE(",
      column,
      ", '$[*]' COLUMNS (",
      columns,
      "value LONGTEXT PATH '$')) AS ",
      quote_field(join.alias)
    ]

    case join_type do
      :cross -> {:ok, ["CROSS JOIN ", source]}
      :inner -> {:ok, ["INNER JOIN ", source, " ON TRUE"]}
      :left -> {:ok, ["LEFT JOIN ", source, " ON TRUE"]}
    end
  end

  # A json_each-style rowset over a JSON array: one row per element, its
  # document in `value`. JSON_TABLE requires a literal path, so only a plain
  # `$`, `.member` and `[index]` path is inlined; anything else is refused.
  def render_table_function_join(
        %TableFunctionJoin{
          source_kind: :json_rowset,
          join_type: join_type,
          options: %{function: :json_each, source: source, path: path}
        } = join,
        _selecto
      )
      when join_type in [:inner, :left] and is_binary(source) do
    with {:ok, path} <- json_rowset_path(path),
         true <- Regex.match?(~r/\A[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\z/, source) do
      join_sql = if join_type == :left, do: "LEFT JOIN ", else: "INNER JOIN "

      {:ok,
       [
         join_sql,
         "JSON_TABLE(",
         source,
         ", '",
         path,
         "[*]' COLUMNS (value JSON PATH '$')) AS ",
         quote_field(join.alias),
         " ON TRUE"
       ]}
    else
      _ -> unsupported_join(:json_rowset)
    end
  end

  def render_table_function_join(%TableFunctionJoin{} = join, _selecto),
    do: unsupported_join(join.source_kind)

  defp json_rowset_path(nil), do: {:ok, "$"}

  defp json_rowset_path(path) when is_binary(path) do
    if Regex.match?(~r/\A\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])*\z/, path),
      do: {:ok, path},
      else: :error
  end

  defp json_rowset_path(_path), do: :error

  defp unsupported_join(kind) do
    {:error,
     Selecto.Error.validation_error("MariaDB does not support this lateral join", %{
       join: kind,
       unsupported_feature: :table_function_join
     })}
  end

  @impl true
  def render_text_search_predicate(%Predicate{selectors: []}, _selecto) do
    text_search_error("MariaDB text search requires at least one field")
  end

  def render_text_search_predicate(%Predicate{} = predicate, _selecto) do
    with {:ok, mode_sql} <- mode_sql(predicate.mode),
         {:ok, query_sql} <- search_query_sql(predicate.query, predicate.mode) do
      {:ok,
       [
         " MATCH(",
         Enum.intersperse(predicate.selectors, ", "),
         ") AGAINST (",
         query_sql,
         mode_sql,
         ") "
       ]}
    end
  end

  # Governed prefix lookups bind normalized `word:* & word:*` text. Boolean
  # mode requires every word (`+word*`); the regex admits no boolean operator,
  # quote or parenthesis, and the text stays a bound parameter.
  defp search_query_sql(text, :prefix) when is_binary(text) and byte_size(text) <= 4096 do
    if Regex.match?(~r/\A[\p{L}\p{N}_]+:\*(?: & [\p{L}\p{N}_]+:\*)*\z/u, text),
      do: {:ok, ["CONCAT('+', REPLACE(REPLACE(", {:param, text}, ", ':*', '*'), ' & ', ' +'))"]},
      else: text_search_error("MariaDB prefix search requires normalized word prefixes")
  end

  defp search_query_sql(_text, :prefix),
    do: text_search_error("MariaDB prefix search requires normalized word prefixes")

  defp search_query_sql(text, _mode), do: {:ok, {:param, text}}

  @impl true
  def render_text_search_rank(%Rank{} = rank, selecto) do
    with :ok <- require_fields(rank.fields),
         :ok <- require_query(rank.query),
         :ok <- reject_weights(rank.weights),
         :ok <- validate_fields(selecto, rank.fields),
         {:ok, mode_sql} <- mode_sql(rank.mode),
         {:ok, query_sql} <- search_query_sql(rank.query, rank.mode) do
      field_sql =
        rank.fields
        |> Enum.map(&qualified_root_field(selecto, &1))
        |> Enum.intersperse(", ")

      # A field selector, like PostgreSQL's, so callers can also order by the
      # rank expression (governed co-domain lookups do).
      {:ok,
       {:field,
        {:custom_sql, ["MATCH(", field_sql, ") AGAINST (", query_sql, mode_sql, ")"], %{}},
        rank.alias}}
    end
  end

  defp mode_sql(mode) when mode in [nil, :natural, :websearch, :plain],
    do: {:ok, " IN NATURAL LANGUAGE MODE"}

  defp mode_sql(mode) when mode in [:boolean, :prefix], do: {:ok, " IN BOOLEAN MODE"}

  defp mode_sql(:query_expansion),
    do: {:ok, " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"}

  defp mode_sql(mode),
    do: text_search_error("MariaDB does not support this text-search mode", mode: mode)

  defp require_fields([]),
    do: text_search_error("MariaDB text_search_rank/3 requires at least one field")

  defp require_fields(_fields), do: :ok

  defp require_query(nil),
    do: text_search_error("MariaDB text_search_rank/3 requires a :query option")

  defp require_query(_query), do: :ok

  defp reject_weights([]), do: :ok

  defp reject_weights(_weights),
    do: text_search_error("MariaDB text_search_rank/3 does not support :weights yet")

  defp validate_fields(selecto, fields) do
    missing = Enum.reject(fields, &root_column_conf(selecto, &1))

    if missing == [],
      do: :ok,
      else: text_search_error("MariaDB text_search_rank/3 field not found", fields: missing)
  end

  defp qualified_root_field(selecto, field) do
    [
      Selecto.Builder.Sql.Helpers.quote_identifier(selecto, "selecto_root"),
      ".",
      Selecto.Builder.Sql.Helpers.quote_identifier(selecto, field)
    ]
  end

  defp root_column_conf(selecto, field) do
    columns = selecto |> Map.get(:config, %{}) |> Map.get(:columns, %{})
    Map.get(columns, field) || Map.get(columns, safe_existing_atom(field))
  end

  defp safe_existing_atom(field) when is_binary(field) do
    try do
      String.to_existing_atom(field)
    rescue
      ArgumentError -> nil
    end
  end

  defp safe_existing_atom(_field), do: nil

  defp text_search_error(message, details \\ []) do
    {:error,
     Selecto.Error.validation_error(
       message,
       Map.new([unsupported_feature: :text_search] ++ details)
     )}
  end

  @impl true
  def render_window_frame_boundary(%FrameBoundary{} = boundary, _selecto) do
    {:ok,
     [
       "INTERVAL ",
       boundary.amount,
       " ",
       boundary.unit |> Atom.to_string() |> String.upcase(),
       " ",
       boundary.direction |> Atom.to_string() |> String.upcase()
     ]}
  end

  defp operation_extraction(operation, as_text, selecto) do
    render_json_extraction(
      %Extraction{
        column: operation.column,
        path: parse_operation_path(operation.path),
        as_text: as_text,
        table_alias: operation.table_alias
      },
      selecto
    )
  end

  defp operation_contains(operation) do
    %Contains{
      column: operation.column,
      value: operation.value,
      table_alias: operation.table_alias
    }
  end

  defp operation_key_exists(operation) do
    %KeyExists{
      column: operation.column,
      path: parse_operation_path(operation.path),
      table_alias: operation.table_alias
    }
  end

  defp json_mutation(function, operation) do
    [
      function,
      "(",
      operation_column(operation),
      ", '",
      operation.path |> parse_operation_path() |> json_path(),
      "', ",
      json_value(operation.value),
      ")"
    ]
  end

  defp json_path_function(function, %{path: nil} = operation),
    do: [function, "(", operation_column(operation), ")"]

  defp json_path_function(function, operation) do
    [
      function,
      "(",
      operation_column(operation),
      ", '",
      operation.path |> parse_operation_path() |> json_path(),
      "')"
    ]
  end

  defp object_pairs(pairs) when is_list(pairs) do
    pairs
    |> Enum.map(fn {key, value} -> [json_value(to_string(key)), ", ", json_value(value)] end)
    |> Enum.intersperse(", ")
  end

  defp json_values(values) when is_list(values),
    do: values |> Enum.map(&json_value/1) |> Enum.intersperse(", ")

  defp json_value(value) when is_binary(value), do: string_literal(value)
  defp json_value(value) when is_integer(value), do: Integer.to_string(value)
  defp json_value(value) when is_float(value), do: Float.to_string(value)
  defp json_value(true), do: "true"
  defp json_value(false), do: "false"
  defp json_value(nil), do: "null"

  defp json_value(value) when is_map(value) or is_list(value),
    do: ["CAST(", value |> Jason.encode!() |> string_literal(), " AS JSON)"]

  defp json_value(value), do: value |> inspect() |> string_literal()

  defp operation_column(%{options: options} = operation) when is_map(options),
    do:
      Map.get(options, :column_sql) ||
        operation_column_ref(operation.column, operation.table_alias)

  defp operation_column(operation),
    do: operation_column_ref(operation.column, operation.table_alias)

  defp operation_column_ref(column, nil), do: quote_field(column)

  defp operation_column_ref(column, table_alias),
    do: [quote_field(table_alias), ".", quote_field(column)]

  defp operation_field(operation, key, field),
    do: Map.get(operation.options || %{}, key) || quote_field(field)

  defp operation_pairs(operation),
    do: Map.get(operation.options || %{}, :pairs_sql) || object_pairs(operation.value)

  defp column_ref(fragment), do: operation_column(fragment)
  defp quote_field(field), do: SelectoDBMariaDB.Adapter.quote_identifier(field)

  defp collection_order_by(order_by) when order_by in [nil, []], do: []

  defp collection_order_by(order_by) do
    [
      " ORDER BY ",
      order_by
      |> Enum.map(fn {expression, direction} ->
        [expression, " ", direction |> Atom.to_string() |> String.upcase()]
      end)
      |> Enum.intersperse(", ")
    ]
  end

  defp json_path(path) do
    path
    |> Enum.reduce("$", fn segment, acc ->
      case Integer.parse(to_string(segment)) do
        {index, ""} -> acc <> "[#{index}]"
        _ -> acc <> "." <> safe_segment!(segment)
      end
    end)
    |> escape_literal()
  end

  defp parse_operation_path(nil), do: []

  defp parse_operation_path(path) do
    path
    |> String.replace_prefix("$.", "")
    |> String.split(~r/[\.\[\]]/, trim: true)
  end

  defp safe_segment!(segment) do
    segment = to_string(segment)

    if Regex.match?(~r/\A[A-Za-z_][A-Za-z0-9_]*\z/, segment),
      do: segment,
      else: raise(ArgumentError, "invalid JSON path segment: #{inspect(segment)}")
  end

  defp cast_json(extraction, nil), do: extraction
  defp cast_json(extraction, :integer), do: ["CAST(", extraction, " AS SIGNED)"]
  defp cast_json(extraction, :decimal), do: ["CAST(", extraction, " AS DECIMAL(38, 10))"]
  defp cast_json(extraction, :float), do: ["CAST(", extraction, " AS DOUBLE)"]
  defp cast_json(extraction, :boolean), do: ["CAST(", extraction, " AS UNSIGNED)"]
  defp cast_json(extraction, :date), do: ["CAST(", extraction, " AS DATE)"]

  defp cast_json(extraction, cast) when cast in [:datetime, :utc_datetime],
    do: ["CAST(", extraction, " AS DATETIME)"]

  defp cast_json(_extraction, cast),
    do: raise(ArgumentError, "unsupported MariaDB JSON cast: #{inspect(cast)}")

  defp combine(values, separator, renderer) do
    values
    |> Enum.map(fn value -> renderer.(value) |> elem(1) end)
    |> Enum.intersperse(separator)
    |> then(&{:ok, &1})
  end

  defp unsupported_json(operation) do
    {:error,
     Selecto.Error.validation_error("MariaDB does not support this JSON operation", %{
       operation: operation.operation,
       unsupported_feature: :json_operation
     })}
  end

  defp temporal_conversion_requested?(options) do
    Map.get(options, :epoch_storage) not in [nil, false] or
      Map.get(options, :timezone) not in [nil, ""]
  end

  defp unsupported_datetime(operation) do
    {:error,
     Selecto.Error.validation_error("MariaDB does not support this datetime operation", %{
       operation: operation.operation,
       unsupported_feature: :datetime_operation
     })}
  end

  # JSON paths contain only validated identifier segments and array indexes.
  defp escape_literal(value), do: value |> to_string() |> String.replace("'", "''")

  # A hexadecimal literal has no quote or escape characters, so its meaning does
  # not depend on NO_BACKSLASH_ESCAPES or any other sql_mode. The introducer
  # keeps it a utf8mb4 character string rather than a binary string.
  defp string_literal(value) when is_binary(value) do
    if String.valid?(value),
      do: ["_utf8mb4 X'", Base.encode16(value), "'"],
      else: raise(ArgumentError, "MariaDB JSON string values must be valid UTF-8")
  end
end
