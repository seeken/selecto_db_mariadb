defmodule SelectoDBMariaDB.Dialect do
  @moduledoc false

  @behaviour Selecto.DB.Dialect

  alias Selecto.Dialect.Collection.Operation, as: CollectionOperation
  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Predicate.Comparison
  alias Selecto.Dialect.Window.FrameBoundary

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
    encoded = fragment.value |> Jason.encode!() |> escape_literal()
    {:ok, ["JSON_CONTAINS(", column_ref(fragment), ", '", encoded, "')"]}
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
    candidate = fragment.value |> Jason.encode!() |> escape_literal()

    {:ok,
     [
       "JSON_CONTAINS(",
       column_ref(fragment),
       ", '",
       candidate,
       "', '",
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
        {:ok, ["JSON_ARRAYAGG(", operation_column(operation), ")"]}

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

      unsupported ->
        {:error,
         Selecto.Error.validation_error("MariaDB does not support this collection operation", %{
           operation: unsupported,
           unsupported_feature: :collection_operation
         })}
    end
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

  defp json_value(value) when is_binary(value), do: ["'", escape_literal(value), "'"]
  defp json_value(value) when is_integer(value), do: Integer.to_string(value)
  defp json_value(value) when is_float(value), do: Float.to_string(value)
  defp json_value(true), do: "true"
  defp json_value(false), do: "false"
  defp json_value(nil), do: "null"

  defp json_value(value) when is_map(value) or is_list(value),
    do: ["CAST('", value |> Jason.encode!() |> escape_literal(), "' AS JSON)"]

  defp json_value(value), do: ["'", value |> inspect() |> escape_literal(), "'"]

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

  defp escape_literal(value), do: value |> to_string() |> String.replace("'", "''")
end
