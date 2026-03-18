defmodule SelectoDBMariaDB.Adapter do
  @moduledoc """
  MariaDB adapter for Selecto backed by `MyXQL`.
  """

  @behaviour Selecto.DB.Adapter

  @missing_dependency {:adapter_dependency_missing, :myxql}

  @impl true
  def name, do: :mariadb

  @impl true
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    if dependency_available?() do
      case MyXQL.start_link(opts) do
        {:ok, conn} -> {:ok, conn}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, @missing_dependency}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def execute(connection, query, params, opts) do
    if is_map(connection) and Map.has_key?(connection, :adapter) and
         Map.has_key?(connection, :connection) do
      execute_direct(Map.get(connection, :connection), query, params, opts)
    else
      execute_direct(connection, query, params, opts)
    end
  end

  defp execute_direct(connection, query, params, opts) do
    if dependency_available?() do
      case MyXQL.query(connection, normalize_query(query), params, opts) do
        {:ok, result} -> {:ok, normalize_result(result)}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, @missing_dependency}
    end
  end

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "`", "``")
    "`#{escaped}`"
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def supports?(feature),
    do:
      feature in [
        :cte,
        :window_functions,
        :transactions,
        :rollup,
        :rollup_with_rollup,
        :schema_introspection
      ]

  @impl true
  def rollup_sql(grouped_clauses), do: [grouped_clauses, " with rollup"]

  @impl true
  def list_tables(connection, opts \\ []) do
    with {:ok, schema} <- resolve_schema(connection, opts) do
      query = """
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
        AND table_type = 'BASE TABLE'
      ORDER BY table_name
      """

      case introspection_query(connection, query, [schema]) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [table_name] -> table_name end)}
        {:error, reason} -> {:error, {:query_failed, reason}}
      end
    end
  end

  @impl true
  def introspect_table(connection, table_name, opts \\ []) do
    include_associations = Keyword.get(opts, :include_associations, true)
    expand = Keyword.get(opts, :expand, false)

    with {:ok, schema} <- resolve_schema(connection, opts),
         {:ok, columns} <- get_columns(connection, table_name, schema),
         {:ok, primary_key} <- get_primary_key(connection, table_name, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema) do
      fields = Enum.map(columns, & &1.column_name)

      field_types =
        Enum.into(columns, %{}, fn column ->
          {column.column_name, map_mysql_type(column.data_type, column.column_type)}
        end)

      associations =
        cond do
          not include_associations ->
            %{}

          expand ->
            case build_expanded_associations(connection, table_name, schema, primary_key) do
              {:ok, expanded_associations} -> expanded_associations
              {:error, _reason} -> build_associations(foreign_keys)
            end

          true ->
            build_associations(foreign_keys)
        end

      column_metadata =
        Enum.into(columns, %{}, fn column ->
          {column.column_name,
           %{
             type: Map.get(field_types, column.column_name),
             nullable: column.is_nullable == "YES",
             default: column.column_default,
             max_length: column.character_maximum_length,
             precision: column.numeric_precision || column.datetime_precision,
             scale: column.numeric_scale
           }}
        end)

      {:ok,
       %{
         table_name: table_name,
         schema: schema,
         fields: fields,
         field_types: field_types,
         primary_key: primary_key,
         associations: associations,
         columns: column_metadata,
         source: :mariadb
       }}
    end
  end

  defp dependency_available? do
    Code.ensure_loaded?(MyXQL) and function_exported?(MyXQL, :start_link, 1) and
      function_exported?(MyXQL, :query, 4)
  end

  defp introspection_query(%{query_fun: query_fun}, query, params)
       when is_function(query_fun, 3) do
    query_fun.(query, params, prepared: false)
  end

  defp introspection_query(connection, query, params) do
    execute(connection, query, params, prepared: false)
  end

  defp resolve_schema(connection, opts) do
    case Keyword.get(opts, :schema) do
      nil -> current_database(connection)
      "" -> current_database(connection)
      "public" -> current_database(connection)
      schema -> {:ok, schema}
    end
  end

  defp current_database(connection) do
    case introspection_query(connection, "SELECT DATABASE()", []) do
      {:ok, %{rows: [[database] | _]}} when is_binary(database) and database != "" ->
        {:ok, database}

      {:ok, _result} ->
        {:error, :missing_database_name}

      {:error, reason} ->
        {:error, {:database_name_query_failed, reason}}
    end
  end

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp get_columns(connection, table_name, schema) do
    query = """
    SELECT
      column_name,
      data_type,
      column_type,
      is_nullable,
      column_default,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      datetime_precision,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ?
    ORDER BY ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             column_name,
                             data_type,
                             column_type,
                             is_nullable,
                             column_default,
                             max_length,
                             precision,
                             scale,
                             datetime_precision,
                             _ordinal_position
                           ] ->
           %{
             column_name: String.to_atom(column_name),
             data_type: data_type,
             column_type: column_type,
             is_nullable: is_nullable,
             column_default: column_default,
             character_maximum_length: max_length,
             numeric_precision: precision,
             numeric_scale: scale,
             datetime_precision: datetime_precision
           }
         end)}

      {:error, reason} ->
        {:error, {:columns_query_failed, reason}}
    end
  end

  defp get_primary_key(connection, table_name, schema) do
    query = """
    SELECT column_name
    FROM information_schema.key_column_usage
    WHERE table_schema = ?
      AND table_name = ?
      AND constraint_name = 'PRIMARY'
    ORDER BY ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[single_key]]}} -> {:ok, String.to_atom(single_key)}
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [key] -> String.to_atom(key) end)}
      {:error, reason} -> {:error, {:primary_key_query_failed, reason}}
    end
  end

  defp get_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      constraint_name,
      column_name,
      referenced_table_schema,
      referenced_table_name,
      referenced_column_name
    FROM information_schema.key_column_usage
    WHERE table_schema = ?
      AND table_name = ?
      AND referenced_table_name IS NOT NULL
    ORDER BY constraint_name, ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             constraint_name,
                             column_name,
                             foreign_schema,
                             foreign_table,
                             foreign_col
                           ] ->
           %{
             constraint_name: constraint_name,
             column_name: String.to_atom(column_name),
             foreign_table_schema: foreign_schema,
             foreign_table_name: foreign_table,
             foreign_column_name: String.to_atom(foreign_col)
           }
         end)}

      {:error, reason} ->
        {:error, {:foreign_keys_query_failed, reason}}
    end
  end

  defp get_reverse_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      table_name AS referencing_table,
      column_name AS referencing_column,
      referenced_column_name AS referenced_column,
      constraint_name
    FROM information_schema.key_column_usage
    WHERE referenced_table_schema = ?
      AND referenced_table_name = ?
    ORDER BY table_name, constraint_name, ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             referencing_table,
                             referencing_column,
                             referenced_column,
                             constraint_name
                           ] ->
           %{
             referencing_table: referencing_table,
             referencing_column: String.to_atom(referencing_column),
             referenced_column: String.to_atom(referenced_column),
             constraint_name: constraint_name
           }
         end)}

      {:error, reason} ->
        {:error, {:reverse_foreign_keys_query_failed, reason}}
    end
  end

  defp build_associations(foreign_keys) do
    Enum.into(foreign_keys, %{}, fn foreign_key ->
      association_name =
        foreign_key.column_name
        |> Atom.to_string()
        |> String.replace_suffix("_id", "")
        |> String.to_atom()

      related_module_name = table_name_to_module(foreign_key.foreign_table_name)

      {association_name,
       %{
         type: :belongs_to,
         association_type: :belongs_to,
         related_schema: related_module_name,
         related_module_name: related_module_name,
         related_table: foreign_key.foreign_table_name,
         queryable: String.to_atom(foreign_key.foreign_table_name),
         field: association_name,
         owner_key: foreign_key.column_name,
         related_key: foreign_key.foreign_column_name,
         join_type: :inner,
         is_through: false,
         constraint_name: foreign_key.constraint_name
       }}
    end)
  end

  defp build_expanded_associations(connection, table_name, schema, primary_key) do
    with {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema),
         {:ok, reverse_foreign_keys} <- get_reverse_foreign_keys(connection, table_name, schema),
         {:ok, junction_tables} <- detect_junction_tables(connection, schema) do
      belongs_to = build_associations(foreign_keys)
      primary_key_field = normalize_primary_key(primary_key)

      has_many =
        Enum.into(reverse_foreign_keys, %{}, fn reverse_foreign_key ->
          association_name = String.to_atom(reverse_foreign_key.referencing_table)
          related_module_name = table_name_to_module(reverse_foreign_key.referencing_table)

          {association_name,
           %{
             type: :has_many,
             association_type: :has_many,
             related_schema: related_module_name,
             related_module_name: related_module_name,
             related_table: reverse_foreign_key.referencing_table,
             queryable: String.to_atom(reverse_foreign_key.referencing_table),
             field: association_name,
             owner_key: primary_key_field,
             related_key: reverse_foreign_key.referencing_column,
             join_type: :left,
             is_through: false,
             constraint_name: reverse_foreign_key.constraint_name
           }}
        end)

      many_to_many =
        junction_tables
        |> Enum.filter(fn junction -> table_name in junction.tables end)
        |> Enum.flat_map(fn junction ->
          {this_foreign_keys, other_foreign_keys} =
            Enum.split_with(junction.foreign_keys, fn foreign_key ->
              foreign_key.foreign_table_name == table_name
            end)

          Enum.map(other_foreign_keys, fn other_foreign_key ->
            association_name = String.to_atom(other_foreign_key.foreign_table_name)
            related_module_name = table_name_to_module(other_foreign_key.foreign_table_name)

            owner_foreign_key =
              case this_foreign_keys do
                [foreign_key | _] -> foreign_key.column_name
                _ -> primary_key_field
              end

            {association_name,
             %{
               type: :many_to_many,
               association_type: :many_to_many,
               related_schema: related_module_name,
               related_module_name: related_module_name,
               related_table: other_foreign_key.foreign_table_name,
               queryable: String.to_atom(other_foreign_key.foreign_table_name),
               field: association_name,
               owner_key: primary_key_field,
               related_key: other_foreign_key.foreign_column_name,
               join_type: :left,
               is_through: false,
               join_through: junction.table,
               join_keys: [
                 {owner_foreign_key, primary_key_field},
                 {other_foreign_key.column_name, other_foreign_key.foreign_column_name}
               ]
             }}
          end)
        end)
        |> Enum.into(%{})

      {:ok, belongs_to |> Map.merge(has_many) |> Map.merge(many_to_many)}
    end
  end

  defp detect_junction_tables(connection, schema) do
    with {:ok, tables} <- list_tables(connection, schema: schema) do
      junction_tables =
        Enum.flat_map(tables, fn table ->
          case analyze_junction_table(connection, table, schema) do
            {:ok, junction_table} -> [junction_table]
            _ -> []
          end
        end)

      {:ok, junction_tables}
    end
  end

  defp analyze_junction_table(connection, table, schema) do
    with {:ok, columns} <- get_columns(connection, table, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table, schema),
         {:ok, primary_key} <- get_primary_key(connection, table, schema),
         true <- junction_table?(columns, foreign_keys) do
      primary_key_fields = normalize_primary_keys(primary_key)
      foreign_key_fields = Enum.map(foreign_keys, & &1.column_name)
      all_fields = Enum.map(columns, & &1.column_name)

      {:ok,
       %{
         table: table,
         foreign_keys: foreign_keys,
         primary_key: primary_key,
         extra_columns: all_fields -- Enum.uniq(primary_key_fields ++ foreign_key_fields),
         tables: Enum.map(foreign_keys, & &1.foreign_table_name)
       }}
    else
      false -> {:error, :not_junction_table}
      {:error, reason} -> {:error, reason}
    end
  end

  defp junction_table?(columns, foreign_keys) do
    foreign_key_fields = MapSet.new(Enum.map(foreign_keys, & &1.column_name))

    data_fields =
      columns
      |> Enum.map(& &1.column_name)
      |> Enum.reject(fn field ->
        field_name = Atom.to_string(field)

        field_name in ["id", "inserted_at", "updated_at", "created_at"] or
          String.ends_with?(field_name, "_at")
      end)

    length(foreign_keys) == 2 and Enum.all?(data_fields, &MapSet.member?(foreign_key_fields, &1))
  end

  defp normalize_primary_key([primary_key | _]), do: primary_key
  defp normalize_primary_key(primary_key) when is_atom(primary_key), do: primary_key
  defp normalize_primary_key(_), do: :id

  defp normalize_primary_keys(primary_key) when is_list(primary_key), do: primary_key
  defp normalize_primary_keys(primary_key) when is_atom(primary_key), do: [primary_key]
  defp normalize_primary_keys(_), do: []

  defp map_mysql_type(data_type, column_type) do
    normalized_data_type = String.downcase(to_string(data_type || ""))
    normalized_column_type = String.downcase(to_string(column_type || ""))

    cond do
      normalized_data_type in ["tinyint"] and
          String.starts_with?(normalized_column_type, "tinyint(1)") ->
        :boolean

      normalized_data_type in ["tinyint", "smallint", "mediumint", "int", "bigint"] ->
        :integer

      normalized_data_type in ["decimal", "numeric"] ->
        :decimal

      normalized_data_type in ["float", "double", "real"] ->
        :float

      normalized_data_type in [
        "char",
        "varchar",
        "text",
        "tinytext",
        "mediumtext",
        "longtext",
        "enum",
        "set",
        "json"
      ] ->
        :string

      normalized_data_type in ["date"] ->
        :date

      normalized_data_type in ["time"] ->
        :time

      normalized_data_type in ["datetime", "timestamp"] ->
        :naive_datetime

      normalized_data_type in [
        "binary",
        "varbinary",
        "blob",
        "tinyblob",
        "mediumblob",
        "longblob"
      ] ->
        :binary

      normalized_data_type in ["uuid"] ->
        :binary_id

      true ->
        :string
    end
  end

  defp table_name_to_module(table_name) when is_binary(table_name) do
    table_name
    |> singularize()
    |> Macro.camelize()
  end

  defp singularize(word) do
    cond do
      String.ends_with?(word, "ies") ->
        String.replace_suffix(word, "ies", "y")

      String.ends_with?(word, "sses") ->
        String.replace_suffix(word, "sses", "ss")

      String.ends_with?(word, "ses") ->
        String.replace_suffix(word, "ses", "s")

      String.ends_with?(word, "s") and not String.ends_with?(word, "ss") ->
        String.replace_suffix(word, "s", "")

      true ->
        word
    end
  end

  defp normalize_result(%{rows: rows} = result) do
    columns =
      result
      |> Map.get(:columns, [])
      |> Enum.map(&normalize_column_name/1)

    %{
      rows: rows || [],
      columns: columns
    }
  end

  defp normalize_column_name(%{name: name}) when is_binary(name), do: name
  defp normalize_column_name(name) when is_binary(name), do: name
  defp normalize_column_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_column_name(other), do: to_string(other)
end
