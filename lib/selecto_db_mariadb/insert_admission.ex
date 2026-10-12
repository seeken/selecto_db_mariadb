defmodule SelectoDBMariaDB.InsertAdmission do
  @moduledoc false

  # Storage-side admission of a guarded insert (a captured query predicate or
  # tenant scope). The portable candidate check in SelectoUpdato runs first;
  # this then decides on what MariaDB actually stored, inside the write's own
  # transaction:
  #
  # - before the INSERT, a session without strict sql_mode, or a table with an
  #   INSERT trigger, is refused
  #   (`query_rule_not_evaluable`): a trigger can rewrite the row or write
  #   elsewhere after any check;
  # - after the INSERT, the row is re-read by its primary key together with
  #   the predicate, so column types, DECIMAL rounding, collations and
  #   defaults decide. No matching row is `query_rule_violation`; the caller's
  #   transaction then rolls the INSERT back.
  #
  # The key is the assigned primary key, or LAST_INSERT_ID() for a generated
  # one. Anything else is not evaluable.

  alias Selecto.Write.{Command, Error}
  alias SelectoDBMariaDB.WriteCompiler

  @identifier ~r/\A[A-Za-z_][A-Za-z0-9_$]{0,63}\z/

  def before_insert(
        connection,
        %Command{operation: :insert, metadata: %{insert_admission: admission}} = command,
        execute,
        opts
      ) do
    with %{predicate: _predicate, primary_key: key} <- admission,
         true <- is_binary(key) and Regex.match?(@identifier, key),
         {:ok, schema, table} <- relation(command.relation),
         # Without strict mode MariaDB stores a converted value with only a
         # warning (`'0x10'` becomes 0), which the re-read would then admit.
         {:ok, %{rows: [[sql_mode]]}} <-
           execute.(connection, "SELECT @@SESSION.sql_mode", [], opts),
         true <- strict?(sql_mode),
         {:ok, %{rows: [[count]]}} <-
           execute.(
             connection,
             "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = " <>
               schema_sql(schema) <>
               " AND EVENT_OBJECT_TABLE = ? AND EVENT_MANIPULATION = 'INSERT'",
             schema_params(schema) ++ [table],
             opts
           ) do
      if count == 0, do: :ok, else: denied(:query_rule_not_evaluable)
    else
      _ -> denied(:query_rule_not_evaluable)
    end
  end

  def before_insert(_connection, _command, _execute, _opts), do: :ok

  defp strict?(sql_mode) when is_binary(sql_mode) do
    modes = String.split(sql_mode, ",")
    "STRICT_TRANS_TABLES" in modes or "STRICT_ALL_TABLES" in modes
  end

  defp strict?(_sql_mode), do: false

  def after_insert(
        connection,
        %Command{operation: :insert, metadata: %{insert_admission: admission}} = command,
        insert_result,
        execute,
        opts
      ) do
    with %{predicate: predicate, primary_key: key} <- admission,
         {:ok, key_value} <- key_value(command, key, insert_result),
         {:ok, guard} <-
           WriteCompiler.compile_predicate(predicate, context: Keyword.get(opts, :context, %{})),
         sql =
           "SELECT COUNT(*) FROM " <>
             quote_relation(command.relation) <>
             " WHERE " <> quote_id(key) <> " = ? AND (" <> guard.text <> ")",
         {:ok, %{rows: [[count]]}} <- execute.(connection, sql, [key_value | guard.params], opts) do
      if count == 1, do: :ok, else: denied(:query_rule_violation)
    else
      {:error, %Error{type: :query_enforcement}} = error -> error
      _ -> denied(:query_rule_not_evaluable)
    end
  end

  def after_insert(_connection, _command, _result, _execute, _opts), do: :ok

  # Strict-mode conversion failures (1264 out of range, 1265 truncated, 1292
  # and 1366 incorrect value) mean the candidate has no stored form the guard
  # could be evaluated on; the insert is refused as not evaluable.
  @conversion_errors [1264, 1265, 1292, 1366]

  def storage_error(
        %Command{operation: :insert, metadata: %{insert_admission: _}},
        {:error, %Selecto.Error{details: %{code: code}}}
      )
      when code in @conversion_errors,
      do: denied(:query_rule_not_evaluable)

  def storage_error(_command, result), do: result

  defp key_value(%Command{assignments: assignments}, key, insert_result) do
    case Enum.find(assignments, &(to_string(field_name(&1.field)) == key)) do
      %{value: {:literal, value}} when not is_nil(value) ->
        {:ok, value}

      nil ->
        case get_in(insert_result, [:metadata, :last_insert_id]) do
          id when is_integer(id) and id > 0 -> {:ok, id}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp field_name({:field, field}), do: field
  defp field_name(field), do: field

  defp relation(relation) do
    case relation |> to_string() |> String.split(".") do
      [table] -> identifiers([nil, table])
      [schema, table] -> identifiers([schema, table])
      _ -> :error
    end
  end

  defp identifiers([schema, table] = parts) do
    if Enum.all?(Enum.reject(parts, &is_nil/1), &Regex.match?(@identifier, &1)),
      do: {:ok, schema, table},
      else: :error
  end

  defp schema_sql(nil), do: "DATABASE()"
  defp schema_sql(_schema), do: "?"
  defp schema_params(nil), do: []
  defp schema_params(schema), do: [schema]

  defp quote_relation(relation) do
    relation
    |> to_string()
    |> String.split(".")
    |> Enum.map_join(".", &quote_id/1)
  end

  defp quote_id(identifier), do: SelectoDBMariaDB.Adapter.quote_identifier(identifier)

  defp denied(code),
    do:
      {:error,
       Error.new(:query_enforcement, "MariaDB insert cannot satisfy the captured query guard",
         details: %{code: code, adapter: :mariadb}
       )}
end
