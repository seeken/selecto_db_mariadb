defmodule SelectoDBMariaDB.WriteCompiler do
  @moduledoc false

  alias Selecto.Write.{Batch, Command, Error, Preview}

  @spec preview(Command.t() | Batch.t(), keyword()) ::
          {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%Batch{commands: commands}, opts) do
    with {:ok, statements} <- map_commands(commands, &compile(&1, opts)) do
      {:ok, %Preview{statements: statements, metadata: %{dialect: :mariadb, atomic?: true}}}
    end
  end

  def preview(%Command{} = command, opts) do
    with {:ok, statement} <- compile(command, opts) do
      {:ok, %Preview{statements: [statement], metadata: %{dialect: :mariadb}}}
    end
  end

  @spec compile(Command.t(), keyword()) ::
          {:ok, %{text: String.t(), params: [term()]}} | {:error, Error.t()}
  def compile(%Command{operation: :insert} = command, opts), do: compile_insert(command, opts)
  def compile(%Command{operation: :upsert} = command, opts), do: compile_upsert(command, opts)
  def compile(%Command{operation: :update} = command, opts), do: compile_update(command, opts)
  def compile(%Command{operation: :delete} = command, opts), do: compile_delete(command, opts)

  defp compile_insert(command, opts) do
    opts = with_field_types(opts, command.metadata)

    with {:ok, assignments} <- compile_assignments(command.assignments, opts),
         :ok <- require_assignments(assignments, :insert),
         {:ok, returning} <- compile_returning(command.returning),
         {:ok, guards} <- compile_foreign_key_guards(command.metadata, assignments) do
      {columns, values, params} = assignment_parts(assignments)

      values_clause =
        if guards.text,
          do: "SELECT #{Enum.join(values, ", ")} WHERE #{guards.text}",
          else: "VALUES (#{Enum.join(values, ", ")})"

      {:ok,
       %{
         text:
           "INSERT INTO #{quote_relation(command.relation)} (#{Enum.join(columns, ", ")}) " <>
             values_clause <> returning,
         params: params ++ guards.params
       }}
    end
  end

  defp compile_upsert(command, opts) do
    opts = with_field_types(opts, command.metadata)

    with {:ok, assignments} <- compile_assignments(command.assignments, opts),
         :ok <- require_assignments(assignments, :upsert),
         :ok <- validate_conflict_target(command.metadata),
         {:ok, update_assignments} <-
           compile_upsert_update_fields(command.metadata, assignments),
         {:ok, returning} <- compile_returning(command.returning),
         {:ok, guards} <- compile_foreign_key_guards(command.metadata, assignments) do
      {columns, values, params} = assignment_parts(assignments)

      values_clause =
        if guards.text,
          do: "SELECT #{Enum.join(values, ", ")} WHERE #{guards.text}",
          else: "VALUES (#{Enum.join(values, ", ")})"

      {:ok,
       %{
         text:
           "INSERT INTO #{quote_relation(command.relation)} (#{Enum.join(columns, ", ")}) " <>
             values_clause <>
             " ON DUPLICATE KEY UPDATE #{compile_conflict_action(update_assignments, command.metadata)}" <>
             returning,
         params: params ++ guards.params
       }}
    end
  end

  defp compile_update(%Command{predicate: nil}, _opts),
    do: {:error, Error.new(:missing_predicate, "update requires a portable predicate")}

  defp compile_update(command, opts) do
    opts = with_field_types(opts, command.metadata)

    with {:ok, assignments} <- compile_assignments(command.assignments, opts),
         :ok <- require_assignments(assignments, :update),
         {:ok, predicate} <-
           compile_predicate(command.predicate, opts, assignment_parameter_count(assignments)),
         {:ok, guards} <- compile_foreign_key_guards(command.metadata, assignments),
         {:ok, returning} <- compile_returning(command.returning) do
      {columns, values, assignment_params} = assignment_parts(assignments)
      set = Enum.zip_with(columns, values, &"#{&1} = #{&2}") |> Enum.join(", ")

      guard_text =
        case guards.text do
          nil -> ""
          text -> " AND " <> renumber(text, length(predicate.params))
        end

      {:ok,
       %{
         text:
           "UPDATE #{quote_relation(command.relation)} SET #{set} WHERE #{predicate.text}" <>
             guard_text <> returning,
         params: assignment_params ++ predicate.params ++ guards.params
       }}
    end
  end

  defp compile_delete(%Command{predicate: nil}, _opts),
    do: {:error, Error.new(:missing_predicate, "delete requires a portable predicate")}

  defp compile_delete(command, opts) do
    with {:ok, predicate} <- compile_predicate(command.predicate, opts, 0),
         {:ok, returning} <- compile_returning(command.returning) do
      {:ok,
       %{
         text:
           "DELETE FROM #{quote_relation(command.relation)} WHERE #{predicate.text}" <> returning,
         params: predicate.params
       }}
    end
  end

  @doc false
  def compile_predicate(predicate, opts \\ [], offset \\ 0)

  def compile_predicate({:and, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " AND ", opts, offset)

  def compile_predicate({:or, predicates}, opts, offset) when is_list(predicates),
    do: compile_predicate_list(predicates, " OR ", opts, offset)

  def compile_predicate({:not, predicate}, opts, offset) do
    with {:ok, compiled} <- compile_predicate(predicate, opts, offset) do
      {:ok, %{text: "NOT (#{compiled.text})", params: compiled.params}}
    end
  end

  def compile_predicate({:in, {:field, field}, values}, opts, offset)
      when is_list(values) and values != [] do
    values
    |> Enum.reduce_while({:ok, [], [], offset}, fn value, {:ok, texts, params, next} ->
      case compile_value(value, opts, next) do
        {:ok, compiled} ->
          {:cont,
           {:ok, [compiled.text | texts], params ++ compiled.params,
            next + length(compiled.params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, texts, params, _next} ->
        {:ok,
         %{
           text: "#{quote_field(field, opts)} IN (#{texts |> Enum.reverse() |> Enum.join(", ")})",
           params: params
         }}

      error ->
        error
    end
  end

  def compile_predicate({operator, {:field, field}, value}, opts, offset)
      when operator in [:eq, :neq, :gt, :gte, :lt, :lte] do
    with {:ok, compiled} <- compile_value(value, opts, offset) do
      operator_text = %{eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<="}[operator]

      {:ok,
       %{
         text: "#{quote_field(field, opts)} #{operator_text} #{compiled.text}",
         params: compiled.params
       }}
    end
  end

  def compile_predicate({:is_null, {:field, field}}, opts, _offset),
    do: {:ok, %{text: "#{quote_field(field, opts)} IS NULL", params: []}}

  def compile_predicate({:not_null, {:field, field}}, opts, _offset),
    do: {:ok, %{text: "#{quote_field(field, opts)} IS NOT NULL", params: []}}

  def compile_predicate(predicate, _opts, _offset) do
    {:error,
     Error.new(:invalid_predicate, "unsupported portable MariaDB predicate",
       details: %{predicate: predicate}
     )}
  end

  @doc false
  def compile_value(value, opts \\ [], offset \\ 0)

  def compile_value({:literal, {:system, :now}}, _opts, _offset),
    do: {:ok, %{text: "CURRENT_TIMESTAMP", params: []}}

  def compile_value({:literal, value}, _opts, offset), do: parameter(value, offset)

  def compile_value({:context, key}, opts, offset) do
    case fetch_context(Keyword.get(opts, :context, %{}), key) do
      {:ok, value} ->
        parameter(value, offset)

      :error ->
        {:error,
         Error.new(:missing_context, "required write context value is missing",
           details: %{key: key}
         )}
    end
  end

  def compile_value({:field, field}, opts, _offset),
    do: {:ok, %{text: quote_field(field, opts), params: []}}

  def compile_value({kind, _}, _opts, _offset) when kind in [:unsafe_sql, :unsafe_fragment],
    do: {:error, Error.new(:invalid_command, "raw SQL is not allowed in portable writes")}

  def compile_value(value, _opts, offset), do: parameter(value, offset)

  defp compile_assignments(assignments, opts) do
    assignments
    |> Enum.reduce_while({:ok, [], 0}, fn %{field: field, value: value},
                                          {:ok, compiled, offset} ->
      case compile_value(value, opts, offset) do
        {:ok, value} ->
          assignment = %{
            field: field,
            column: quote_identifier(field),
            text: cast_assignment(value.text, field, opts),
            params: value.params
          }

          {:cont, {:ok, [assignment | compiled], offset + length(value.params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, assignments, _offset} -> {:ok, Enum.reverse(assignments)}
      error -> error
    end
  end

  defp compile_foreign_key_guards(metadata, assignments) do
    metadata
    |> Map.get(:foreign_key_guards, [])
    |> Enum.reduce_while({:ok, [], [], assignment_parameter_count(assignments)}, fn guard,
                                                                                    {:ok, texts,
                                                                                     params,
                                                                                     offset} ->
      with %{field: field, relation: relation, target_field: target_field} <- guard,
           %{params: [value]} <-
             Enum.find(assignments, &(to_string(&1.field) == to_string(field))),
           true <- valid_ref?(relation) and valid_ref?(target_field) do
        text =
          "EXISTS (SELECT 1 FROM #{quote_relation(relation)} WHERE " <>
            "#{quote_identifier(target_field)} = ?)"

        {:cont, {:ok, [text | texts], params ++ [value], offset + 1}}
      else
        _ ->
          {:halt,
           {:error,
            Error.new(
              :invalid_foreign_key_guard,
              "foreign-key guard must reference an assigned scalar value",
              details: %{guard: guard}
            )}}
      end
    end)
    |> case do
      {:ok, [], [], _offset} ->
        {:ok, %{text: nil, params: []}}

      {:ok, texts, params, _offset} ->
        {:ok, %{text: texts |> Enum.reverse() |> Enum.join(" AND "), params: params}}

      error ->
        error
    end
  end

  defp compile_predicate_list([], _separator, _opts, _offset),
    do: {:error, Error.new(:invalid_predicate, "boolean predicate groups must not be empty")}

  defp compile_predicate_list(predicates, separator, opts, offset) do
    predicates
    |> Enum.reduce_while({:ok, [], [], offset}, fn predicate, {:ok, texts, params, next} ->
      case compile_predicate(predicate, opts, next) do
        {:ok, compiled} ->
          {:cont,
           {:ok, [compiled.text | texts], params ++ compiled.params,
            next + length(compiled.params)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, texts, params, _next} ->
        {:ok,
         %{text: "(" <> (texts |> Enum.reverse() |> Enum.join(separator)) <> ")", params: params}}

      error ->
        error
    end
  end

  defp compile_returning(:none), do: {:ok, ""}

  defp compile_returning(value),
    do:
      {:error,
       Error.new(
         :write_capability_missing,
         "MariaDB adapter cannot preserve portable returning semantics",
         details: %{returning: value}
       )}

  defp validate_conflict_target(metadata) do
    target = Map.get(metadata, :conflict_target)
    declared = Map.get(metadata, :declared_conflict_targets)

    cond do
      not (is_list(target) and target != [] and Enum.all?(target, &valid_ref?/1)) ->
        {:error, Error.new(:invalid_command, "upsert requires a valid conflict target")}

      not single_matching_conflict_target?(target, declared) ->
        {:error,
         Error.new(
           :write_capability_missing,
           "MariaDB cannot target one unique constraint when several may conflict",
           details: %{
             required: :single_declared_conflict_target,
             conflict_target: target,
             declared_conflict_targets: declared
           }
         )}

      true ->
        :ok
    end
  end

  defp single_matching_conflict_target?(target, [declared]) when is_list(declared) do
    normalize_fields(target) == normalize_fields(declared)
  end

  defp single_matching_conflict_target?(_target, _declared), do: false

  defp normalize_fields(fields), do: Enum.map(fields, &to_string/1)

  defp compile_upsert_update_fields(metadata, assignments) do
    case Map.fetch(metadata, :upsert_update_fields) do
      {:ok, fields} when is_list(fields) ->
        normalized = Enum.map(fields, &to_string/1)
        assigned = MapSet.new(assignments, &to_string(&1.field))

        cond do
          not Enum.all?(fields, &valid_ref?/1) ->
            invalid_upsert_fields(fields, :invalid_field)

          length(normalized) != MapSet.size(MapSet.new(normalized)) ->
            invalid_upsert_fields(fields, :duplicate_field)

          Enum.any?(normalized, &(not MapSet.member?(assigned, &1))) ->
            invalid_upsert_fields(fields, :field_not_assigned)

          true ->
            requested = MapSet.new(normalized)
            {:ok, Enum.filter(assignments, &MapSet.member?(requested, to_string(&1.field)))}
        end

      _ ->
        {:error,
         Error.new(:invalid_command, "upsert requires a domain-governed update field list")}
    end
  end

  defp invalid_upsert_fields(fields, reason),
    do:
      {:error,
       Error.new(:invalid_command, "invalid domain-governed upsert update field list",
         details: %{fields: fields, reason: reason}
       )}

  defp compile_conflict_action([], metadata) do
    field = metadata |> Map.fetch!(:conflict_target) |> hd() |> quote_identifier()
    "#{field} = #{field}"
  end

  defp compile_conflict_action(assignments, _metadata),
    do: Enum.map_join(assignments, ", ", &"#{&1.column} = VALUES(#{&1.column})")

  defp require_assignments([], operation),
    do: {:error, Error.new(:invalid_command, "#{operation} requires at least one assignment")}

  defp require_assignments(_assignments, _operation), do: :ok

  defp assignment_parts(assignments),
    do:
      {Enum.map(assignments, & &1.column), Enum.map(assignments, & &1.text),
       Enum.flat_map(assignments, & &1.params)}

  defp assignment_parameter_count(assignments),
    do: assignments |> Enum.flat_map(& &1.params) |> length()

  defp with_field_types(opts, _metadata), do: opts
  defp cast_assignment(text, _field, _opts), do: text

  defp parameter(value, _offset), do: {:ok, %{text: "?", params: [value]}}

  defp renumber(text, _delta), do: text

  defp map_commands(commands, fun) do
    Enum.reduce_while(commands, {:ok, []}, fn command, {:ok, statements} ->
      case fun.(command) do
        {:ok, statement} -> {:cont, {:ok, statements ++ [statement]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp quote_relation(relation),
    do: relation |> to_string() |> String.split(".") |> Enum.map_join(".", &quote_identifier/1)

  defp quote_identifier(identifier),
    do: identifier |> to_string() |> String.replace("`", "``") |> then(&"`#{&1}`")

  defp quote_field(field, opts) do
    case Keyword.get(opts, :predicate_relation_alias) do
      nil -> quote_identifier(field)
      alias_name -> quote_identifier(alias_name) <> "." <> quote_identifier(field)
    end
  end

  defp valid_ref?(value) when is_atom(value), do: not is_nil(value)
  defp valid_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp valid_ref?(_value), do: false

  defp fetch_context(context, key) when is_map(context) do
    cond do
      Map.has_key?(context, key) ->
        {:ok, Map.fetch!(context, key)}

      is_atom(key) and Map.has_key?(context, Atom.to_string(key)) ->
        {:ok, Map.fetch!(context, Atom.to_string(key))}

      true ->
        case Enum.find(context, fn {context_key, _value} ->
               is_atom(context_key) and Atom.to_string(context_key) == to_string(key)
             end) do
          {_key, value} -> {:ok, value}
          nil -> :error
        end
    end
  end

  defp fetch_context(_context, _key), do: :error
end
