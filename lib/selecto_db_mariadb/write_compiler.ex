defmodule SelectoDBMariaDB.WriteCompiler do
  @moduledoc false

  alias Selecto.Write.{Batch, Command, Error, Preview}

  @spec preview(Command.t() | Batch.t(), keyword()) ::
          {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%Batch{commands: commands}, opts) do
    with {:ok, statements} <- map_commands(commands, &compile_tagged(&1, opts)) do
      statements = Enum.map(statements, &preview_statement/1)

      {:ok, %Preview{statements: statements, metadata: %{dialect: :mariadb, atomic?: true}}}
    end
  end

  def preview(%Command{} = command, opts) do
    with {:ok, statement} <- compile_tagged(command, opts) do
      {:ok, %Preview{statements: [preview_statement(statement)], metadata: %{dialect: :mariadb}}}
    end
  end

  @spec compile(Command.t(), keyword()) ::
          {:ok, %{text: String.t(), params: [term()]}} | {:error, Error.t()}
  def compile(%Command{} = command, opts) do
    with {:ok, statement} <- compile_tagged(command, opts) do
      {:ok, %{statement | params: bind_params(statement.params)}}
    end
  end

  defp compile_tagged(%Command{operation: :insert} = command, opts),
    do: compile_insert(command, opts)

  defp compile_tagged(%Command{operation: :upsert} = command, opts),
    do: compile_upsert(command, opts)

  defp compile_tagged(%Command{operation: :update} = command, opts),
    do: compile_update(command, opts)

  defp compile_tagged(%Command{operation: :delete} = command, opts),
    do: compile_delete(command, opts)

  # An exact tenant compare binds its value twice (once for the indexable
  # collation compare, once for the byte compare). The second bind is tagged
  # while compiling: the statement executes every bind, and a preview reports
  # the logical parameters, each value once, with the executable binds in
  # `bind_params`.
  @exact_bind :"$selecto_exact_bind"

  defp exact_bind(value), do: {@exact_bind, value}
  defp exact_binds(params), do: Enum.map(params, &exact_bind/1)

  @doc false
  # The values bound to the statement's placeholders, in order.
  def bind_params(params), do: Enum.map(params, &untag/1)

  defp untag({@exact_bind, value}), do: value
  defp untag(value), do: value

  defp logical_params(params), do: Enum.reject(params, &match?({@exact_bind, _}, &1))

  defp preview_statement(statement) do
    binds = bind_params(statement.params)
    logical = logical_params(statement.params)

    if binds == logical,
      do: %{statement | params: binds},
      else: statement |> Map.put(:params, logical) |> Map.put(:bind_params, binds)
  end

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

    with :ok <- refuse_scoped_upsert(command, opts),
         {:ok, assignments} <- compile_assignments(command.assignments, opts),
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
    opts = with_field_types(opts, command.metadata)

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
        texts = Enum.reverse(texts)
        column = quote_field(field, opts)

        if exact_field?(field, opts) do
          casts = Enum.map_join(texts, ", ", &utf8mb4_bytes/1)

          {:ok,
           %{
             text:
               "(#{column} IN (#{Enum.join(texts, ", ")}) AND " <>
                 "#{utf8mb4_bytes(column)} IN (#{casts}))",
             params: params ++ exact_binds(params)
           }}
        else
          {:ok, %{text: "#{column} IN (#{Enum.join(texts, ", ")})", params: params}}
        end

      error ->
        error
    end
  end

  def compile_predicate({operator, {:field, field}, value}, opts, offset)
      when operator in [:eq, :neq, :gt, :gte, :lt, :lte] do
    with {:ok, compiled} <- compile_value(value, opts, offset) do
      operator_text = %{eq: "=", neq: "!=", gt: ">", gte: ">=", lt: "<", lte: "<="}[operator]
      column = quote_field(field, opts)

      exact? =
        operator in [:eq, :neq] and
          (exact_field?(field, opts) or exact_value_field?(value, opts))

      if exact? do
        exact =
          "(#{column} = #{compiled.text} AND " <>
            "#{utf8mb4_bytes(column)} = #{utf8mb4_bytes(compiled.text)})"

        {:ok,
         %{
           text: if(operator == :eq, do: exact, else: "NOT " <> exact),
           params: compiled.params ++ exact_binds(compiled.params)
         }}
      else
        {:ok, %{text: "#{column} #{operator_text} #{compiled.text}", params: compiled.params}}
      end
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

  # A guard proves the referenced row exists. A guard that names
  # `tenant_field` must also carry `tenant_value`; the referenced row must then
  # belong to that tenant. Its columns are qualified by a subquery alias so a
  # column missing from the referenced relation fails instead of resolving to
  # the outer write target.
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
           true <- valid_ref?(relation) and valid_ref?(target_field),
           {:ok, tenant} <- foreign_key_guard_tenant(guard) do
        {text, guard_params} = foreign_key_guard_text(relation, target_field, value, tenant)

        {:cont, {:ok, [text | texts], params ++ guard_params, offset + length(guard_params)}}
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

  defp foreign_key_guard_tenant(guard) do
    case {Map.fetch(guard, :tenant_field), Map.get(guard, :tenant_value)} do
      {:error, _value} ->
        {:ok, nil}

      {{:ok, tenant_field}, tenant_value} when not is_nil(tenant_value) ->
        if valid_ref?(tenant_field),
          do: {:ok, {tenant_field, tenant_value, Map.get(guard, :tenant_exact, true) != false}},
          else: :error

      _invalid ->
        :error
    end
  end

  defp foreign_key_guard_text(relation, target_field, value, nil) do
    {"EXISTS (SELECT 1 FROM #{quote_relation(relation)} WHERE " <>
       "#{quote_identifier(target_field)} = ?)", [value]}
  end

  # The parent's tenant column compares by its stored bytes unless the domain
  # types it as a non-text value (`tenant_exact: false`), so a tenant 't1'
  # guard never admits a parent of 'T1' or 't1 '.
  defp foreign_key_guard_text(relation, target_field, value, {tenant_field, tenant, exact?}) do
    alias_name = quote_identifier("selecto_fk_parent")
    tenant_column = "#{alias_name}.#{quote_identifier(tenant_field)}"

    {tenant_text, tenant_params} =
      if exact?,
        do:
          {"(#{tenant_column} = ? AND #{utf8mb4_bytes(tenant_column)} = CAST(? AS BINARY))",
           [tenant, exact_bind(tenant)]},
        else: {"#{tenant_column} = ?", [tenant]}

    {"EXISTS (SELECT 1 FROM #{quote_relation(relation)} AS #{alias_name} " <>
       "WHERE #{alias_name}.#{quote_identifier(target_field)} = ? " <>
       "AND #{tenant_text})", [value | tenant_params]}
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

  # ON DUPLICATE KEY UPDATE fires on the primary key and on every unique index,
  # not only on the declared conflict target, so it can update a row that lies
  # outside a tenant or other write scope. Like the Go MySQL-family adapters,
  # refuse an upsert whenever any write scope applies.
  defp refuse_scoped_upsert(command, opts) do
    case upsert_scope(command, Keyword.get(opts, :context, %{})) do
      nil ->
        :ok

      scope ->
        {:error,
         Error.new(
           :unsupported_scope_predicate,
           "MySQL-family adapters cannot apply write scope predicates to upserts",
           details: %{scope: scope, upsert_strategy: :on_duplicate_key}
         )}
    end
  end

  defp upsert_scope(%Command{predicate: predicate}, _context) when not is_nil(predicate),
    do: :predicate

  defp upsert_scope(%Command{metadata: metadata}, context) do
    cond do
      Map.has_key?(metadata, :query_enforcement) -> :query_enforcement
      trusted_tenant?(context) -> :tenant
      true -> nil
    end
  end

  defp trusted_tenant?(context) when is_map(context) do
    Enum.any?([:tenant_id, "tenant_id", :tenant, "tenant"], &(Map.get(context, &1) != nil)) or
      List.wrap(Map.get(context, :required_filters) || Map.get(context, "required_filters")) !=
        []
  end

  defp trusted_tenant?(_context), do: false

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

  # Tenant and site columns (`metadata.tenant_fields`) compare by their stored
  # bytes, never under the column's collation: under MariaDB's default
  # case-insensitive collations (and PAD SPACE ones) tenant 't1' would
  # otherwise update or delete rows of 'T1' and 't1 '. A column the domain
  # types as a non-text value (integer, decimal, UUID) keeps the plain form.
  defp with_field_types(opts, metadata) when is_map(metadata) do
    types = Map.get(metadata, :field_types, %{})

    exact =
      metadata
      |> Map.get(:tenant_fields, [])
      |> List.wrap()
      |> Enum.map(&to_string/1)
      |> Enum.filter(&Selecto.Builder.ExactCompare.text_type?(field_type(types, &1)))

    Keyword.put(opts, :exact_fields, exact)
  end

  defp with_field_types(opts, _metadata), do: opts

  @doc false
  # The predicate options a compiled command uses, for re-reads of its row.
  def predicate_opts(%Command{metadata: metadata}, opts), do: with_field_types(opts, metadata)

  defp field_type(types, field) when is_map(types) do
    Enum.find_value(types, fn {key, type} -> if to_string(key) == field, do: type end)
  end

  defp field_type(_types, _field), do: nil

  defp exact_field?(field, opts),
    do: to_string(field_name(field)) in Keyword.get(opts, :exact_fields, [])

  # The utf8mb4 bytes of a bound value (`?`, already utf8mb4) or of a column,
  # converted first: CAST(col AS BINARY) alone would compare a utf16, ucs2,
  # utf32 or latin1 column's own charset bytes and never match.
  defp utf8mb4_bytes("?"), do: "CAST(? AS BINARY)"
  defp utf8mb4_bytes(column), do: "CAST(CONVERT(#{column} USING utf8mb4) AS BINARY)"

  defp exact_value_field?({:field, field}, opts), do: exact_field?(field, opts)
  defp exact_value_field?(_value, _opts), do: false

  defp field_name({:field, field}), do: field
  defp field_name(field), do: field
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
