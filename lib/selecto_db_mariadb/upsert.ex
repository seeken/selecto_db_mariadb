defmodule SelectoDBMariaDB.Upsert do
  @moduledoc false

  # PostgreSQL's INSERT ... ON CONFLICT (target) DO UPDATE without ON DUPLICATE
  # KEY UPDATE, which fires on the primary key and every unique index: an
  # upsert whose assigned id belonged to another row would overwrite that row.
  #
  # The declared target must be exactly the columns of one whole-column
  # unique index of an InnoDB table (information_schema), as PostgreSQL
  # requires a unique index matching the conflict target. In the write's own
  # transaction:
  #
  # - a plain probe finds the target's row; present, it is locked (FOR
  #   UPDATE) and updated by the target, without an INSERT first, whose
  #   duplicate would leave a shared lock that racing sessions deadlock on;
  # - absent (or gone by the lock), the row is inserted inside a savepoint.
  #   On a duplicate the savepoint is rolled back and, as PostgreSQL's arbiter
  #   index does, the target alone decides: its row is locked and updated, and
  #   with no such row the collision was on another unique key and the typed
  #   unique violation stands. A NULL target value never conflicts.
  #
  # A savepoint rollback that fails (a deadlock victim's transaction is
  # already gone, 1305) never replaces the original error. Tenant and site
  # columns of the target compare by their stored bytes (WriteCompiler).

  alias Selecto.Write.{Command, Error}

  @identifier ~r/\A[A-Za-z_][A-Za-z0-9_$]{0,63}\z/

  @doc false
  # `{:ok, 1}` or `{:error, reason}`. `execute` runs one statement on `conn`
  # (the write's transaction) and `control` one savepoint statement.
  def execute(conn, %Command{} = command, plan, execute, control, opts) do
    with :ok <- verify_target(conn, command.relation, plan.target, execute, opts) do
      run(conn, command, plan, execute, control, opts)
    end
  end

  defp run(conn, command, plan, execute, control, opts) do
    table = quote_relation(command.relation)

    probe =
      if plan.null_target?,
        do: {:ok, %{rows: []}},
        else:
          execute.(
            conn,
            "SELECT 1 FROM #{table} WHERE #{plan.where.text} LIMIT 1",
            plan.where.params,
            opts
          )

    case probe do
      {:ok, %{rows: [_ | _]}} ->
        case lock_and_update(conn, table, plan, execute, opts) do
          :absent -> insert(conn, table, plan, execute, control, opts)
          result -> result
        end

      {:ok, _} ->
        insert(conn, table, plan, execute, control, opts)

      error ->
        error
    end
  end

  defp insert(conn, table, plan, execute, control, opts) do
    savepoint = "selecto_upsert_#{System.unique_integer([:positive])}"

    with :ok <- control.(conn, "SAVEPOINT #{savepoint}") do
      case execute.(conn, plan.insert.text, plan.insert.params, opts) do
        {:ok, %{num_rows: 1}} ->
          with :ok <- control.(conn, "RELEASE SAVEPOINT #{savepoint}"), do: {:ok, 1}

        {:ok, %{num_rows: count}} ->
          # A reference guard refused the row (INSERT ... SELECT ... WHERE).
          _ = control.(conn, "ROLLBACK TO SAVEPOINT #{savepoint}")
          {:error, cardinality(count)}

        {:error, %Selecto.Error{details: %{category: :unique_violation}}} = duplicate ->
          case control.(conn, "ROLLBACK TO SAVEPOINT #{savepoint}") do
            :ok when plan.null_target? ->
              duplicate

            :ok ->
              case lock_and_update(conn, table, plan, execute, opts) do
                :absent -> duplicate
                result -> result
              end

            _rollback_failed ->
              duplicate
          end

        {:error, _reason} = error ->
          # The original error stands whether or not the rollback succeeds.
          _ = control.(conn, "ROLLBACK TO SAVEPOINT #{savepoint}")
          error
      end
    end
  end

  defp lock_and_update(conn, table, plan, execute, opts) do
    case execute.(
           conn,
           "SELECT 1 FROM #{table} WHERE #{plan.where.text} FOR UPDATE",
           plan.where.params,
           opts
         ) do
      {:ok, %{rows: []}} ->
        :absent

      {:ok, _locked} when is_nil(plan.update) ->
        {:ok, 1}

      {:ok, _locked} ->
        case execute.(conn, plan.update.text, plan.update.params, opts) do
          {:ok, %{num_rows: 1}} -> {:ok, 1}
          {:ok, %{num_rows: count}} -> {:error, cardinality(count)}
          error -> error
        end

      error ->
        error
    end
  end

  # The target must be the whole column set of exactly one unique index (no
  # prefix index) of an InnoDB table.
  defp verify_target(conn, relation, target, execute, opts) do
    with {:ok, schema, table} <- relation_parts(relation),
         {:ok, %{rows: [[engine]]}} <-
           execute.(
             conn,
             "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ?",
             schema_params(schema) ++ [table],
             opts
           ),
         true <- is_binary(engine) and String.downcase(engine) == "innodb",
         {:ok, %{rows: rows}} <-
           execute.(
             conn,
             "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS " <>
               "WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ? AND NON_UNIQUE = 0",
             schema_params(schema) ++ [table],
             opts
           ) do
      wanted = target |> Enum.map(&String.downcase/1) |> MapSet.new()

      matches =
        rows
        |> Enum.group_by(fn [index | _] -> index end)
        |> Enum.count(fn {_index, columns} ->
          Enum.all?(columns, fn [_index, column, prefix] ->
            is_binary(column) and is_nil(prefix)
          end) and
            MapSet.new(columns, fn [_index, column, _prefix] -> String.downcase(column) end) ==
              wanted and length(columns) == MapSet.size(wanted)
        end)

      if matches >= 1, do: :ok, else: refuse(:no_matching_unique_index)
    else
      false -> refuse(:non_transactional_engine)
      {:ok, _} -> refuse(:unknown_table)
      {:error, _} = error -> error
      :error -> refuse(:invalid_relation)
    end
  end

  defp refuse(reason),
    do:
      {:error,
       Error.new(
         :write_capability_missing,
         "MySQL-family upserts need a conflict target that is exactly one unique index",
         details: %{code: :unsupported_upsert_conflict_target, reason: reason}
       )}

  defp cardinality(count),
    do:
      Error.new(:cardinality_mismatch, "write affected an unexpected number of rows",
        details: %{expected: {:exactly, 1}, actual: count}
      )

  defp relation_parts(relation) do
    parts = relation |> to_string() |> String.split(".")

    if Enum.all?(parts, &Regex.match?(@identifier, &1)) do
      case parts do
        [table] -> {:ok, nil, table}
        [schema, table] -> {:ok, schema, table}
        _ -> :error
      end
    else
      :error
    end
  end

  defp schema_sql(nil), do: "DATABASE()"
  defp schema_sql(_schema), do: "?"
  defp schema_params(nil), do: []
  defp schema_params(schema), do: [schema]

  defp quote_relation(relation) do
    relation
    |> to_string()
    |> String.split(".")
    |> Enum.map_join(".", &("`" <> String.replace(&1, "`", "``") <> "`"))
  end
end
