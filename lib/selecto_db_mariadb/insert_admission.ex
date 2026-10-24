defmodule SelectoDBMariaDB.InsertAdmission do
  @moduledoc false

  # Storage-side admission of a guarded insert (a captured query predicate or
  # tenant scope). The portable candidate check in SelectoUpdato runs first;
  # this then decides on what MariaDB actually stored, inside the write's own
  # transaction:
  #
  # - before the INSERT, a session without strict sql_mode or a table that is
  #   not InnoDB (a non-transactional engine keeps a violating row after the
  #   rollback; a view has no engine) is refused (`query_rule_not_evaluable`);
  #   a row trigger that rewrites the row is seen by the re-read below;
  # - after the INSERT, the row is re-read by its primary key together with
  #   the predicate, so column types, DECIMAL rounding, collations and
  #   defaults decide. No matching row is `query_rule_violation`; the caller's
  #   transaction then rolls the INSERT back.
  #
  # The key is the declared single-column primary key: its assigned value, or
  # LAST_INSERT_ID() only when that column is the table's AUTO_INCREMENT
  # column and the statement generated a value. The key is compared in the
  # column's own type (a string key is never compared to a number, which
  # MariaDB would coerce: '101-existing' = 101), and exactly one row must carry
  # it before the predicate is applied. Anything else is not evaluable.
  #
  # information_schema shows only objects the session user has a privilege
  # on; a least-privilege user without one on the table sees no trigger. Give
  # the writing user its normal table privileges (TRIGGER is not required to
  # see triggers on tables it can write).

  alias Selecto.Write.{Command, Error}
  alias SelectoDBMariaDB.WriteCompiler

  @identifier ~r/\A[A-Za-z_][A-Za-z0-9_$]{0,63}\z/

  @doc false
  # Every governed write takes the table's metadata lock (refusing a
  # TEMPORARY shadow) and needs InnoDB: MEMORY, MyISAM and Aria tables keep a
  # change the transaction cannot roll back (w3exrev17 item 26a, w3exrev18
  # items 27 and 32).
  def require_transactional(connection, %Command{} = command, execute, opts) do
    with :ok <- lock_metadata(connection, command, execute, opts) do
      if engine_checked_here?(command),
        do: transactional_engine(connection, command, execute, opts),
        else: :ok
    else
      other -> retryable_or_denied(other)
    end
  end

  defp transactional_engine(connection, %Command{} = command, execute, opts) do
    with {:ok, schema, table} <- relation(command.relation),
         {:ok, %{rows: rows}} <-
           execute.(
             connection,
             "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ?",
             schema_params(schema) ++ [table],
             opts
           ) do
      case rows do
        [[engine]] when is_binary(engine) ->
          if String.downcase(engine) == "innodb", do: :ok, else: non_transactional(engine)

        _ ->
          non_transactional(nil)
      end
    else
      other -> retryable_or_denied(other)
    end
  end

  # Every governed write needs InnoDB (w3exrev18 item 32): a non-
  # transactional table keeps a partial change when a row trigger SIGNALs
  # or a count, guard or later batch command refuses. Guarded inserts,
  # updates and upserts check the engine themselves (before_insert/4,
  # before_update/4, before_upsert/4) and refuse as not evaluable.
  defp engine_checked_here?(%Command{} = command),
    do:
      not Enum.any?([:post_image, :insert_admission], &Map.has_key?(command.metadata || %{}, &1))

  defp non_transactional(engine),
    do:
      {:error,
       Error.new(
         :transaction_unsupported,
         "MariaDB cannot roll back this write: the table is not InnoDB",
         details: %{code: :non_transactional_engine, engine: engine, adapter: :mariadb}
       )}

  def before_insert(
        connection,
        %Command{operation: :insert, metadata: %{insert_admission: admission}} = command,
        execute,
        opts
      ) do
    with %{predicate: _predicate, primary_key: key} <- admission,
         true <- is_binary(key) and Regex.match?(@identifier, key),
         {:ok, schema, table} <- relation(command.relation),
         :ok <- lock_metadata(connection, command, execute, opts),
         # Without strict mode MariaDB stores a converted value with only a
         # warning (`'0x10'` becomes 0), which the re-read would then admit.
         {:ok, %{rows: [[sql_mode]]}} <-
           execute.(connection, "SELECT @@SESSION.sql_mode", [], opts),
         true <- strict?(sql_mode),
         {:ok, %{rows: [[engine]]}} <-
           execute.(
             connection,
             "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ?",
             schema_params(schema) ++ [table],
             opts
           ),
         true <- is_binary(engine) and String.downcase(engine) == "innodb",
         # A BEFORE INSERT trigger can rewrite NEW, the key included, so the
         # re-read by the assigned key could find an older in-scope row while
         # the rewritten one commits: such a table is refused. AFTER INSERT
         # triggers cannot modify the row (or the table they fire on).
         {:ok, %{rows: [[0]]}} <-
           execute.(
             connection,
             "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = " <>
               schema_sql(schema) <>
               " AND EVENT_OBJECT_TABLE = ? AND EVENT_MANIPULATION = 'INSERT' AND ACTION_TIMING = 'BEFORE'",
             schema_params(schema) ++ [table],
             opts
           ) do
      :ok
    else
      other -> retryable_or_denied(other)
    end
  end

  def before_insert(_connection, _command, _execute, _opts), do: :ok

  @doc false
  # A scoped upsert (`metadata.post_image`) runs the emulation's INSERT or
  # UPDATE branch; a row trigger on either could move the stored row out of
  # the scope (or rewrite the key the branch was chosen by), so a table with
  # any INSERT or UPDATE trigger, or one that is not InnoDB, is refused.
  def before_upsert(
        connection,
        %Command{operation: :upsert, metadata: %{post_image: _}} = command,
        execute,
        opts
      ) do
    with {:ok, schema, table} <- relation(command.relation),
         :ok <- lock_metadata(connection, command, execute, opts),
         {:ok, %{rows: [[engine]]}} <-
           execute.(
             connection,
             "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ?",
             schema_params(schema) ++ [table],
             opts
           ),
         true <- is_binary(engine) and String.downcase(engine) == "innodb",
         {:ok, %{rows: [[0]]}} <-
           execute.(
             connection,
             "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = " <>
               schema_sql(schema) <>
               " AND EVENT_OBJECT_TABLE = ? AND EVENT_MANIPULATION IN ('INSERT', 'UPDATE')",
             schema_params(schema) ++ [table],
             opts
           ) do
      :ok
    else
      other -> retryable_or_denied(other)
    end
  end

  # Every upsert's emulation reads the engine and unique indexes; hold the
  # metadata lock before that too.
  def before_upsert(connection, %Command{operation: :upsert} = command, execute, opts) do
    case lock_metadata(connection, command, execute, opts) do
      :ok -> :ok
      other -> retryable_or_denied(other)
    end
  end

  def before_upsert(_connection, _command, _execute, _opts), do: :ok

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
         true <- is_binary(key) and Regex.match?(@identifier, key),
         {:ok, schema, table} <- relation(command.relation),
         {:ok, column} <- key_column(connection, schema, table, key, execute, opts),
         {:ok, key_value} <- key_value(command, key, column, insert_result),
         # Tenant and site columns of the guard compare by their stored bytes
         # (WriteCompiler.predicate_opts/2).
         {:ok, guard} <-
           WriteCompiler.compile_predicate(
             predicate,
             WriteCompiler.predicate_opts(
               scope_exact(command, predicate),
               context: Keyword.get(opts, :context, %{})
             )
           ),
         from = "SELECT COUNT(*) FROM " <> quote_relation(command.relation),
         by_key = " WHERE " <> quote_id(key) <> " = ?",
         {:ok, %{rows: [[1]]}} <- execute.(connection, from <> by_key, [key_value], opts),
         {:ok, %{rows: [[count]]}} <-
           execute.(
             connection,
             from <> by_key <> " AND (" <> guard.text <> ")",
             [key_value | WriteCompiler.bind_params(guard.params)],
             opts
           ) do
      cond do
        count == 1 ->
          :ok

        # A row trigger rewrote the candidate: what the host sent cannot be
        # decided against the guard (typed_insert_admission TA005).
        insert_triggers?(connection, schema, table, execute, opts) ->
          denied(:query_rule_not_evaluable)

        true ->
          denied(:query_rule_violation)
      end
    else
      {:error, %Error{type: :query_enforcement}} = error -> error
      other -> retryable_or_denied(other)
    end
  end

  def after_insert(_connection, _command, _result, _execute, _opts), do: :ok

  defp insert_triggers?(connection, schema, table, execute, opts) do
    case execute.(
           connection,
           "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = " <>
             schema_sql(schema) <> " AND EVENT_OBJECT_TABLE = ? AND EVENT_MANIPULATION = 'INSERT'",
           schema_params(schema) ++ [table],
           opts
         ) do
      {:ok, %{rows: [[count]]}} when is_integer(count) -> count > 0
      _ -> true
    end
  end

  # TW-09 post-image of a guarded UPDATE (`metadata.post_image`: the engine
  # tenant and the trusted and host scope). Before the UPDATE the rows its
  # predicate selects are locked and their keys read (FOR UPDATE, host-exact
  # predicate); after it they are re-read by those keys under the scope, so
  # an update that assigns a scoped column, a BEFORE UPDATE trigger and a
  # generated column all decide on what MariaDB stored. Every locked row must
  # still match, else `query_rule_violation` and the transaction rolls back.
  # The key must be the table's whole single-column primary key (an enforced
  # NOT NULL unique key: a decoy row cannot share it); the table must be
  # InnoDB (a view or a non-transactional engine is refused).
  def before_update(
        connection,
        %Command{operation: :update, metadata: %{post_image: %{key: [key]}}} = command,
        execute,
        opts
      ) do
    with true <- Regex.match?(@identifier, key),
         {:ok, schema, table} <- relation(command.relation),
         :ok <- lock_metadata(connection, command, execute, opts),
         {:ok, %{rows: [[engine]]}} <-
           execute.(
             connection,
             "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = " <>
               schema_sql(schema) <> " AND TABLE_NAME = ?",
             schema_params(schema) ++ [table],
             opts
           ),
         true <- is_binary(engine) and String.downcase(engine) == "innodb",
         {:ok, column} <- key_column(connection, schema, table, key, execute, opts),
         {:ok, predicate} <-
           WriteCompiler.compile_predicate(
             command.predicate,
             WriteCompiler.predicate_opts(command, context: Keyword.get(opts, :context, %{}))
           ),
         {:ok, %{rows: rows}} <-
           execute.(
             connection,
             "SELECT " <>
               quote_id(key) <>
               " FROM " <>
               quote_relation(command.relation) <>
               " WHERE " <> predicate.text <> " FOR UPDATE",
             WriteCompiler.bind_params(predicate.params),
             opts
           ),
         keys = Enum.map(rows, &hd/1),
         true <- Enum.all?(keys, &match?({:ok, _}, typed_key(&1, column.type))) do
      {:ok, keys}
    else
      other -> retryable_or_denied(other)
    end
  end

  def before_update(
        _connection,
        %Command{operation: :update, metadata: %{post_image: _}},
        _e,
        _o
      ),
      do: denied(:query_rule_not_evaluable)

  def before_update(_connection, _command, _execute, _opts), do: {:ok, nil}

  def after_update(_connection, _command, nil, _execute, _opts), do: :ok
  def after_update(_connection, _command, [], _execute, _opts), do: :ok

  def after_update(
        connection,
        %Command{metadata: %{post_image: %{predicate: predicate, key: [key]}}} = command,
        keys,
        execute,
        opts
      ) do
    with {:ok, scope} <-
           WriteCompiler.compile_predicate(
             predicate,
             WriteCompiler.predicate_opts(
               scope_exact(command, predicate),
               context: Keyword.get(opts, :context, %{})
             )
           ),
         markers = Enum.map_join(keys, ", ", fn _ -> "?" end),
         {:ok, %{rows: [[count]]}} <-
           execute.(
             connection,
             "SELECT COUNT(*) FROM " <>
               quote_relation(command.relation) <>
               " WHERE " <> quote_id(key) <> " IN (" <> markers <> ") AND (" <> scope.text <> ")",
             keys ++ WriteCompiler.bind_params(scope.params),
             opts
           ) do
      if count == length(keys), do: :ok, else: denied(:query_rule_violation)
    else
      {:error, %Error{type: :query_enforcement}} = error -> error
      other -> retryable_or_denied(other)
    end
  end

  # Every column the scope (or insert guard) compares is identity: it
  # compares by its stored bytes by declared type, whether or not the
  # command listed it among its tenant fields.
  defp scope_exact(%Command{metadata: metadata} = command, predicate) do
    fields =
      Enum.uniq(List.wrap(Map.get(metadata, :tenant_fields, [])) ++ predicate_fields(predicate))

    %{command | metadata: Map.put(metadata, :tenant_fields, fields)}
  end

  defp predicate_fields({:field, field}), do: [to_string(field)]

  defp predicate_fields(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.flat_map(&predicate_fields/1)

  defp predicate_fields(list) when is_list(list), do: Enum.flat_map(list, &predicate_fields/1)
  defp predicate_fields(_value), do: []

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

  # The declared key must be the table's whole primary key.
  defp key_column(connection, schema, table, key, execute, opts) do
    case execute.(
           connection,
           "SELECT COLUMN_NAME, DATA_TYPE, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = " <>
             schema_sql(schema) <> " AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'",
           schema_params(schema) ++ [table],
           opts
         ) do
      {:ok, %{rows: [[name, type, extra]]}} when is_binary(name) ->
        if String.downcase(name) == String.downcase(key),
          do:
            {:ok,
             %{
               type: type |> to_string() |> String.downcase(),
               auto_increment: extra |> to_string() |> String.downcase() =~ "auto_increment"
             }},
          else: :error

      _ ->
        :error
    end
  end

  @integer_types ~w(tinyint smallint mediumint int integer bigint)
  @string_types ~w(char varchar binary varbinary tinytext text mediumtext longtext)

  defp key_value(%Command{assignments: assignments}, key, column, insert_result) do
    case Enum.find(assignments, &(to_string(field_name(&1.field)) == key)) do
      %{value: {:literal, value}} when not is_nil(value) ->
        typed_key(value, column.type)

      nil ->
        # The OK packet reports a value generated by this statement only; it
        # names this row only when the key column is the AUTO_INCREMENT one.
        case get_in(insert_result, [:metadata, :last_insert_id]) do
          id when is_integer(id) and id > 0 and column.auto_increment ->
            typed_key(id, column.type)

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp typed_key(value, type) when type in @integer_types and is_integer(value), do: {:ok, value}

  defp typed_key(value, type) when type in @integer_types and is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _ -> :error
    end
  end

  defp typed_key(value, type) when type in @string_types and is_binary(value), do: {:ok, value}
  defp typed_key(_value, _type), do: :error

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

  # A shared metadata lock on the table, held by the write's transaction
  # until it ends, taken before any engine or trigger catalog read: a
  # concurrent CREATE/DROP TRIGGER (or ALTER) then waits for the commit
  # instead of slipping in between the check and the write (w3exrev16).
  #
  # A TEMPORARY table of the same name shadows the base table for this
  # session and is invisible to information_schema, so every engine, key and
  # trigger check would read the wrong table (w3exrev18 item 27): it is
  # refused, detected through SHOW CREATE TABLE after the lock.
  defp lock_metadata(connection, command, execute, opts) do
    relation = quote_relation(command.relation)

    with {:ok, _} <- execute.(connection, "SELECT 1 FROM " <> relation <> " LIMIT 0", [], opts),
         {:ok, %{rows: [[_name, create | _]]}} when is_binary(create) <-
           execute.(connection, "SHOW CREATE TABLE " <> relation, [], opts) do
      if Regex.match?(~r/\A\s*CREATE\s+TEMPORARY\s+TABLE/i, create),
        do: temporary_shadow(),
        else: :ok
    else
      {:error, _} = error -> error
      _ -> :error
    end
  end

  defp temporary_shadow,
    do:
      {:error,
       Error.new(
         :query_enforcement,
         "MariaDB write target is shadowed by a TEMPORARY table",
         details: %{
           code: :query_rule_not_evaluable,
           reason: :temporary_table_shadow,
           adapter: :mariadb
         }
       )}

  # A deadlock (1213) or lock wait timeout (1205) while taking a lock or
  # reading the catalog is the canonical retryable write failure, not a
  # property of the scope (w3exrev17 MEDIUM): it keeps its category so the
  # caller may retry. Anything else stays not evaluable.
  defp retryable_or_denied({:error, %Selecto.Error{details: %{category: category}}} = error)
       when category in [:deadlock_detected, :lock_timeout],
       do: error

  defp retryable_or_denied({:error, %Error{}} = error), do: error
  defp retryable_or_denied(_other), do: denied(:query_rule_not_evaluable)

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
       Error.new(:query_enforcement, "MariaDB write cannot satisfy the captured query guard",
         details: %{code: code, adapter: :mariadb}
       )}
end
