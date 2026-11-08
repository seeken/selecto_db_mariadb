defmodule SelectoDBMariaDB.Adapter do
  @moduledoc """
  MariaDB adapter for Selecto backed by `MyXQL`.

  Writes reach this adapter through the governed entry point, `SelectoUpdato`.
  `execute_write/3` refuses a write without the `Selecto.Write.Authorization`
  issued for exactly that payload (`:ungoverned_write`).
  `execute_write_unsafe/3` skips that check and exists for trusted tooling and
  adapter tests only.
  """

  @behaviour Selecto.DB.Adapter
  @behaviour Selecto.DB.WriteAdapter

  alias Selecto.Write.{Batch, Command, Error, Graph, Result}
  alias SelectoDBMariaDB.{InsertAdmission, Session, StoredValues, Upsert, WriteCompiler}

  @missing_dependency {:adapter_dependency_missing, :myxql}

  @impl true
  def name, do: :mariadb

  @impl true
  def dialect, do: SelectoDBMariaDB.Dialect

  @impl true
  def capability(:text_search) do
    %{
      feature: :text_search,
      supported?: true,
      modes: [:natural, :websearch, :plain, :boolean, :query_expansion, :prefix],
      # MATCH reads only a FULLTEXT index on the searched table's own columns,
      # and MariaDB refuses a column list without one, so a governed lookup
      # never searches content outside its relation.
      governed_lookup?: true,
      default_mode: :natural,
      document_type: :text_search_document,
      help: "MariaDB full-text search with natural-language, boolean, and query-expansion modes."
    }
  end

  def capability(feature), do: %{feature: feature, supported?: supports?(feature)}

  @impl true
  def normalize_type(type) when is_binary(type) do
    case type |> String.trim() |> String.downcase() do
      value when value in ["tinyint", "smallint", "mediumint", "int", "integer", "bigint"] ->
        :integer

      value when value in ["float", "double"] ->
        :float

      value when value in ["decimal", "numeric"] ->
        :decimal

      value when value in ["char", "varchar", "text", "tinytext", "mediumtext", "longtext"] ->
        :string

      value when value in ["binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"] ->
        :binary

      value when value in ["bool", "boolean"] ->
        :boolean

      "date" ->
        :date

      "time" ->
        :time

      value when value in ["datetime", "timestamp"] ->
        :naive_datetime

      "json" ->
        :map

      _unknown ->
        type
    end
  end

  def normalize_type(type), do: Selecto.TypeSystem.normalize_type(type)

  @impl true
  def type_family(type), do: type |> normalize_type() |> Selecto.TypeFamily.of()

  @impl true
  def normalize_execution_result(%{rows: rows, columns: columns} = result) do
    {:ok, %{result | rows: rows || [], columns: Enum.map(columns || [], &to_string/1)}}
  end

  def normalize_execution_result(result), do: {:error, {:invalid_adapter_result, result}}

  @impl true
  def normalize_error(%Selecto.Error{} = error), do: error
  def normalize_error(%MyXQL.Error{} = error), do: sanitize_error(error)
  def normalize_error(%DBConnection.ConnectionError{} = error), do: sanitize_error(error)
  def normalize_error(reason), do: Selecto.Error.from_reason(reason)

  @impl true
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  # A connection the host already holds.
  def connect(%DBConnection{} = connection), do: {:ok, connection}
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
  def disconnect(connection) when is_pid(connection) do
    if Process.alive?(connection), do: GenServer.stop(connection)
    :ok
  end

  def disconnect(_connection), do: :ok

  @impl true
  def execute(connection, query, params, opts) do
    if is_map(connection) and Map.has_key?(connection, :adapter) and
         Map.has_key?(connection, :connection) do
      execute_direct(Map.get(connection, :connection), query, params, opts)
    else
      execute_direct(connection, query, params, opts)
    end
  end

  @impl true
  # A read-only server-side cursor; see SelectoDBMariaDB.RowStream for the
  # connection-ownership rules.
  def stream(connection, query, params, opts) do
    connection = resolve_connection(connection)

    # A Selecto read's stream (Selecto.Executor.selecto_read?/1, captured
    # here: the producer runs in its own process) refuses TEMPORARY shadows.
    opts =
      if Selecto.Executor.selecto_read?(opts),
        do: Keyword.put(opts, :selecto_session_guard, true),
        else: opts

    # The text as the host wrote it: statement/1 only prefixes what is sent.
    opts = Keyword.put(opts, :selecto_statement, {normalize_query(query), params})

    cond do
      not dependency_available?() ->
        {:error, @missing_dependency}

      is_pid(connection) and Process.alive?(connection) ->
        SelectoDBMariaDB.RowStream.open(connection, statement(query, opts), params, opts)

      is_atom(connection) and not is_nil(connection) and is_pid(Process.whereis(connection)) ->
        SelectoDBMariaDB.RowStream.open(connection, statement(query, opts), params, opts)

      true ->
        {:error, {:invalid_connection, connection}}
    end
  end

  @impl true
  # A transaction Selecto owns ends through Session.owned_transaction/3, never
  # through MyXQL.transaction/3: MyXQL sends a bare COMMIT or ROLLBACK, which
  # obeys the host's completion_type.
  def transaction(connection, fun, opts) when is_function(fun, 1) do
    if dependency_available?() do
      connection
      |> resolve_connection()
      |> run_transaction(fun, opts)
    else
      {:error, @missing_dependency}
    end
  rescue
    exception -> {:error, exception}
  catch
    :exit, reason -> {:error, reason}
  end

  defp run_transaction(%DBConnection{conn_mode: :transaction} = tx, fun, _opts) do
    if Session.owned?(tx), do: nested_transaction(tx, fun), else: host_transaction(tx, fun)
  end

  defp run_transaction(connection, fun, opts) do
    connection
    |> Session.checkout(&owned_transaction(&1, fun, opts), opts)
    |> transaction_result()
  end

  defp owned_transaction(checked_out, fun, opts) do
    case {DBConnection.status(checked_out), Session.host_locks(checked_out, [], opts)} do
      {:idle, :none} -> Session.owned_transaction(checked_out, fun, opts)
      {:idle, held} when held in [:held, :unsettled] -> {:error, host_table_locks()}
      {:idle, {:error, _} = error} -> error
      _not_idle -> {:error, unowned_transaction()}
    end
  end

  # Inside a transaction Selecto owns: a failure dooms the owner's ending.
  defp nested_transaction(tx, fun) do
    case fun.(tx) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        Session.fail_transaction(tx)
        {:error, reason}

      result ->
        {:ok, result}
    end
  end

  # Inside a host's DBConnection transaction: no statement is sent here.
  defp host_transaction(tx, fun) do
    MyXQL.transaction(tx, fn nested ->
      case fun.(nested) do
        {:ok, result} -> result
        {:error, reason} -> MyXQL.rollback(nested, reason)
        result -> result
      end
    end)
  end

  defp transaction_result(
         {:error, %DBConnection.ConnectionError{message: "transaction rolling back"}}
       ),
       do: {:error, :rollback}

  defp transaction_result({:error, %DBConnection.ConnectionError{} = error}),
    do: {:error, sanitize_error(error)}

  defp transaction_result({:error, %MyXQL.Error{} = error}), do: {:error, sanitize_error(error)}
  defp transaction_result(result), do: result

  # Each call first verifies the session (SelectoDBMariaDB.Session) on the
  # connection that then runs the statement.
  defp execute_direct(connection, query, params, opts) do
    if dependency_available?() do
      with :ok <- require_binary_protocol(opts) do
        route(
          connection,
          query,
          &execute_checked(&1, query, params, opts),
          Keyword.put(opts, :selecto_statement, {normalize_query(query), params})
        )
      end
    else
      {:error, @missing_dependency}
    end
  rescue
    error in DBConnection.ConnectionError -> {:error, sanitize_error(error)}
  end

  # A Selecto read follows Session.guarded_read/5; a catalog read Selecto issues
  # for itself ends what it opened; anything else runs as written.
  defp route(connection, query, fun, opts) do
    cond do
      Selecto.Executor.selecto_read?(opts) -> Session.read(connection, query, fun, opts)
      Keyword.get(opts, :selecto_catalog) == true -> Session.catalog(connection, fun, opts)
      true -> Session.run(connection, fun, opts)
    end
  end

  # A Selecto read (marked by Selecto's executor,
  # Selecto.Executor.selecto_read?/1) first refuses a relation a TEMPORARY
  # table shadows.
  defp execute_checked(connection, query, params, opts) do
    if Selecto.Executor.selecto_read?(opts) and not Session.host_text?(opts) do
      with :ok <- Session.refuse_temporary_shadows(connection, query, opts) do
        execute_unchecked(connection, query, params, opts)
      end
    else
      execute_unchecked(connection, query, params, opts)
    end
  end

  @doc false
  # Before a read compiles (Selecto.Executor.prepare_read/1): the storage
  # data types of every column of the domain's relations, so a scope
  # comparison on a TIMESTAMP column compiles zone-independently
  # (Selecto.Builder.ExactCompare.zone_column?/3, item 62a). Resolved in the
  # database each relation names (the connection's current one when it
  # names none).
  # DECIMAL carries its scale, so a scope literal can be checked for exact
  # representation (R3 clarification 2).
  @storage_type_sql "CASE WHEN DATA_TYPE IN ('decimal', 'numeric') THEN " <>
                      "CONCAT(DATA_TYPE, '(', NUMERIC_PRECISION, ',', NUMERIC_SCALE, ')') " <>
                      "ELSE DATA_TYPE END AS DATA_TYPE"

  def prepare_read(%{domain: domain} = selecto, connection) when is_map(domain) do
    with :ok <- nested_cte_shadowing(selecto) do
      prepare_storage_types(selecto, domain, connection)
    end
  end

  def prepare_read(selecto, _connection), do: {:ok, selecto}

  # MariaDB lets a name from an enclosing WITH win over a CTE or VALUES list of
  # the same name that a CTE body defines in a WITH of its own (11.4: `WITH x AS
  # (SELECT 1 v), y AS (WITH x AS (SELECT 2 v) SELECT v FROM x) SELECT v FROM y`
  # gives 1). Such a body is refused rather than read the outer one. A derived
  # table (a subquery join) shadows as written and is left alone.
  # A body's list identical to the enclosing one of that name reads the same rows
  # either way, and is allowed. Under WITH RECURSIVE MariaDB shows a body the
  # statement's later CTEs too, so they count as enclosing there.
  defp nested_cte_shadowing(selecto) do
    selecto
    |> Selecto.Executor.read_scopes()
    |> Enum.filter(&(&1.kind in [:cte_body, :recursive_part]))
    |> Enum.flat_map(fn %{query: body, outer: outer, enclosing: enclosing} ->
      [parent | _] = enclosing
      visible = if recursive_with?(parent), do: outer ++ list_names(parent), else: outer
      enclosing_lists = Enum.flat_map(enclosing, &lists/1)

      for list <- lists(body),
          name = list_name(list),
          name in visible,
          list not in enclosing_lists,
          do: name
    end)
    |> case do
      [] ->
        :ok

      names ->
        {:error,
         %Selecto.Error{
           type: :query_enforcement,
           message:
             "MariaDB does not let a CTE body's own CTE or VALUES list shadow an " <>
               "enclosing one of the same name",
           details: %{
             code: :query_rule_not_evaluable,
             reason: :nested_cte_shadowing,
             relations: Enum.uniq(names),
             adapter: :mariadb
           }
         }}
    end
  end

  defp lists(query) do
    set = Map.get(query, :set) || %{}
    List.wrap(Map.get(set, :ctes)) ++ List.wrap(Map.get(set, :values_clauses))
  end

  defp list_name(%{name: name}) when not is_nil(name), do: to_string(name)
  defp list_name(%{alias: name}) when not is_nil(name), do: to_string(name)
  defp list_name(_list), do: nil

  defp list_names(query), do: query |> lists() |> Enum.map(&list_name/1) |> Enum.reject(&is_nil/1)

  defp recursive_with?(query),
    do:
      query
      |> Map.get(:set, %{})
      |> Map.get(:ctes)
      |> List.wrap()
      |> Enum.any?(&match?(%{type: :recursive}, &1))

  defp prepare_storage_types(selecto, domain, connection) do
    domains = [domain | Enum.map(Selecto.Executor.set_operands(selecto), &Map.get(&1, :domain))]

    names = domains |> Enum.reduce([], &collect_relations/2) |> Enum.uniq()

    case Enum.flat_map(names, fn name -> Enum.map(split_relation(name), &{name, &1}) end) do
      [] ->
        {:ok, selecto}

      relations ->
        # One query per relation, tagged with the domain's own name for it,
        # so the types are keyed by the exact (database, table) the server
        # resolves for that name, then column (R18, R31). Names are never
        # case-folded: with lower_case_table_names = 0 the database and
        # table match by BINARY comparison (`rv_v` and `rv_V` are two
        # tables); otherwise the server folds them and so does the match.
        # A name that matches more than one table refuses (fail closed).
        name_match = fn column ->
          "(BINARY #{column} = BINARY ? OR (@@lower_case_table_names <> 0 AND #{column} = ?))"
        end

        sql =
          Enum.map_join(relations, " UNION ALL ", fn
            {_name, {nil, _table}} ->
              "SELECT ? AS selecto_relation, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, " <>
                "#{@storage_type_sql} " <>
                "FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA = BINARY DATABASE() " <>
                "AND " <> name_match.("TABLE_NAME")

            {_name, {_schema, _table}} ->
              "SELECT ? AS selecto_relation, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, " <>
                "#{@storage_type_sql} " <>
                "FROM information_schema.COLUMNS WHERE " <>
                name_match.("TABLE_SCHEMA") <> " AND " <> name_match.("TABLE_NAME")
          end)

        params =
          Enum.flat_map(relations, fn
            {name, {nil, table}} -> [name, table, table]
            {name, {schema, table}} -> [name, schema, schema, table, table]
          end)

        case execute(connection, sql, params, selecto_catalog: true) do
          {:ok, %{rows: rows}} ->
            ambiguous =
              rows
              |> Enum.group_by(&hd/1, fn [_, schema, table | _] -> {schema, table} end)
              |> Enum.filter(fn {_name, tables} -> length(Enum.uniq(tables)) > 1 end)
              |> Enum.map(&elem(&1, 0))

            if ambiguous != [] do
              {:error,
               %Selecto.Error{
                 type: :query_enforcement,
                 message: "a domain relation name matches more than one table",
                 details: %{
                   code: :query_rule_not_evaluable,
                   reason: :relation_ambiguous,
                   relations: ambiguous,
                   adapter: :mariadb
                 }
               }}
            else
              types =
                Enum.reduce(rows, %{}, fn [relation, _schema, _table, column, type], acc ->
                  relation = to_string(relation)
                  column = column |> to_string() |> String.downcase()
                  type = type |> to_string() |> String.downcase()

                  Map.update(acc, relation, %{column => MapSet.new([type])}, fn columns ->
                    Map.update(columns, column, MapSet.new([type]), &MapSet.put(&1, type))
                  end)
                end)

              types = prove_view_columns(connection, rows, types)
              set = Map.get(selecto, :set) || %{}

              set =
                set
                |> Map.put(:storage_column_types, types)
                |> Map.put(:zone_independent_timestamps, true)

              {:ok, %{selecto | set: set}}
            end

          {:error, _} = error ->
            error
        end
    end
  end

  # MySQL lists a view's base tables in information_schema.VIEW_TABLE_USAGE;
  # MariaDB has no such table (the shape test alone proves the column).
  @view_column_usage false
  # R40, R40b, R40c: a column of a VIEW used in a scope compare keeps a type
  # only when the view text proves it is a base-table column passed through
  # as is: the view is one plain SELECT (or a UNION whose every branch is
  # one) whose FROM list holds only base tables, and the column's item is
  # exactly `alias`.`col` AS `name` with the alias naming one of them; on
  # MySQL, information_schema.VIEW_TABLE_USAGE must also list that base
  # table (MySQL 8.4 has no VIEW_COLUMN_USAGE; the shape test proves the
  # column). The column then takes the base column's type (a TIMESTAMP
  # compares by epoch). Anything else (DATE_FORMAT(ts), ts + INTERVAL 0
  # SECOND, a derived table, a nested view) is unproven and a scope compare
  # on it refuses (column_unresolved), whatever type the view reports.
  @unproven "selecto_unproven_view_column"

  defp prove_view_columns(connection, rows, types) do
    columns =
      Enum.map(rows, fn [tag, schema, table, column, type] ->
        {to_string(tag), to_string(schema), to_string(table), to_string(column),
         String.downcase(to_string(type))}
      end)

    relations = columns |> Enum.map(fn {_, s, t, _, _} -> {s, t} end) |> Enum.uniq()

    with [_ | _] <- relations,
         {:ok, %{rows: definitions}} <-
           execute(
             connection,
             Enum.map_join(relations, " UNION ALL ", fn _ ->
               "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS " <>
                 "WHERE BINARY TABLE_SCHEMA = BINARY ? AND BINARY TABLE_NAME = BINARY ?"
             end),
             Enum.flat_map(relations, fn {s, t} -> [s, t] end),
             selecto_catalog: true
           ) do
      definitions =
        Map.new(definitions, fn [s, t, d] ->
          {{to_string(s), to_string(t)}, to_string(d || "")}
        end)

      Enum.reduce(columns, types, fn {tag, schema, table, column, _reported}, acc ->
        case Map.fetch(definitions, {schema, table}) do
          :error ->
            acc

          {:ok, definition} ->
            # An unproven column refuses scope compares here, whatever the
            # view reports for it: MariaDB's derived column types do not prove
            # the base column's.
            kind = view_column_type(connection, schema, table, definition, column)
            put_in(acc, [tag, String.downcase(column)], MapSet.new([kind]))
        end
      end)
    else
      [] -> types
      _error -> mark_unproven(columns, types)
    end
  end

  defp view_column_type(connection, schema, view, definition, column) do
    # R40d: a backslash anywhere in the view text (MySQL and MariaDB store
    # \' escapes in string literals) makes the scan uncertain: no proof.
    with false <- String.contains?(definition, "\\"),
         {:ok, branches} <- view_branches(definition),
         sources when is_list(sources) <-
           Enum.reduce_while(branches, [], fn branch, acc ->
             case branch_source(branch, schema, column) do
               {:ok, source} -> {:cont, [source | acc]}
               :error -> {:halt, :error}
             end
           end),
         types when is_list(types) <-
           Enum.reduce_while(sources, [], fn source, acc ->
             case base_column_type(connection, schema, view, source) do
               {:ok, type} -> {:cont, [type | acc]}
               :error -> {:halt, :error}
             end
           end),
         [type] <- Enum.uniq(types) do
      type
    else
      _ -> @unproven
    end
  end

  # The view's SELECT branches, each one plain: any set operator (UNION,
  # INTERSECT, EXCEPT, MINUS) makes more than one, and no proof (R40d).
  defp view_branches(definition) do
    branches =
      definition
      |> split_top(~r/\A(?:union|intersect|except|minus)(?:\s+(?:all|distinct))?\b/i)
      |> Enum.map(&strip_parens/1)

    # R40d: one SELECT only, with no subquery or derived table anywhere.
    single? = match?([_], branches) and not Regex.match?(~r/\(\s*select\b/i, definition)

    if single? and Enum.all?(branches, &Regex.match?(~r/\Aselect\s/i, &1)),
      do: {:ok, branches},
      else: :error
  end

  # {db, table, column} of the base column the branch's item `column` passes
  # through, or :error.
  defp branch_source(branch, schema, column) do
    body = Regex.replace(~r/\Aselect\s+(?:distinct\s+)?/i, branch, "")

    with [items, from] <- split_top(body, ~r/\Afrom\b/i, 2),
         [from | _] <- split_top(from, ~r/\A(?:where|group|having|order|limit|window)\b/i, 2),
         {:ok, tables} <- from_tables(from, schema),
         ident = "`((?:[^`]|``)+)`",
         target = "`" <> Regex.escape(String.replace(column, "`", "``")) <> "`",
         [_ | _] = matches <-
           items
           |> split_top(~r/\A,/)
           |> Enum.map(&String.trim/1)
           |> Enum.map(
             &Regex.run(~r/\A(?:#{ident}\.)?#{ident}\.#{ident}\s+AS\s+#{target}\z/i, &1,
               capture: :all_but_first
             )
           )
           |> Enum.reject(&is_nil/1),
         [[db, qualifier, base_column]] <- matches,
         {:ok, {base_db, base_table}} <- Map.fetch(tables, {unescape(db), unescape(qualifier)}) do
      {:ok, {base_db, base_table, unescape(base_column)}}
    else
      _ -> :error
    end
  end

  # alias (and table name) => {db, table} for a FROM list of base-table
  # references only (comma lists and joins); a derived table refuses.
  defp from_tables(from, schema) do
    ident = "`((?:[^`]|``)+)`"

    items =
      from
      |> split_top(
        ~r/\A(?:,|(?:(?:natural|inner|cross|straight_join|left|right)(?:\s+outer)?\s+)*join\b|straight_join\b)/i
      )
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&(&1 |> split_top(~r/\A(?:on|using)\b/i, 2) |> hd() |> String.trim()))

    Enum.reduce_while(items, {:ok, %{}}, fn item, {:ok, acc} ->
      case Regex.run(~r/\A(?:#{ident}\.)?#{ident}(?:\s+(?:AS\s+)?#{ident})?\z/i, item,
             capture: :all_but_first
           ) do
        nil ->
          {:halt, :error}

        parts ->
          [db, table | alias] = parts ++ []
          db = if db == "", do: schema, else: unescape(db)
          table = unescape(table)

          name =
            case alias do
              [a] when a != "" -> unescape(a)
              _ -> table
            end

          acc = Map.put(acc, {"", name}, {db, table})
          acc = if alias in [[], [""]], do: Map.put(acc, {db, table}, {db, table}), else: acc
          {:cont, {:ok, acc}}
      end
    end)
  end

  defp base_column_type(connection, schema, view, {db, table, column}) do
    vcu =
      if @view_column_usage,
        do:
          " AND EXISTS (SELECT 1 FROM information_schema.VIEW_TABLE_USAGE u " <>
            "WHERE BINARY u.VIEW_SCHEMA = BINARY ? AND BINARY u.VIEW_NAME = BINARY ? " <>
            "AND BINARY u.TABLE_SCHEMA = BINARY c.TABLE_SCHEMA AND BINARY u.TABLE_NAME = BINARY c.TABLE_NAME)",
        else: ""

    params = [db, table, column] ++ if(@view_column_usage, do: [schema, view], else: [])

    case execute(
           connection,
           "SELECT #{@storage_type_sql} FROM information_schema.COLUMNS c " <>
             "JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA " <>
             "AND t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_TYPE = 'BASE TABLE' " <>
             "WHERE BINARY c.TABLE_SCHEMA = BINARY ? AND BINARY c.TABLE_NAME = BINARY ? " <>
             "AND c.COLUMN_NAME = ?" <> vcu,
           params,
           selecto_catalog: true
         ) do
      {:ok, %{rows: [[type]]}} -> {:ok, String.downcase(to_string(type))}
      _ -> :error
    end
  end

  # Splits `text` where `separator` matches at parenthesis depth 0, outside
  # backtick identifiers and string literals (at most `parts` pieces).
  defp split_top(text, separator, parts \\ :infinity),
    do: split_top(text, separator, parts, 0, 0, nil, [])

  defp split_top(text, _separator, _parts, start, i, _quote, acc) when i >= byte_size(text),
    do: Enum.reverse([binary_part(text, start, byte_size(text) - start) | acc])

  defp split_top(text, separator, parts, start, i, quote, acc) do
    char = :binary.at(text, i)
    boundary? = i == 0 or not word_char?(:binary.at(text, i - 1)) or char == ?,

    match =
      if quote == nil and char not in [?`, ?', ?", ?(] and boundary? and
           (parts == :infinity or length(acc) + 1 < parts),
         do: Regex.run(separator, binary_part(text, i, byte_size(text) - i)),
         else: nil

    cond do
      quote != nil ->
        split_top(
          text,
          separator,
          parts,
          start,
          i + 1,
          if(char == quote, do: nil, else: quote),
          acc
        )

      char in [?`, ?', ?"] ->
        split_top(text, separator, parts, start, i + 1, char, acc)

      char == ?( ->
        close = matching_paren(text, i)
        split_top(text, separator, parts, start, close + 1, nil, acc)

      match != nil ->
        piece = binary_part(text, start, i - start)
        next = i + byte_size(hd(match))
        split_top(text, separator, parts, next, next, nil, [piece | acc])

      true ->
        split_top(text, separator, parts, start, i + 1, nil, acc)
    end
  end

  defp matching_paren(text, i), do: matching_paren(text, i + 1, 1, nil)

  defp matching_paren(text, i, _depth, _quote) when i >= byte_size(text), do: byte_size(text) - 1

  defp matching_paren(text, i, depth, quote) do
    char = :binary.at(text, i)

    cond do
      quote != nil -> matching_paren(text, i + 1, depth, if(char == quote, do: nil, else: quote))
      char in [?`, ?', ?"] -> matching_paren(text, i + 1, depth, char)
      char == ?( -> matching_paren(text, i + 1, depth + 1, nil)
      char == ?) and depth == 1 -> i
      char == ?) -> matching_paren(text, i + 1, depth - 1, nil)
      true -> matching_paren(text, i + 1, depth, nil)
    end
  end

  defp word_char?(char), do: char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_

  defp strip_parens(text) do
    text = String.trim(text)

    if String.starts_with?(text, "(") and matching_paren(text, 0) == byte_size(text) - 1,
      do: strip_parens(binary_part(text, 1, byte_size(text) - 2)),
      else: text
  end

  defp unescape(name), do: String.replace(name || "", "``", "`")

  defp mark_unproven(columns, types) do
    Enum.reduce(columns, types, fn {tag, _schema, _table, column, _reported}, acc ->
      put_in(acc, [tag, String.downcase(column)], MapSet.new([@unproven]))
    end)
  end

  # Every relation a domain names: source and schema tables, association
  # bridges.
  defp collect_relations(map, acc) when is_map(map) and not is_struct(map) do
    Enum.reduce(map, acc, fn
      {key, value}, acc
      when key in [:source_table, "source_table"] and
             (is_binary(value) or (is_atom(value) and not is_nil(value))) ->
        [to_string(value) | acc]

      {key, %{} = through}, acc when key in [:through, "through"] ->
        table = Map.get(through, :table) || Map.get(through, "table")
        acc = if is_binary(table), do: [table | acc], else: acc
        collect_relations(through, acc)

      {_key, value}, acc ->
        collect_relations(value, acc)
    end)
  end

  defp collect_relations(list, acc) when is_list(list),
    do: Enum.reduce(list, acc, &collect_relations/2)

  defp collect_relations(_value, acc), do: acc

  defp split_relation(relation) do
    case String.split(relation, ".") do
      [table] -> [{nil, table}]
      [schema, table] -> [{schema, table}]
      _ -> []
    end
  end

  @doc false
  # A statement on a connection whose session this call already verified.
  def execute_unchecked(connection, query, params, opts) do
    with :ok <- require_binary_protocol(opts) do
      connection = resolve_connection(connection)

      case MyXQL.query(connection, statement(query, opts), params, opts) do
        {:ok, result} ->
          # A stored value outside the supported range is a typed error
          # (SelectoDBMariaDB.StoredValues).
          with :ok <- Session.not_cut(connection, result.num_warnings, opts),
               :ok <- StoredValues.check(result.rows),
               do: {:ok, normalize_result(result)}

        {:error, reason} ->
          {:error, sanitize_error(reason)}
      end
    end
  end

  # MyXQL always negotiates CLIENT_MULTI_STATEMENTS, so a text-protocol query
  # would run every statement in the string. Prepared (binary) statements hold
  # exactly one.
  defp require_binary_protocol(opts) do
    case Keyword.get(opts, :query_type, :binary) do
      :binary ->
        :ok

      query_type ->
        {:error,
         Selecto.Error.validation_error("MariaDB adapter executes only prepared statements", %{
           adapter: :mariadb,
           option: :query_type,
           value: query_type
         })}
    end
  end

  @unique_violation_codes [1022, 1062, 1169, 1586]
  @foreign_key_violation_codes [1216, 1217, 1451, 1452]
  @not_null_violation_codes [1048, 1364]
  @check_violation_codes [3819, 4025]
  # Strict-mode value errors: out of range (1264, 1690), truncated (1265,
  # 1406), incorrect value (1292, 1366, 1367). The value does not fit its
  # column; nothing about the value leaves the adapter.
  @data_violation_codes [1264, 1265, 1292, 1366, 1367, 1406, 1690]
  # The server rolled the whole transaction back (1213) or gave up waiting for
  # a lock (1205); the caller may retry.
  @deadlock_codes [1213]
  @lock_timeout_codes [1205]
  # KILL QUERY (1317) and the server's statement time limit (3024 on MySQL,
  # 1969 on MariaDB).
  @canceled_codes [1317, 3024, 1969]

  # Driver errors carry the server's message (which can quote row values) and
  # the statement text. Only a stable category and the numeric server code
  # leave the adapter.
  @doc false
  # A read's own READ ONLY transaction refused a write it reached (R55).
  def sanitize_error(%MyXQL.Error{mysql: %{code: 1792}}) do
    %Selecto.Error{
      type: :query_enforcement,
      message: "MariaDB refuses a read that tried to write",
      details: %{code: :query_rule_not_evaluable, reason: :read_only_read, adapter: :mariadb}
    }
  end

  def sanitize_error(%MyXQL.Error{mysql: %{code: code}}) when code in @canceled_codes do
    Selecto.Error.query_error("MariaDB canceled the statement", nil, [], %{
      adapter: :mariadb,
      category: :query_canceled,
      code: code,
      sqlstate: "70100"
    })
  end

  def sanitize_error(%MyXQL.Error{mysql: %{code: code}}) when is_integer(code) do
    Selecto.Error.query_error("MariaDB rejected the statement", nil, [], %{
      adapter: :mariadb,
      category: error_category(code),
      code: code
    })
  end

  def sanitize_error(%DBConnection.ConnectionError{}) do
    Selecto.Error.connection_error("MariaDB connection failed", %{
      adapter: :mariadb,
      category: :connection_error
    })
  end

  def sanitize_error(%Selecto.Error{} = error), do: error

  def sanitize_error(_reason) do
    Selecto.Error.query_error("MariaDB query failed", nil, [], %{
      adapter: :mariadb,
      category: :database_error
    })
  end

  defp error_category(code) when code in @unique_violation_codes, do: :unique_violation
  defp error_category(code) when code in @foreign_key_violation_codes, do: :foreign_key_violation
  defp error_category(code) when code in @not_null_violation_codes, do: :not_null_violation
  defp error_category(code) when code in @check_violation_codes, do: :check_violation
  defp error_category(code) when code in @data_violation_codes, do: :data_violation
  defp error_category(code) when code in @deadlock_codes, do: :deadlock_detected
  defp error_category(code) when code in @lock_timeout_codes, do: :lock_timeout
  defp error_category(_code), do: :database_error

  @impl true
  def placeholder(_index), do: "?"

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "`", "``")
    "`#{escaped}`"
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def format_datetime(expression, "YYYY-Q") do
    [
      "CONCAT(DATE_FORMAT(",
      expression,
      ", '%Y'), '-', QUARTER(",
      expression,
      "))"
    ]
  end

  def format_datetime(expression, format) do
    case mysql_datetime_format(format) do
      nil -> ["CAST(", expression, " AS CHAR)"]
      native_format -> ["DATE_FORMAT(", expression, ", '", native_format, "')"]
    end
  end

  @impl true
  def supports?(feature),
    do:
      feature in [
        :stream,
        :json_agg_limit,
        :json_table_rowset,
        :postgresql_null_order,
        # Tenant and site columns compare by their stored bytes, never under
        # the column's collation (Selecto.Builder.ExactCompare).
        :exact_tenant_compare,
        # UUID columns are text or MariaDB's native UUID type, both written in
        # the string form: filters bind the string, not the 16-byte binary.
        :uuid_text_storage,
        # WITH ROLLUP returns no grand total on empty input; Selecto adds it
        # (Selecto.Builder.Sql.RollupTotal) as PostgreSQL returns it.
        :rollup_without_empty_total,
        :text_search,
        :fulltext_search,
        :cte,
        :window_functions,
        :transactions,
        :rollup,
        :rollup_with_rollup,
        :schema_introspection,
        :json
      ]

  @column_catalog_sql """
  SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ?
  """

  @impl Selecto.DB.WriteAdapter
  # information_schema COLUMN_TYPE (length, precision, scale, fractional
  # seconds) with ` collate <COLLATION_NAME>` (query_enforced_scalar v2
  # verified metadata; the collation name decides determinism).
  def column_catalog(connection, table) when is_binary(table) do
    {schema, name} =
      case String.split(table, ".", parts: 2) do
        [schema, name] -> {schema, name}
        [name] -> {nil, name}
      end

    case execute(connection, @column_catalog_sql, [schema, name], selecto_catalog: true) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Map.new(rows, fn [column, type, collation] ->
           type = String.downcase(to_string(type))
           type = if is_binary(collation), do: "#{type} collate #{collation}", else: type
           {to_string(column), %{type: type, deterministic: nil}}
         end)}

      {:error, _reason} = error ->
        error
    end
  end

  @impl Selecto.DB.WriteAdapter
  def write_capabilities(connection) do
    %{
      protocol_version: Selecto.Write.Capabilities.protocol_version(),
      insert: true,
      update: true,
      upsert: :single_declared_conflict_target,
      delete: true,
      returning: false,
      generated_keys: false,
      transactions: true,
      atomic_batch: true,
      write_graph: false,
      prepared_candidate_state: false,
      # Guarded inserts are re-read under their predicate in the write
      # transaction (SelectoDBMariaDB.InsertAdmission), so DECIMAL scale
      # rounding is part of admission.
      typed_insert_admission: true,
      typed_insert_precision: true,
      dialect: :mariadb,
      server_version: server_version(connection),
      upsert_strategy: :on_duplicate_key
    }
  end

  @impl Selecto.DB.WriteAdapter
  def preview_write(_connection, %Command{} = command, opts),
    do: WriteCompiler.preview(command, opts)

  def preview_write(_connection, %Batch{} = batch, opts), do: WriteCompiler.preview(batch, opts)
  def preview_write(_connection, %Graph{} = graph, _opts), do: unsupported_graph(graph)
  def preview_write(_connection, write, _opts), do: invalid_write_input(write)

  @doc """
  Executes a governed write.

  `opts[:authorization]` must be the `Selecto.Write.Authorization` that the
  governed entry point (`SelectoUpdato`) issued for exactly this command,
  batch, or graph. Without it the write fails with `:ungoverned_write` before
  any statement runs.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_write(connection, write, opts) do
    with :ok <- Selecto.Write.Authorization.require_for(write, opts) do
      execute_write_unsafe(connection, write, opts)
    end
  end

  @doc """
  Executes a write without domain governance.

  For trusted tooling and adapter tests only; application code writes through
  `SelectoUpdato`.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_write_unsafe(connection, %Command{} = command, opts) do
    with :ok <- Command.validate(command) do
      with_write_transaction(connection, [command.relation], fn tx, mode ->
        with {:ok, session} <- verify_write_session(tx, mode),
             opts = Keyword.put(opts, :selecto_session, session),
             {:ok, result} <- execute_write_command(tx, command, opts),
             :ok <- session_unchanged(tx, opts) do
          {:ok, result}
        end
      end)
    end
  end

  def execute_write_unsafe(connection, %Batch{} = batch, opts) do
    with :ok <- Batch.validate(batch) do
      relations = Enum.map(batch.commands, & &1.relation)

      with_write_transaction(connection, relations, fn tx, mode ->
        with {:ok, session} <- verify_write_session(tx, mode) do
          opts =
            opts
            |> Keyword.put(:batch_commands, length(batch.commands))
            |> Keyword.put(:selecto_session, session)

          batch.commands
          |> Enum.reduce_while({:ok, []}, fn command, {:ok, results} ->
            case execute_write_command(tx, command, opts) do
              {:ok, result} -> {:cont, {:ok, results ++ [result]}}
              {:error, _} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, results} ->
              with :ok <- session_unchanged(tx, opts), do: {:ok, results}

            error ->
              error
          end
        end
      end)
    end
  end

  def execute_write_unsafe(_connection, %Graph{} = graph, _opts), do: unsupported_graph(graph)
  def execute_write_unsafe(_connection, write, _opts), do: invalid_write_input(write)

  # The write's own transaction connection carries the session check
  # (SelectoDBMariaDB.Session) before any statement of the write runs. A
  # managed transaction on a connection whose session has autocommit=0 would
  # BEGIN over the host's open implicit transaction (item 42; a pooled
  # connection handed back that way, or a host's own): it is refused, never
  # written into.
  defp verify_write_session(_tx, {:verified, session}), do: {:ok, session}

  defp verify_write_session(tx, mode) do
    case Session.state(tx) do
      {:ok, %{autocommit: autocommit}} when mode == :managed and autocommit in [0, "0"] ->
        {:error,
         Error.new(
           :transaction_unsupported,
           "MariaDB connection holds an open implicit transaction (autocommit=0)",
           details: %{adapter: :mariadb, code: :leaked_transaction}
         )}

      {:ok, session} ->
        {:ok, session}

      {:error, %Selecto.Error{details: %{category: :invalid_session, setting: setting}}} ->
        {:error,
         Error.new(:invalid_connection, "MariaDB session settings are not safe for Selecto",
           details: %{adapter: :mariadb, code: :invalid_session, setting: setting}
         )}

      {:error, %Selecto.Error{details: %{category: category}}} ->
        execution_failed(category)

      {:error, reason} ->
        {:error, write_error(:execution_failed, reason)}
    end
  end

  # The session's time_zone and sql_mode after a write statement (items 59,
  # 62): a row trigger can SET them, and MariaDB does not roll a session
  # variable back. A change restores the values seen before and refuses; the
  # write's transaction or savepoint then rolls its DML back.
  defp session_unchanged(tx, opts) do
    case Keyword.get(opts, :selecto_session) do
      nil ->
        :ok

      session ->
        case Session.unchanged(tx, session) do
          :ok ->
            :ok

          {:error, %Selecto.Error{details: %{reason: :session_state_changed}}} ->
            {:error,
             Error.new(
               :query_enforcement,
               "MariaDB write changed the session's time_zone or sql_mode",
               details: %{
                 code: :query_rule_violation,
                 reason: :session_state_changed,
                 adapter: :mariadb
               }
             )}

          # The check itself failed (a kill, a lost connection): that failure
          # is what the caller is told.
          {:error, %Selecto.Error{details: %{category: category}}} ->
            execution_failed(category)

          {:error, reason} ->
            {:error, write_error(:execution_failed, reason)}
        end
    end
  end

  # Upserts run the target-exact emulation (SelectoDBMariaDB.Upsert), never
  # ON DUPLICATE KEY UPDATE.
  defp execute_write_command(connection, %Command{operation: :upsert} = command, opts) do
    with :ok <-
           InsertAdmission.require_transactional(connection, command, &execute_unchecked/4, opts),
         # After the metadata lock, like every catalog read of the write.
         {:ok, command} <- with_storage_types(connection, command, opts),
         :ok <- InsertAdmission.before_upsert(connection, command, &execute_unchecked/4, opts),
         {:ok, plan} <- WriteCompiler.upsert_plan(command, opts),
         {:ok, affected} <-
           Upsert.execute(
             connection,
             command,
             plan,
             &execute_unchecked/4,
             &control_statement/2,
             opts
           ),
         :ok <- session_unchanged(connection, opts),
         {:ok, affected} <- check_cardinality(affected, command.expected_cardinality) do
      {:ok,
       %Result{
         operation: :upsert,
         affected_rows: affected,
         rows: [],
         metadata: %{dialect: :mariadb}
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, %Selecto.Error{details: %{category: category}}} -> execution_failed(category)
      {:error, reason} -> {:error, write_error(:execution_failed, reason)}
    end
  end

  defp execute_write_command(connection, command, opts) do
    with :ok <-
           InsertAdmission.require_transactional(connection, command, &execute_unchecked/4, opts),
         # After the metadata lock, like every catalog read of the write.
         {:ok, command} <- with_storage_types(connection, command, opts),
         :ok <-
           InsertAdmission.before_insert(connection, command, &execute_unchecked/4, opts),
         {:ok, locked} <-
           InsertAdmission.before_update(connection, command, &execute_unchecked/4, opts),
         {:ok, statement} <- WriteCompiler.compile(command, opts),
         {:ok, query_result} <-
           InsertAdmission.storage_error(
             command,
             execute_unchecked(connection, statement.text, statement.params, opts)
           ),
         :ok <- session_unchanged(connection, opts),
         {:ok, affected_rows} <- enforce_cardinality(command, query_result),
         :ok <-
           InsertAdmission.after_insert(
             connection,
             command,
             query_result,
             &execute_unchecked/4,
             opts
           ),
         :ok <-
           InsertAdmission.after_update(connection, command, locked, &execute_unchecked/4, opts) do
      {:ok,
       %Result{
         operation: command.operation,
         affected_rows: affected_rows,
         rows: result_rows(query_result),
         metadata: %{dialect: :mariadb}
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, %Selecto.Error{details: %{category: category}}} -> execution_failed(category)
      {:error, reason} -> {:error, write_error(:execution_failed, reason)}
    end
  end

  defp scope_fields(%Command{metadata: metadata}) do
    metadata = metadata || %{}

    guards =
      [
        get_in(metadata, [:post_image, :predicate]),
        get_in(metadata, [:insert_admission, :predicate])
      ]

    (List.wrap(Map.get(metadata, :tenant_fields, [])) ++
       Enum.flat_map(guards, &predicate_fields/1))
    |> Enum.map(&(&1 |> to_string() |> String.split(".") |> List.last() |> String.downcase()))
    |> Enum.uniq()
  end

  defp predicate_fields({:field, field}), do: [to_string(field)]

  defp predicate_fields(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.flat_map(&predicate_fields/1)

  defp predicate_fields(list) when is_list(list), do: Enum.flat_map(list, &predicate_fields/1)
  defp predicate_fields(_value), do: []

  # The columns of the written relation stored as TIMESTAMP: every
  # comparison of the write and its re-reads on one compares by epoch
  # (WriteCompiler, item 62a).
  defp with_storage_types(connection, %Command{} = command, opts) do
    {schema_sql, params} =
      case command.relation |> to_string() |> String.split(".") do
        [schema, table] -> {"?", [schema, table]}
        [table] -> {"DATABASE()", [table]}
        _other -> {"DATABASE()", [to_string(command.relation)]}
      end

    case execute_unchecked(
           connection,
           "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = " <>
             schema_sql <> " AND TABLE_NAME = ? AND DATA_TYPE = 'timestamp'",
           params,
           opts
         ) do
      {:ok, %{rows: rows}} ->
        # Only scope columns (tenant fields, the post-image and admission
        # guards) take the epoch form; the caller's own predicates keep the
        # server's semantics (R12).
        scope = scope_fields(command)

        fields =
          rows
          |> Enum.map(fn [column] -> to_string(column) end)
          |> Enum.filter(&(String.downcase(&1) in scope))

        {:ok, %{command | metadata: Map.put(command.metadata, :storage_timestamp_fields, fields)}}

      {:error, _} = error ->
        error
    end
  end

  defp execution_failed(category), do: {:error, write_error(:execution_failed, category)}

  # Savepoint statements of a write (text protocol; fixed statement text).
  defp control_statement(connection, sql) do
    case MyXQL.query(connection, sql, [], query_type: :text) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, sanitize_error(reason)}
    end
  end

  defp enforce_cardinality(%Command{expected_cardinality: expected}, result),
    do: check_cardinality(Map.get(result, :num_rows, 0), expected)

  @doc false
  def logical_affected_rows(:upsert, physical) when physical in [1, 2], do: 1
  def logical_affected_rows(_operation, physical), do: physical

  defp check_cardinality(count, expected) do
    if cardinality_matches?(count, expected) do
      {:ok, count}
    else
      {:error,
       Error.new(:cardinality_mismatch, "write affected an unexpected number of rows",
         details: %{expected: expected, actual: count}
       )}
    end
  end

  defp cardinality_matches?(count, {:exactly, expected}), do: count == expected
  defp cardinality_matches?(count, {:at_most, expected}), do: count <= expected
  defp cardinality_matches?(count, {:at_least, expected}), do: count >= expected
  defp cardinality_matches?(count, {:between, minimum, maximum}), do: count in minimum..maximum
  defp cardinality_matches?(_count, :many), do: true

  defp result_rows(%{rows: rows, columns: columns}) do
    Enum.map(rows, fn row -> Map.new(Enum.zip(columns, row)) end)
  end

  # Inside the caller's transaction the write runs under its own savepoint,
  # so a refusal after its DML (a post-image violation) undoes that DML even
  # when the caller ignores the error and commits (w3exrev16 item 18).
  #
  # A checked-out connection with autocommit=0 is inside the host's implicit
  # transaction even before its first statement (items 29, 42): the write
  # takes a savepoint there too, never BEGIN/COMMIT (which would commit the
  # host's work, item 39).
  defp with_write_transaction(%DBConnection{} = connection, relations, fun) do
    case {DBConnection.status(connection), implicit_transaction(connection)} do
      {:transaction, _} -> with_savepoint(connection, &fun.(&1, :host))
      {_idle, {:ok, true}} -> with_savepoint(connection, &fun.(&1, :host))
      {_idle, {:ok, false}} -> owned_write(connection, relations, fun)
      {_idle, {:error, reason}} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  # A pid or pool: the checkout is Selecto's, so a transaction on it is one
  # nobody here holds. Beginning over it would be an implicit commit or a
  # rollback of the host's work, so the write refuses without a statement
  # (as PostgreSQL does).
  defp with_write_transaction(connection, relations, fun),
    do: owned_write(connection, relations, fun)

  defp owned_write(connection, relations, fun) do
    connection
    |> Session.checkout(fn checked_out ->
      if DBConnection.status(checked_out) == :idle,
        do: with_managed_transaction(checked_out, relations, fun),
        else: {:error, unowned_transaction()}
    end)
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  rescue
    exception -> {:error, write_error(:transaction_failed, exception)}
  catch
    :exit, reason -> {:error, write_error(:transaction_failed, reason)}
  end

  # Checks come before START TRANSACTION, which would commit an open
  # transaction and release the session's table locks.
  defp with_managed_transaction(connection, relations, fun) do
    with {:ok, session} <- verify_write_session(connection, :managed),
         :ok <- no_host_locks(connection, relations) do
      Session.transact(connection, "START TRANSACTION", fn ->
        case fun.(connection, {:verified, session}) do
          {:ok, _} = ok -> {:commit, ok}
          {:error, _} = error -> {:rollback, error}
        end
      end)
    end
  end

  defp no_host_locks(connection, relations) do
    quoted = relations |> Enum.uniq() |> Enum.map(&WriteCompiler.quote_relation/1)

    case Session.host_locks(connection, quoted) do
      :none -> :ok
      held when held in [:held, :unsettled] -> {:error, host_table_locks()}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  defp unowned_transaction do
    Error.new(
      :query_enforcement,
      "MariaDB write refused: this connection is in a transaction Selecto did not open and " <>
        "the caller does not hold; use DBConnection.run/3 and pass its connection",
      details: %{code: :query_rule_not_evaluable, reason: :unowned_transaction, adapter: :mariadb}
    )
  end

  defp host_table_locks do
    Error.new(
      :query_enforcement,
      "MariaDB transaction refused: this session holds table locks that START TRANSACTION " <>
        "would release",
      details: %{code: :query_rule_not_evaluable, reason: :host_table_locks, adapter: :mariadb}
    )
  end

  # Neither answer is safe to guess: a savepoint outside a transaction undoes
  # nothing, and BEGIN inside one commits it.
  defp implicit_transaction(connection) do
    case MyXQL.query(connection, "SELECT @@session.autocommit", [], query_type: :text) do
      {:ok, %MyXQL.Result{rows: [[autocommit]]}} -> {:ok, autocommit in [0, "0"]}
      {:ok, _unreadable} -> {:error, :session_unreadable}
      {:error, _} = error -> error
    end
  end

  defp with_savepoint(connection, fun) do
    name = "selecto_write_#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}"
    query = &MyXQL.query(connection, &1, [], query_type: :text)

    case query.("SAVEPOINT #{name}") do
      {:ok, _} ->
        result =
          try do
            fun.(connection)
          rescue
            exception -> {:error, write_error(:transaction_failed, exception)}
          end

        case result do
          {:ok, _} = ok ->
            case query.("RELEASE SAVEPOINT #{name}") do
              {:ok, _} ->
                ok

              {:error, reason} ->
                rollback_to_savepoint(connection, query, name)
                {:error, write_error(:transaction_failed, reason)}
            end

          {:error, %Error{}} = error ->
            rollback_to_savepoint(connection, query, name)
            error

          {:error, reason} ->
            rollback_to_savepoint(connection, query, name)
            {:error, write_error(:transaction_failed, reason)}
        end

      {:error, reason} ->
        {:error, write_error(:transaction_failed, reason)}
    end
  end

  # A deadlock (or another error InnoDB answers by rolling back the whole
  # transaction) leaves no savepoint to return to: the caller's transaction
  # is then marked failed, as a nested rollback would, so it cannot commit
  # as if its earlier statements had survived.
  defp rollback_to_savepoint(connection, query, name, attempts \\ 3) do
    case query.("ROLLBACK TO SAVEPOINT #{name}") do
      {:ok, _} ->
        _ = query.("RELEASE SAVEPOINT #{name}")
        :ok

      # A kill can land on the undo: the write's DML is still there.
      {:error, %MyXQL.Error{mysql: %{code: 1317}}} when attempts > 1 ->
        rollback_to_savepoint(connection, query, name, attempts - 1)

      {:error, _} ->
        Session.fail_transaction(connection)
    end
  end

  defp server_version(connection) do
    case introspection_query(connection, "SELECT VERSION()", []) do
      {:ok, %{rows: [[version] | _]}} -> to_string(version)
      _ -> nil
    end
  rescue
    _exception -> nil
  catch
    :exit, _reason -> nil
  end

  defp resolve_connection(%{adapter: _adapter, connection: connection}), do: connection
  defp resolve_connection(connection), do: connection

  defp unsupported_graph(graph) do
    {:error,
     Error.new(:write_capability_missing, "MariaDB adapter cannot preserve portable graph writes",
       details: %{write: graph, missing: [:write_graph, :generated_keys]}
     )}
  end

  defp invalid_write_input(write) do
    {:error,
     Error.new(:invalid_command, "expected a portable write command or batch",
       details: %{actual: write}
     )}
  end

  defp write_error(type, reason),
    do: Error.adapter_failure(type, :mariadb, reason, "MariaDB write failed")

  @impl true
  def rollup_sql(grouped_clauses), do: [grouped_clauses, " with rollup"]

  defp mysql_datetime_format("YYYY-MM-DD"), do: "%Y-%m-%d"
  defp mysql_datetime_format("YYYY-MM"), do: "%Y-%m"
  defp mysql_datetime_format("YYYY"), do: "%Y"
  defp mysql_datetime_format("YYYY-WW"), do: "%x-%v"
  defp mysql_datetime_format("MM"), do: "%m"
  defp mysql_datetime_format("DD"), do: "%d"
  defp mysql_datetime_format("D"), do: "%w"
  defp mysql_datetime_format("HH24"), do: "%H"
  defp mysql_datetime_format(_format), do: nil

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
  def list_relations(connection, opts \\ []) do
    include_views = Keyword.get(opts, :include_views, false)

    with {:ok, schema} <- resolve_schema(connection, opts) do
      query =
        if include_views do
          """
          SELECT table_name,
                 CASE table_type
                   WHEN 'BASE TABLE' THEN 'table'
                   WHEN 'VIEW' THEN 'view'
                 END AS source_kind
          FROM information_schema.tables
          WHERE table_schema = ?
            AND table_type IN ('BASE TABLE', 'VIEW')
          ORDER BY table_name
          """
        else
          """
          SELECT table_name, 'table' AS source_kind
          FROM information_schema.tables
          WHERE table_schema = ?
            AND table_type = 'BASE TABLE'
          ORDER BY table_name
          """
        end

      case introspection_query(connection, query, [schema]) do
        {:ok, %{rows: rows}} ->
          {:ok,
           Enum.map(rows, fn [table_name, source_kind] ->
             %{name: table_name, source_kind: normalize_relation_source_kind(source_kind)}
           end)}

        {:error, reason} ->
          {:error, {:query_failed, reason}}
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
    execute(connection, query, params, prepared: false, selecto_catalog: true)
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

  defp normalize_relation_source_kind("table"), do: :table
  defp normalize_relation_source_kind("view"), do: :view
  defp normalize_relation_source_kind(other), do: other

  @no_subquery_cache "SET STATEMENT optimizer_switch='subquery_cache=off'"
  # max_error_count at its maximum, so a cut cannot hide behind other warnings;
  # group_concat_max_len at the largest the server takes, since more warns or
  # fails; max_recursive_iterations at Selecto.Advanced.CTE's limit on max_depth.
  @read_pins ", max_error_count=65535, group_concat_max_len=1073741824, " <>
               "max_recursive_iterations=10000"

  @doc false
  # Every statement this adapter sends runs with MariaDB's subquery cache off.
  # The cache keys a correlated subquery's outer references under the
  # column's collation, so a row holding 'T7' (or 't7 ' under PAD SPACE)
  # reused the result computed for 't7' whatever exact comparison the SQL
  # used. SET STATEMENT applies to the one statement only, never to the
  # host's session. (Savepoint statements cannot carry it and hold no
  # subquery.)
  #
  # A statement Selecto built also pins what would cut its result silently
  # (R74). At the server's default of 1000 iterations a clean chain stops early.
  # A declared depth is its own bound, in the statement's text, so a cycle stops
  # there; 10,000 bounds a CTE that declares none, beside one that does or not.
  # What still gets cut fails in Session.not_cut/3.
  def statement(query, opts \\ []) do
    query = normalize_query(query)

    if String.starts_with?(query, @no_subquery_cache),
      do: query,
      else: @no_subquery_cache <> read_pins(opts) <> " FOR " <> query
  end

  defp read_pins(opts) do
    if Selecto.Executor.selecto_read?(opts) and not Session.host_text?(opts),
      do: @read_pins,
      else: ""
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

  @doc false
  def normalize_result(%{rows: rows} = result) do
    columns =
      result
      |> Map.get(:columns, [])
      |> Kernel.||([])
      |> Enum.map(&normalize_column_name/1)

    %{
      rows: rows || [],
      columns: columns,
      num_rows: Map.get(result, :num_rows, length(rows || [])),
      metadata: %{last_insert_id: Map.get(result, :last_insert_id)}
    }
  end

  defp normalize_column_name(%{name: name}) when is_binary(name), do: name
  defp normalize_column_name(name) when is_binary(name), do: name
  defp normalize_column_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_column_name(other), do: to_string(other)
end
