defmodule SelectoDBMariaDB.Session do
  @moduledoc false

  # Every read, stream and write first proves, on the very connection it then
  # uses, that the session can carry Selecto's semantics. One round trip per
  # call; nothing is cached per connection, since a host can change session
  # variables between calls.
  #
  # - time_zone is UTC ('UTC', '+00:00', 'Etc/UTC'), or SYSTEM while the
  #   server's system zone is UTC. MyXQL decodes TIMESTAMP values as UTC and
  #   has no client-side zone option, so any other session zone (and 'GMT',
  #   which is Europe/London's winter abbreviation, not UTC) would shift them;
  #   CURRENT_TIMESTAMP would also store local time into UTC columns.
  # - character_set_client, _connection and _results are utf8mb4, so text
  #   travels unchanged both ways.
  # - sql_mode has STRICT_TRANS_TABLES or STRICT_ALL_TABLES, so a value the
  #   column cannot hold is an error rather than a silently converted row.
  #
  # Anything else is refused before the statement runs, with an error naming
  # only the failing setting.

  #
  # The check also returns the session's time_zone and sql_mode (and
  # autocommit), the settings literal and TIMESTAMP interpretation depend on.
  # A stored function a domain view calls, or a row trigger of a write, can
  # SET them mid-statement and leave them changed (items 59, 62): every read,
  # every stream batch and every write statement compares them again after
  # it; a change restores the values seen before (session variables are not
  # transactional) and refuses with `session_state_changed`, discarding the
  # rows. A change made and undone inside one statement cannot be seen by
  # the compare; TIMESTAMP scope comparisons are zone-independent instead
  # (item 62a, `UNIX_TIMESTAMP(col)`).

  @query "SELECT @@session.time_zone, @@system_time_zone, " <>
           "@@session.character_set_client, @@session.character_set_connection, " <>
           "@@session.character_set_results, @@session.sql_mode, @@session.autocommit"

  @utc_zones ["UTC", "+00:00", "Etc/UTC"]

  @doc false
  # Run `fun` on one checked-out connection after the session check. `conn`
  # is a pool or connection pid, a registered name, or a checked-out
  # `DBConnection` (inside a transaction).
  def run(conn, fun, opts \\ []) do
    DBConnection.run(
      conn,
      fn checked_out ->
        with {:ok, before} <- state(checked_out, opts),
             {:ok, _} = result <- fun.(checked_out),
             :ok <- unchanged(checked_out, before, opts) do
          result
        end
      end,
      Keyword.take(opts, [:timeout, :queue_timeout, :queue_target, :queue_interval])
    )
  end

  @doc false
  def verify(conn, opts \\ []) do
    case state(conn, opts) do
      {:ok, _state} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc false
  # The checked session state: `%{time_zone, sql_mode, autocommit}`.
  def state(conn, opts \\ []) do
    case MyXQL.query(
           conn,
           SelectoDBMariaDB.Adapter.statement(@query),
           [],
           [query_type: :text] ++ Keyword.take(opts, [:timeout])
         ) do
      {:ok, %MyXQL.Result{rows: [[time_zone, _, _, _, _, sql_mode, autocommit] = row]}} ->
        with :ok <- check(Enum.take(row, 6)) do
          {:ok, %{time_zone: time_zone, sql_mode: sql_mode, autocommit: autocommit}}
        end

      {:ok, _result} ->
        refuse(:session)

      {:error, reason} ->
        {:error, SelectoDBMariaDB.Adapter.sanitize_error(reason)}
    end
  end

  @doc false
  # Compares the session with `before` after a statement; on a change the
  # values seen before are restored and the statement refused.
  def unchanged(conn, %{time_zone: time_zone, sql_mode: sql_mode}, opts \\ []) do
    case MyXQL.query(
           conn,
           "SELECT @@session.time_zone, @@session.sql_mode",
           [],
           [query_type: :text] ++ Keyword.take(opts, [:timeout])
         ) do
      {:ok, %MyXQL.Result{rows: [[^time_zone, ^sql_mode]]}} ->
        :ok

      _changed ->
        _ =
          MyXQL.query(conn, "SET SESSION time_zone = ?, SESSION sql_mode = ?", [
            time_zone,
            sql_mode
          ])

        changed()
    end
  end

  @doc false
  # A TEMPORARY table shadows a base table of the same name for this session
  # (a host's, or one a write's row trigger created), and MySQL lists no
  # temporary table in information_schema. A Selecto read refuses when any
  # relation its statement names after FROM or JOIN (parenthesised joins
  # included, R2) resolves to one
  # (SHOW CREATE TABLE answers for the temporary table first); a name that
  # does not resolve (a CTE, a derived table, a keyword) is no temporary
  # table.
  def refuse_temporary_shadows(conn, query, opts \\ []) do
    query
    |> relations()
    |> Enum.reduce_while(:ok, fn relation, :ok ->
      case MyXQL.query(
             conn,
             "SHOW CREATE TABLE " <> relation,
             [],
             [query_type: :text] ++ Keyword.take(opts, [:timeout])
           ) do
        {:ok, %MyXQL.Result{rows: [[_name, create | _]]}} when is_binary(create) ->
          if Regex.match?(~r/\A\s*CREATE\s+TEMPORARY\s+TABLE/i, create),
            do: {:halt, temporary_shadow()},
            else: {:cont, :ok}

        _other ->
          {:cont, :ok}
      end
    end)
  end

  @doc false
  def relations(query) do
    ~r/\b(?:from|join)\s*(?:\(\s*)*((?:`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\s*\.\s*(?:`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_$]*))?)/i
    |> Regex.scan(IO.iodata_to_binary(query), capture: :all_but_first)
    |> Enum.map(fn [relation] -> String.replace(relation, ~r/\s+/, "") end)
    |> Enum.uniq()
  end

  defp temporary_shadow,
    do:
      {:error,
       %Selecto.Error{
         type: :query_enforcement,
         message: "MariaDB read relation is shadowed by a TEMPORARY table",
         details: %{code: :query_rule_not_evaluable, reason: :temporary_shadow, adapter: :mariadb}
       }}

  @doc false
  def changed,
    do:
      {:error,
       %Selecto.Error{
         type: :query_enforcement,
         message: "MariaDB statement changed the session's time_zone or sql_mode",
         details: %{
           code: :query_rule_violation,
           reason: :session_state_changed,
           adapter: :mariadb
         }
       }}

  @doc false
  # The decision on one row of the session query; pure, for unit tests.
  def check([time_zone, system_time_zone, client, connection, results, sql_mode]) do
    cond do
      not utc?(time_zone, system_time_zone) -> refuse(:time_zone)
      not Enum.all?([client, connection, results], &(&1 == "utf8mb4")) -> refuse(:character_set)
      not strict?(sql_mode) -> refuse(:sql_mode)
      true -> :ok
    end
  end

  def check(_row), do: refuse(:session)

  defp utc?(zone, _system) when zone in @utc_zones, do: true
  defp utc?("SYSTEM", system) when system in @utc_zones, do: true
  defp utc?(_zone, _system), do: false

  defp strict?(sql_mode) when is_binary(sql_mode) do
    modes = String.split(sql_mode, ",")
    "STRICT_TRANS_TABLES" in modes or "STRICT_ALL_TABLES" in modes
  end

  defp strict?(_sql_mode), do: false

  defp refuse(setting) do
    {:error,
     Selecto.Error.connection_error("MariaDB session settings are not safe for Selecto", %{
       adapter: :mariadb,
       category: :invalid_session,
       setting: setting
     })}
  end
end
