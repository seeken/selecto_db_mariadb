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

  # Selecto's transaction endings. completion_type belongs to the host: at 1
  # a bare COMMIT or ROLLBACK opens a new transaction and at 2 it closes the
  # connection, so every ending Selecto sends names both clauses, on every
  # path. MyXQL's own commit and rollback callbacks send the bare statement
  # and are not used for transactions Selecto owns.
  @commit "COMMIT AND NO CHAIN NO RELEASE"
  @rollback "ROLLBACK AND NO CHAIN NO RELEASE"

  @lock_probe_relation "selecto_lock_probe_absent"

  # The errors that say a name is no table this session can reach: no such
  # table, database or default database, text that is no name, no privilege,
  # not locked. A read of such a name fails on its own. Nothing else says the
  # name is absent, a kill above all: a guard does not pass on its own failure.
  @absent [1146, 1049, 1046, 1064, 1142, 1100]
  @undo_attempts 3

  @doc false
  def commit_statement, do: @commit

  @doc false
  def rollback_statement, do: @rollback

  @doc false
  # Run `fun` on one checked-out connection after the session check. `conn`
  # is a pool or connection pid, a registered name, or a checked-out
  # `DBConnection` (inside a transaction). The statement runs as written.
  def run(conn, fun, opts \\ []) do
    checkout(
      conn,
      fn checked_out ->
        with {:ok, before} <- state(checked_out, opts),
             {:ok, _} = result <- fun.(checked_out),
             :ok <- unchanged(checked_out, before, opts) do
          result
        end
      end,
      opts
    )
  end

  @doc false
  # A catalog read Selecto issues for itself. At autocommit=0 it can open a
  # transaction on an idle connection; that snapshot is Selecto's, so it ends
  # here and the read that follows finds the connection as the host left it.
  def catalog(conn, fun, opts \\ []) do
    checkout(
      conn,
      fn checked_out ->
        with {:ok, before} <- state(checked_out, opts) do
          if DBConnection.status(checked_out) == :idle do
            ended(checked_out, fn -> checked(checked_out, before, fun, opts) end, opts)
          else
            checked(checked_out, before, fun, opts)
          end
        end
      end,
      opts
    )
  end

  @doc false
  # A Selecto read of `query` (R55): see guarded_read/5.
  def read(conn, query, fun, opts \\ []) do
    checkout(
      conn,
      fn checked_out ->
        with {:ok, before} <- state(checked_out, opts) do
          guarded_read(
            checked_out,
            query,
            before,
            fn -> checked(checked_out, before, fun, opts) end,
            opts
          )
        end
      end,
      opts
    )
  end

  defp checked(conn, before, fun, opts) do
    with {:ok, _} = result <- fun.(conn),
         :ok <- unchanged(conn, before, opts) do
      result
    end
  end

  @doc false
  # How a Selecto read runs on a checked-out connection.
  #
  # Inside a host transaction the read runs as written: no savepoint, no
  # anchor, no read-only mode (R66). A writing view reached that way is the
  # host's to undo. autocommit=0 is a host transaction from its first statement,
  # so a read there runs as written too: it opens what the host's own SELECT
  # would, and its snapshot and locks stay until the host ends them.
  #
  # On an idle connection, the read is its own START TRANSACTION READ ONLY,
  # ended by ROLLBACK AND NO CHAIN NO RELEASE, so a writing view reached by
  # the read refuses and nothing survives it. Read-only state lives only in
  # that transaction: there is no session setting to restore, and a failed
  # read cannot leave one behind. A row-locking read cannot be READ ONLY; it
  # gets a plain transaction with the same ending.
  #
  # START TRANSACTION releases LOCK TABLES and FLUSH TABLES t WITH READ LOCK,
  # so a read first asks whether the session holds them (host_locks/3). With
  # such locks the read runs as written and keeps them: the owned transaction
  # is not available there, and a writing view at autocommit=1 commits as it
  # would for the host's own SELECT. A global FLUSH TABLES WITH READ LOCK does
  # not show in the probe and survives START TRANSACTION.
  def guarded_read(conn, query, %{autocommit: autocommit}, work, opts \\ []) do
    if DBConnection.status(conn) == :idle and autocommit not in [0, "0"] do
      case host_locks(conn, relations(query), opts) do
        :none ->
          case transact(conn, read_start(query), fn -> {:rollback, work.()} end, opts) do
            {:error, %MyXQL.Error{} = error} -> {:error, sanitize(error)}
            {:error, %DBConnection.ConnectionError{} = error} -> {:error, sanitize(error)}
            other -> other
          end

        :held ->
          ended(conn, work, opts)

        {:error, reason} ->
          settle(conn, {:error, sanitize(reason)}, opts)
      end
    else
      work.()
    end
  end

  defp read_start(query) do
    if Regex.match?(~r/\bFOR\s+(?:UPDATE|SHARE)\b/i, IO.iodata_to_binary(query)),
      do: "START TRANSACTION",
      else: "START TRANSACTION READ ONLY"
  end

  @doc false
  # Whether this session holds LOCK TABLES or FLUSH TABLES t WITH READ LOCK:
  # `:held`, `:none`, or the error that ended the probe. Under such locks a
  # table not locked under this alias answers 1100. The probe is prepared
  # and never executed: MariaDB evaluates a DETERMINISTIC function of a view
  # at optimisation, so running SELECT 1 FROM view WHERE FALSE would commit
  # that function's writes. A relation that does not resolve says nothing,
  # so the next is asked, then a name that exists nowhere (1100 under locks,
  # 1146 otherwise).
  def host_locks(conn, relations, opts \\ []) do
    (relations ++ [@lock_probe_relation])
    |> Enum.reduce_while(:none, fn relation, :none ->
      case lock_probe(conn, relation, opts) do
        :continue -> {:cont, :none}
        :none -> {:halt, :none}
        other -> {:halt, other}
      end
    end)
  end

  defp lock_probe(conn, relation, opts) do
    statement = "SELECT 1 FROM #{relation} AS selecto_lock_probe WHERE FALSE"

    case MyXQL.prepare(conn, "", statement, Keyword.take(opts, [:timeout])) do
      {:ok, query} ->
        _ = MyXQL.close(conn, query, Keyword.take(opts, [:timeout]))
        :none

      {:error, %MyXQL.Error{mysql: %{code: 1100}}} ->
        :held

      {:error, %MyXQL.Error{mysql: %{code: code}}} when code in @absent ->
        :continue

      {:error, _} = error ->
        error
    end
  end

  @doc false
  # One transaction Selecto owns on an idle checked-out connection. `work`
  # returns `{:commit, value}` or `{:rollback, value}`; whatever happens, the
  # connection leaves here idle or closes (settle/3).
  def transact(conn, start, work, opts \\ []) do
    case text(conn, start, opts) do
      {:ok, _} ->
        case attempt(work) do
          {:commit, value} ->
            case text(conn, @commit, opts) do
              {:ok, _} -> value
              {:error, _} = error -> settle(conn, error, opts)
            end

          {:rollback, value} ->
            settle(conn, value, opts)

          {:selecto_session_raise, _, _, _} = raised ->
            settle(conn, raised, opts)
        end

      {:error, _} = error ->
        settle(conn, error, opts)
    end
  end

  # `work` as written, then the end of whatever transaction it opened on a
  # connection that was idle: only a snapshot of Selecto's own read can be in
  # it. ROLLBACK keeps table locks.
  defp ended(conn, work, opts), do: settle(conn, attempt(work), opts)

  defp attempt(work) do
    work.()
  catch
    kind, reason -> {:selecto_session_raise, kind, reason, __STACKTRACE__}
  end

  @doc false
  # Returns `value` once the connection is idle. A rollback can be killed like
  # any statement (MariaDB fails it with 1317, the transaction still open): it
  # is retried, then the connection closes, since a connection that may still
  # hold Selecto's transaction must not be reused. Nothing else runs in between:
  # Selecto never sets autocommit, so no later statement can commit what the
  # rollback did not undo. `value` is the original result in every
  # case. The read-only state a read carries lives in the transaction this
  # ends: START TRANSACTION READ ONLY rather than one-shot SET TRANSACTION,
  # which survives a failed read and a table-less SELECT.
  def settle(conn, value, opts \\ []) do
    if idle?(conn, opts, @undo_attempts), do: value, else: {:selecto_session_close, value}
  end

  defp idle?(conn, opts, attempts) do
    case DBConnection.status(conn) do
      :transaction ->
        case text(conn, @rollback, opts) do
          {:ok, _} -> DBConnection.status(conn) != :transaction
          {:error, %MyXQL.Error{}} when attempts > 1 -> idle?(conn, opts, attempts - 1)
          {:error, %MyXQL.Error{}} -> false
          {:error, _} -> DBConnection.status(conn) != :transaction
        end

      _idle_or_gone ->
        true
    end
  end

  @doc false
  # The undo of a savepoint that could not be returned to, inside a
  # transaction Selecto did not begin on this call. Under a DBConnection
  # transaction no statement is sent: the holder's own ending rolls back.
  # On a transaction Selecto owns, the owner's ending does.
  def fail_transaction(%DBConnection{conn_mode: :transaction, conn_ref: ref} = conn) do
    if Process.get({__MODULE__, :owned, ref}) do
      Process.put({__MODULE__, :owned, ref}, :failed)
    else
      _ = DBConnection.transaction(conn, &DBConnection.rollback(&1, :write_rolled_back))
    end

    :ok
  end

  def fail_transaction(conn) do
    case settle(conn, :ok, []) do
      {:selecto_session_close, :ok} -> close_held(conn)
      :ok -> :ok
    end
  end

  @doc false
  # `fun` gets a connection DBConnection treats as inside a transaction, so
  # DBConnection.rollback/2 and nested transaction/3 calls keep working
  # against a transaction whose ending Selecto sends.
  def owned_transaction(%DBConnection{conn_ref: ref} = conn, fun, opts \\ []) do
    tx = %{conn | conn_mode: :transaction}
    key = {__MODULE__, :owned, ref}
    Process.put(key, :open)

    try do
      transact(
        conn,
        "START TRANSACTION",
        fn ->
          result =
            try do
              fun.(tx)
            catch
              :throw, {DBConnection, ^ref, reason} -> {:error, reason}
            end

          case {Process.get(key), result} do
            {:failed, _} -> {:rollback, {:error, :rollback}}
            {_, {:error, _} = error} -> {:rollback, error}
            {_, {:ok, _} = ok} -> {:commit, ok}
            {_, other} -> {:commit, {:ok, other}}
          end
        end,
        opts
      )
    after
      Process.delete(key)
    end
  end

  @doc false
  def owned?(%DBConnection{conn_ref: ref}), do: Process.get({__MODULE__, :owned, ref}) != nil

  @doc false
  # One connection for `fun`, without DBConnection.run/3's status comparison:
  # that closes a connection whose status changed, and the first InnoDB access
  # at autocommit=0 changes it, taking the host's connection, its locks and
  # its variables. Leaving the callback by throw is the public way past the
  # comparison.
  #
  # The comparison is what closes a connection `fun` could not return to idle
  # (settle/3): returning normally lets it run, and the original result
  # comes back after it.
  def checkout(conn, fun, opts \\ [])

  # A connection the caller holds: it cannot go on holding what could not be
  # undone, so it closes under the caller, whose next statement says so.
  def checkout(%DBConnection{} = conn, fun, _opts) do
    case fun.(conn) do
      {:selecto_session_close, value} ->
        close_held(conn)
        finish(value)

      value ->
        finish(value)
    end
  end

  def checkout(conn, fun, opts) do
    ref = make_ref()

    try do
      DBConnection.run(
        conn,
        fn checked_out ->
          case fun.(checked_out) do
            {:selecto_session_close, value} ->
              Process.put(ref, {:closed, value})
              :closed

            value ->
              throw({ref, value})
          end
        end,
        Keyword.take(opts, [:timeout, :queue_timeout, :queue_target, :queue_interval])
      )
    catch
      :throw, {^ref, value} ->
        finish(value)

      :error, %DBConnection.ConnectionError{} = error ->
        case Process.get(ref) do
          {:closed, value} -> finish(value)
          nil -> reraise(error, __STACKTRACE__)
        end
    else
      :closed -> finish(elem(Process.get(ref), 1))
    after
      Process.delete(ref)
    end
  end

  defp close_held(%DBConnection{pool_ref: pool_ref}) do
    error = DBConnection.ConnectionError.exception("Selecto could not end its transaction")
    _ = DBConnection.Holder.disconnect(pool_ref, error)
    :ok
  end

  defp finish({:selecto_session_raise, kind, reason, stack}),
    do: :erlang.raise(kind, reason, stack)

  defp finish(value), do: value

  defp text(conn, statement, opts) do
    MyXQL.query(conn, statement, [], [query_type: :text] ++ Keyword.take(opts, [:timeout]))
  end

  defp sanitize(reason), do: SelectoDBMariaDB.Adapter.sanitize_error(reason)

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

      # The check itself failed (a kill, a lost connection): nothing says the
      # session changed, and the failure is what the caller is told.
      {:error, reason} ->
        {:error, sanitize(reason)}

      {:ok, _changed} ->
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

        {:error, %MyXQL.Error{mysql: %{code: code}}} when code in @absent ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, sanitize(reason)}}

        {:ok, _unreadable} ->
          {:halt, temporary_shadow()}
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
