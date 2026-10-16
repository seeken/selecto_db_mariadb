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

  @query "SELECT @@session.time_zone, @@system_time_zone, " <>
           "@@session.character_set_client, @@session.character_set_connection, " <>
           "@@session.character_set_results, @@session.sql_mode"

  @utc_zones ["UTC", "+00:00", "Etc/UTC"]

  @doc false
  # Run `fun` on one checked-out connection after the session check. `conn`
  # is a pool or connection pid, a registered name, or a checked-out
  # `DBConnection` (inside a transaction).
  def run(conn, fun, opts \\ []) do
    DBConnection.run(
      conn,
      fn checked_out ->
        case verify(checked_out, opts) do
          :ok -> fun.(checked_out)
          {:error, _} = error -> error
        end
      end,
      Keyword.take(opts, [:timeout, :queue_timeout, :queue_target, :queue_interval])
    )
  end

  @doc false
  def verify(conn, opts \\ []) do
    case MyXQL.query(
           conn,
           SelectoDBMariaDB.Adapter.statement(@query),
           [],
           [query_type: :text] ++ Keyword.take(opts, [:timeout])
         ) do
      {:ok, %MyXQL.Result{rows: [row]}} -> check(row)
      {:ok, _result} -> refuse(:session)
      {:error, reason} -> {:error, SelectoDBMariaDB.Adapter.sanitize_error(reason)}
    end
  end

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
