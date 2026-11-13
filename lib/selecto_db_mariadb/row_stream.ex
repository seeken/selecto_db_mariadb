defmodule SelectoDBMariaDB.RowStream do
  @moduledoc false

  # A server-side read-only cursor (COM_STMT_EXECUTE with CURSOR_TYPE_READ_ONLY,
  # then COM_STMT_FETCH) streamed by a producer process.
  #
  # Ownership: the producer checks the connection out (Session.checkout/3). A
  # Selecto read's cursor runs as Session.guarded_read/5 says: on an idle
  # connection inside a read-only transaction the producer owns and ends,
  # inside a host transaction as written. While a host holds the connection
  # the producer waits in the pool queue and fails on the queue timeout; it
  # never interleaves with, ends or rolls back host work. Each chunk waits for
  # the consumer's acknowledgement, so at most one fetch is buffered. Closing
  # the stream (or the consumer exiting) cancels the producer, which
  # deallocates the cursor, ends its transaction and checks the connection in.
  #
  # The columns are reported before enumeration, so opening the stream runs the
  # cursor's first fetch. A stream that is opened but never enumerated releases
  # its cursor and connection after `:receive_timeout`.

  alias SelectoDBMariaDB.{Session, StoredValues}

  @default_max_rows 500

  def open(conn, statement, params, opts) do
    max_rows = Keyword.get(opts, :max_rows, @default_max_rows)
    stream_timeout = Keyword.get(opts, :stream_timeout, 30_000)
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    queue_timeout = Keyword.get(opts, :queue_timeout, 100)

    with :ok <- positive_integer(:max_rows, max_rows),
         :ok <- positive_integer(:stream_timeout, stream_timeout),
         :ok <- positive_integer(:receive_timeout, receive_timeout) do
      producer =
        Keyword.get(opts, :stream_producer, fn send_chunk ->
          Session.checkout(
            conn,
            fn checked_out ->
              # The session is checked on the connection the cursor then
              # uses, before the cursor opens; a Selecto read runs as
              # Session.guarded_read/5 says. Text wholly the host's has only
              # its encoding checked (R71).
              host? =
                Keyword.get(opts, :selecto_session_guard) == true and Session.host_text?(opts)

              with {:ok, session} <-
                     if(host?,
                       do: Session.host_state(checked_out),
                       else: Session.state(checked_out)
                     ) do
                cursor = fn ->
                  checked_out
                  |> MyXQL.stream(statement, params,
                    max_rows: max_rows,
                    timeout: stream_timeout
                  )
                  |> Enum.reduce_while(:ok, fn %MyXQL.Result{} = result, :ok ->
                    %{rows: rows, columns: columns, num_warnings: warnings} = result

                    # A stored value outside the supported range ends the
                    # stream with a typed error (StoredValues). The cursor's
                    # first result carries what the statement warned of.
                    with :ok <- Session.not_cut(checked_out, warnings, opts),
                         :ok <- StoredValues.check(rows) do
                      send_chunk.(rows || [], columns || [])
                      {:cont, :ok}
                    else
                      {:error, error} -> {:halt, {:producer_failed, error}}
                    end
                  end)
                end

                if Keyword.get(opts, :selecto_session_guard) == true,
                  do:
                    Session.guarded_read(
                      checked_out,
                      statement,
                      session,
                      cursor,
                      Keyword.take(opts, [
                        :selecto_generated,
                        :selecto_raw_sql,
                        :selecto_statement
                      ])
                    ),
                  else: cursor.()
              end
              |> case do
                {:error, error} -> {:producer_failed, error}
                other -> other
              end
            end,
            timeout: stream_timeout
          )
        end)

      # The cursor opens here, so a failure before the first rows returns
      # typed and the columns are known. Its first rows serve the opening
      # process's first enumeration; any other enumeration (another process,
      # a second pass, one after a halt) runs the statement again with a
      # producer of its own (resource/5).
      state = start_producer(producer, receive_timeout)

      case first_message(state, receive_timeout) do
        {:chunk, rows, columns} ->
          {:ok, resource(state, rows, producer, receive_timeout, queue_timeout), columns}

        {:done, columns} ->
          stop(state, queue_timeout)
          {:ok, [], columns}

        {:error, reason} ->
          stop(state, queue_timeout)
          {:error, reason}
      end
    end
  end

  defp positive_integer(_name, value) when is_integer(value) and value > 0, do: :ok
  defp positive_integer(name, value), do: {:error, {:invalid_stream_option, name, value}}

  # MyXQL returns the cursor's column definitions with its first result, which
  # can hold zero rows; skip empty leading chunks until rows or the end arrive.
  defp first_message(state, receive_timeout, columns \\ nil) do
    ref = state.ref
    monitor_ref = state.monitor_ref

    receive do
      {^ref, {:chunk, [], chunk_columns}} ->
        acknowledge(state)
        first_message(state, receive_timeout, columns || chunk_columns)

      {^ref, {:chunk, rows, chunk_columns}} ->
        {:chunk, rows, columns || chunk_columns}

      {^ref, :done} ->
        {:done, columns || []}

      {^ref, {:producer_failed, reason}} ->
        {:error, reason}

      {^monitor_ref, _reply} ->
        {:error, :stream_producer_ended}

      {:DOWN, ^monitor_ref, :process, _pid, reason} ->
        {:error, {:stream_producer_exit, reason}}
    after
      receive_timeout -> {:error, :stream_open_timeout}
    end
  end

  defp start_producer(producer, receive_timeout) do
    parent = self()
    ref = make_ref()

    task =
      Selecto.TaskSupervisor.async(fn ->
        run_producer(parent, ref, producer, receive_timeout)
      end)

    %{task: task, ref: ref, monitor_ref: task.ref, owner: parent}
  end

  defp resource(state, first_rows, producer, receive_timeout, queue_timeout) do
    Stream.resource(
      fn -> enumeration(state, first_rows, producer, receive_timeout, queue_timeout) end,
      fn
        {state, :done} ->
          {:halt, {state, nil}}

        {state, rows} when is_list(rows) ->
          {rows, {state, nil}}

        {state, nil} ->
          acknowledge(state)
          next_chunk(state, receive_timeout)
      end,
      fn {state, _pending} -> stop(state, queue_timeout) end
    )
  end

  # The opening process's first enumeration takes the cursor already open;
  # every other one opens its own, owned by the enumerating process.
  defp enumeration(state, first_rows, producer, receive_timeout, queue_timeout) do
    if self() == state.owner and claim(state) do
      {state, first_rows}
    else
      # The opening cursor, still unclaimed, holds the connection: it closes
      # first, so this enumeration's own cursor can check the connection out.
      retire_unclaimed(state, receive_timeout)
      fresh = start_producer(producer, receive_timeout)

      case first_message(fresh, receive_timeout) do
        {:chunk, rows, _columns} ->
          {fresh, rows}

        {:done, _columns} ->
          {fresh, :done}

        {:error, reason} ->
          stop(fresh, queue_timeout)
          raise_error(reason)
      end
    end
  end

  defp retire_unclaimed(%{task: task, ref: ref}, receive_timeout) do
    monitor = Process.monitor(task.pid)
    send(task.pid, {ref, :retire_unclaimed, self()})

    receive do
      {^ref, :retired, true} ->
        receive do
          {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
        after
          receive_timeout -> Process.demonitor(monitor, [:flush])
        end

      {^ref, :retired, false} ->
        Process.demonitor(monitor, [:flush])

      {:DOWN, ^monitor, :process, _pid, _reason} ->
        :ok
    end
  end

  # The open cursor answers one claim; a finished or cancelled one, none.
  defp claim(%{task: task, ref: ref}) do
    monitor = Process.monitor(task.pid)
    send(task.pid, {ref, :claim, self()})

    receive do
      {^ref, :claimed, granted?} ->
        Process.demonitor(monitor, [:flush])
        granted?

      {:DOWN, ^monitor, :process, _pid, _reason} ->
        false
    end
  end

  defp next_chunk(state, receive_timeout) do
    ref = state.ref
    monitor_ref = state.monitor_ref

    receive do
      {^ref, {:chunk, rows, _columns}} ->
        {rows, {state, nil}}

      {^ref, :done} ->
        {:halt, {state, nil}}

      {^ref, {:producer_failed, reason}} ->
        raise_error(reason)

      # The producer reports :done or a failure before it ends, so an exit
      # here is never a complete stream.
      {^monitor_ref, _reply} ->
        raise_error(:stream_producer_ended)

      {:DOWN, ^monitor_ref, :process, _pid, reason} ->
        raise_error({:stream_producer_exit, reason})
    after
      receive_timeout ->
        raise Selecto.Error.Refused,
              Selecto.Error.timeout_error("MariaDB stream timed out waiting for rows")
    end
  end

  defp run_producer(parent, ref, producer, idle_timeout) do
    parent_monitor_ref = Process.monitor(parent)

    result =
      try do
        producer.(fn rows, columns ->
          send(parent, {ref, {:chunk, rows, columns}})
          await_ack(parent, parent_monitor_ref, ref, idle_timeout)
        end)
      rescue
        # The failure as the non-stream path returns it, normalized where it
        # is returned or raised (Selecto.Error.stream_failure/2): the server's
        # error and a lost connection come back sanitized there, anything else
        # (a parameter MyXQL cannot encode) is raised there.
        error in [MyXQL.Error, DBConnection.ConnectionError] ->
          {:producer_failed, SelectoDBMariaDB.Adapter.normalize_error(error)}

        exception ->
          {:producer_failed, {:adapter_raised, exception}}
      catch
        :throw, {:selecto_stream_cancelled, ^ref} ->
          {:consumer_cancelled, ref}

        # A consumer that resumes after the idle limit gets an error, never a
        # silently truncated stream.
        :throw, {:selecto_stream_idle, ^ref} ->
          {:producer_failed, Selecto.Error.timeout_error("MariaDB stream was idle too long")}

        kind, reason ->
          {:producer_failed, {kind, reason}}
      after
        Process.demonitor(parent_monitor_ref, [:flush])
      end

    # The producer returns its value or raises, so any other result is a fully
    # consumed cursor.
    case result do
      {:consumer_cancelled, ^ref} -> :ok
      {:producer_failed, reason} -> send(parent, {ref, {:producer_failed, reason}})
      _complete -> send(parent, {ref, :done})
    end
  end

  # An abandoned stream must not hold the cursor and connection forever.
  defp await_ack(parent, parent_monitor_ref, ref, idle_timeout) do
    receive do
      {^ref, :ack} ->
        :ok

      # The open cursor's first rows go to one enumeration of the opening
      # process (resource/5); a later claim is refused.
      {^ref, :claim, from} ->
        send(from, {ref, :claimed, from == parent and Process.put({ref, :claimed}, true) == nil})
        await_ack(parent, parent_monitor_ref, ref, idle_timeout)

      # Another enumeration needs the connection: an unclaimed cursor closes.
      {^ref, :retire_unclaimed, from} ->
        if Process.get({ref, :claimed}) do
          send(from, {ref, :retired, false})
          await_ack(parent, parent_monitor_ref, ref, idle_timeout)
        else
          send(from, {ref, :retired, true})
          throw({:selecto_stream_cancelled, ref})
        end

      {^ref, :cancel} ->
        throw({:selecto_stream_cancelled, ref})

      {:DOWN, ^parent_monitor_ref, :process, ^parent, _} ->
        throw({:selecto_stream_cancelled, ref})
    after
      idle_timeout -> throw({:selecto_stream_idle, ref})
    end
  end

  defp acknowledge(%{task: task, ref: ref}), do: send(task.pid, {ref, :ack})

  defp stop(state, queue_timeout) do
    send(state.task.pid, {state.ref, :cancel})

    # Let the producer deallocate its cursor and check the connection in; an
    # immediate kill would make DBConnection drop the connection. Only the
    # owner may yield a task; any other process waits for it to end.
    if self() == state.owner do
      _ = Task.yield(state.task, queue_timeout) || Task.shutdown(state.task, :brutal_kill)
      Process.demonitor(state.monitor_ref, [:flush])
      drain(state.ref)
    else
      monitor = Process.monitor(state.task.pid)

      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      after
        queue_timeout ->
          Process.exit(state.task.pid, :kill)
          Process.demonitor(monitor, [:flush])
      end
    end

    :ok
  end

  defp drain(ref) do
    receive do
      {^ref, _message} -> drain(ref)
    after
      0 -> :ok
    end
  end

  # Raised while the stream is enumerated: the Selecto.Error the non-stream
  # path returns for the same failure, as Selecto.Error.Refused.
  defp raise_error(reason),
    do: Selecto.Error.raise_stream_failure!(SelectoDBMariaDB.Adapter, reason)
end
