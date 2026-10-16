defmodule SelectoDBMariaDB.RowStream do
  @moduledoc false

  # A server-side read-only cursor (COM_STMT_EXECUTE with CURSOR_TYPE_READ_ONLY,
  # then COM_STMT_FETCH) streamed by a producer process.
  #
  # Ownership: the producer checks the connection out with `DBConnection.run/3`
  # and never issues BEGIN, COMMIT, ROLLBACK or SAVEPOINT. While a host holds
  # the connection (for example inside its own transaction) the producer waits
  # in the pool queue and fails on the queue timeout; it never interleaves with,
  # ends or rolls back host work. Each chunk waits for the consumer's
  # acknowledgement, so at most one fetch is buffered. Closing the stream (or
  # the consumer exiting) cancels the producer, which deallocates the cursor
  # and checks the connection back in.
  #
  # The columns are reported before enumeration, so opening the stream runs the
  # cursor's first fetch. A stream that is opened but never enumerated releases
  # its cursor and connection after `:receive_timeout`.

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
          DBConnection.run(
            conn,
            fn checked_out ->
              # The session check runs on the connection the cursor then uses.
              case SelectoDBMariaDB.Session.verify(checked_out) do
                :ok ->
                  checked_out
                  |> MyXQL.stream(statement, params, max_rows: max_rows, timeout: stream_timeout)
                  |> Enum.reduce_while(:ok, fn %MyXQL.Result{rows: rows, columns: columns}, :ok ->
                    # A stored value outside the supported range ends the
                    # stream with a typed error (SelectoDBMariaDB.StoredValues).
                    case SelectoDBMariaDB.StoredValues.check(rows) do
                      :ok ->
                        send_chunk.(rows || [], columns || [])
                        {:cont, :ok}

                      {:error, error} ->
                        {:halt, {:producer_failed, error}}
                    end
                  end)

                {:error, error} ->
                  {:producer_failed, error}
              end
            end,
            timeout: stream_timeout
          )
        end)

      parent = self()
      ref = make_ref()

      task =
        Selecto.TaskSupervisor.async(fn ->
          run_producer(parent, ref, producer, receive_timeout)
        end)

      state = %{task: task, ref: ref, monitor_ref: task.ref}

      case first_message(state, receive_timeout) do
        {:chunk, rows, columns} ->
          {:ok, resource(state, rows, receive_timeout, queue_timeout), columns}

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

  defp resource(state, first_rows, receive_timeout, queue_timeout) do
    Stream.resource(
      fn -> {state, first_rows} end,
      fn
        {state, rows} when is_list(rows) ->
          {rows, {state, nil}}

        {state, nil} ->
          acknowledge(state)
          next_chunk(state, receive_timeout)
      end,
      fn {state, _pending} -> stop(state, queue_timeout) end
    )
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
        raise Selecto.Error.to_exception(
                Selecto.Error.timeout_error("MariaDB stream timed out waiting for rows")
              )
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
        exception ->
          {:producer_failed, SelectoDBMariaDB.Adapter.normalize_error(exception)}
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

    # DBConnection.run/3 returns the producer's value or raises, so any other
    # result is a fully consumed cursor.
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
    # immediate kill would make DBConnection drop the connection.
    _ = Task.yield(state.task, queue_timeout) || Task.shutdown(state.task, :brutal_kill)
    Process.demonitor(state.monitor_ref, [:flush])
    drain(state.ref)
  end

  defp drain(ref) do
    receive do
      {^ref, _message} -> drain(ref)
    after
      0 -> :ok
    end
  end

  defp raise_error(%Selecto.Error{} = error), do: raise(Selecto.Error.to_exception(error))

  defp raise_error(reason),
    do: raise(Selecto.Error.to_exception(SelectoDBMariaDB.Adapter.normalize_error(reason)))
end
