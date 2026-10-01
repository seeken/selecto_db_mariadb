# selecto_updato is not a dependency of this adapter, and its
# SelectoUpdato.GovernedWrite is the only module Selecto.Write.Authorization
# lets issue an authorization. This stand-in of the same name, compiled only
# for this suite, plays the governed entry point so the adapter's governed
# callbacks can be exercised with a real authorization.
defmodule SelectoUpdato.GovernedWrite do
  @moduledoc false

  alias Selecto.Write.Authorization

  # Matching the result keeps issue/1 out of tail position, so this module's
  # frame is on the stack when Authorization checks its caller.
  def authorize!(write) do
    {:ok, authorization} = Authorization.issue(write)
    authorization
  end

  def prepared(write, context) do
    with {:ok, authorization} <- Authorization.issue(write) do
      {:ok, write, context, authorization}
    end
  end
end

defmodule SelectoDBMariaDB.GovernedWriteBoundaryIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Authorization, Batch, Command, Error, Result}
  alias SelectoDBMariaDB.Adapter, as: Adapter
  alias SelectoUpdato.GovernedWrite

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  @seed [[1, 7, "a"], [2, 7, "b"], [3, 8, "c"]]

  setup do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "selecto_gw_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    driver!(admin, "CREATE DATABASE `#{database}`")
    Adapter.disconnect(admin)

    {:ok, connection} = Adapter.connect(Keyword.put(connection_options(), :database, database))

    driver!(connection, """
    CREATE TABLE `selecto_governed_writes` (
      `id` BIGINT PRIMARY KEY,
      `tenant_id` BIGINT NOT NULL,
      `name` VARCHAR(255) NOT NULL
    ) ENGINE=InnoDB
    """)

    driver!(
      connection,
      "INSERT INTO `selecto_governed_writes` VALUES (1, 7, 'a'), (2, 7, 'b'), (3, 8, 'c')"
    )

    on_exit(fn ->
      {:ok, admin} = Adapter.connect(connection_options())
      driver!(admin, "DROP DATABASE IF EXISTS `#{database}`")
      Adapter.disconnect(admin)
    end)

    %{connection: connection, selecto: %Selecto{adapter: Adapter, connection: connection}}
  end

  test "the normal execute API refuses a raw command, batch, and graph", %{
    connection: connection,
    selecto: selecto
  } do
    for write <- [update(1, "direct"), batch!()] ++ [graph!()] do
      assert {:error, %Error{type: :ungoverned_write}} =
               Adapter.execute_write(connection, write, [])

      assert {:error, %Error{type: :ungoverned_write}} = Selecto.Write.execute(selecto, write)
    end

    assert rows(connection) == @seed
  end

  test "a lookalike, spent, or mismatched authorization is refused", %{connection: connection} do
    command = update(1, "forged")
    forged = struct!(Authorization, ref: make_ref())

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, command, authorization: forged)

    authorization = GovernedWrite.authorize!(command)

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, update(1, "other"), authorization: authorization)

    authorization = GovernedWrite.authorize!(command)

    assert {:ok, %Result{affected_rows: 1}} =
             Adapter.execute_write(connection, command, authorization: authorization)

    execute!(connection, "UPDATE `selecto_governed_writes` SET `name` = 'a' WHERE id = 1")

    assert {:error, %Error{type: :ungoverned_write}} =
             Adapter.execute_write(connection, command, authorization: authorization)

    assert rows(connection) == @seed
  end

  test "a governed command, batch and command execute", %{
    connection: connection,
    selecto: selecto
  } do
    for write <- [update(2, "governed"), batch!()] do
      authorization = GovernedWrite.authorize!(write)

      assert {:ok, _result} =
               Selecto.Write.execute(selecto, write, authorization: authorization)
    end

    assert rows(connection) == [
             [1, 7, "batch"],
             [2, 7, "governed"],
             [3, 8, "c"],
             [4, 7, "batch"]
           ]
  end

  test "the unsafe primitive still executes for trusted tooling", %{connection: connection} do
    assert {:ok, %Result{affected_rows: 1}} =
             Adapter.execute_write_unsafe(connection, update(1, "unsafe"), [])

    assert rows(connection) == [[1, 7, "unsafe"], [2, 7, "b"], [3, 8, "c"]]
  end

  defp update(id, name) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :selecto_governed_writes,
        assignments: [%{field: :name, value: {:literal, name}}],
        predicate: {:eq, {:field, :id}, {:literal, id}}
      })

    command
  end

  defp insert(id, name) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :selecto_governed_writes,
        assignments: [
          %{field: :id, value: {:literal, id}},
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :name, value: {:literal, name}}
        ]
      })

    command
  end

  defp batch! do
    {:ok, batch} = Batch.new([update(1, "batch"), insert(4, "batch")])
    batch
  end

  # This adapter executes no write graphs; a raw graph is still refused as
  # ungoverned before the adapter considers it.
  defp graph! do
    %Selecto.Write.Graph{
      root: {"root", "root"},
      nodes: [
        %Selecto.Write.Graph.Node{
          id: "root",
          path: [],
          relation: :selecto_governed_writes,
          strategy: :ordered,
          rows: [%Selecto.Write.Graph.Row{id: "root", path: [], command: insert(5, "graph")}]
        }
      ]
    }
  end

  defp rows(connection) do
    {:ok, %{rows: rows}} =
      Adapter.execute(
        connection,
        "SELECT `id`, `tenant_id`, `name` FROM `selecto_governed_writes` ORDER BY `id`",
        [],
        []
      )

    rows
  end

  defp execute!(connection, sql), do: driver!(connection, sql)

  # Test-owned DDL and resets use the driver; the adapter runs prepared writes.
  defp driver!(connection, sql) do
    {:ok, result} = MyXQL.query(connection, sql, [], query_type: :text)
    result
  end

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
