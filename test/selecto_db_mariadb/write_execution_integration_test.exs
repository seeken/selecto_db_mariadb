defmodule SelectoDBMariaDB.WriteExecutionIntegrationTest do
  use ExUnit.Case, async: false

  alias Selecto.Write.{Batch, Command, Error, Result}
  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  test "executes guarded flat writes, native upserts, and atomic batches on MariaDB" do
    with_fixture(fn fixture ->
      capabilities = Adapter.write_capabilities(fixture.conn)
      assert capabilities.server_version =~ "MariaDB"
      assert capabilities.upsert == :single_declared_conflict_target

      assert {:ok, tables} = Adapter.list_tables(fixture.conn, schema: fixture.database)
      assert Enum.sort(tables) == ["items", "tenants"]

      assert {:ok, relations} =
               Adapter.list_relations(fixture.conn,
                 schema: fixture.database,
                 include_views: true
               )

      assert Enum.any?(relations, &(&1 == %{name: "items", source_kind: :table}))

      assert {:ok, metadata} =
               Adapter.introspect_table(fixture.conn, "items",
                 schema: fixture.database,
                 expand: false
               )

      assert metadata.primary_key == :id
      assert metadata.field_types.tenant_id == :integer
      assert metadata.source == :mariadb

      assert {:ok, %{rows: rollup_rows}} =
               Adapter.execute(
                 fixture.conn,
                 "SELECT `id`, COUNT(*) FROM `tenants` GROUP BY `id` WITH ROLLUP",
                 [],
                 query_type: :text
               )

      assert length(rollup_rows) == 3

      assert {:ok, %Result{operation: :insert, affected_rows: 1}} =
               Adapter.execute_write(
                 fixture.conn,
                 command!(:insert,
                   assignments: [
                     assignment(:tenant_id, 45),
                     assignment(:external_id, "flat-1"),
                     assignment(:name, "created")
                   ]
                 ),
                 []
               )

      item_id =
        scalar!(fixture.conn, "SELECT `id` FROM `items` WHERE `external_id` = ?", ["flat-1"])

      tenant_predicate =
        {:and,
         [
           {:eq, {:field, :id}, {:literal, item_id}},
           {:eq, {:field, :tenant_id}, {:context, :tenant_id}}
         ]}

      assert {:ok, %Result{affected_rows: 1}} =
               Adapter.execute_write(
                 fixture.conn,
                 command!(:update,
                   assignments: [assignment(:name, "tenant-updated")],
                   predicate: tenant_predicate
                 ),
                 context: %{tenant_id: 45}
               )

      assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
               Adapter.execute_write(
                 fixture.conn,
                 command!(:update,
                   assignments: [assignment(:name, "cross-tenant")],
                   predicate: tenant_predicate
                 ),
                 context: %{tenant_id: 99}
               )

      assert scalar!(fixture.conn, "SELECT `name` FROM `items` WHERE `id` = ?", [item_id]) ==
               "tenant-updated"

      upsert =
        command!(:upsert,
          assignments: [
            assignment(:tenant_id, 45),
            assignment(:external_id, "upsert-1"),
            assignment(:name, "upsert-created")
          ],
          metadata: %{
            conflict_target: [:tenant_id, :external_id],
            declared_conflict_targets: [[:tenant_id, :external_id]],
            upsert_update_fields: [:name]
          }
        )

      assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(fixture.conn, upsert, [])

      changed =
        update_in(upsert.assignments, fn assignments ->
          Enum.map(assignments, fn
            %{field: :name} = assignment -> %{assignment | value: {:literal, "upsert-updated"}}
            assignment -> assignment
          end)
        end)

      assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(fixture.conn, changed, [])
      assert {:ok, %Result{affected_rows: 1}} = Adapter.execute_write(fixture.conn, changed, [])

      assert scalar!(fixture.conn, "SELECT `name` FROM `items` WHERE `external_id` = ?", [
               "upsert-1"
             ]) ==
               "upsert-updated"

      assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
               Adapter.execute_write(
                 fixture.conn,
                 command!(:insert,
                   assignments: [
                     assignment(:tenant_id, 999),
                     assignment(:external_id, "guarded-out"),
                     assignment(:name, "must-not-exist")
                   ],
                   metadata: %{
                     foreign_key_guards: [
                       %{field: :tenant_id, relation: :tenants, target_field: :id}
                     ]
                   }
                 ),
                 []
               )

      first =
        command!(:insert,
          assignments: [
            assignment(:tenant_id, 45),
            assignment(:external_id, "batch-first"),
            assignment(:name, "first")
          ]
        )

      fails_cardinality =
        command!(:update,
          assignments: [assignment(:name, "never")],
          predicate: {:eq, {:field, :external_id}, {:literal, "missing"}}
        )

      {:ok, batch} = Batch.new([first, fails_cardinality])

      assert {:error, %Error{type: :cardinality_mismatch}} =
               Adapter.execute_write(fixture.conn, batch, [])

      assert scalar!(fixture.conn, "SELECT COUNT(*) FROM `items` WHERE `external_id` = ?", [
               "batch-first"
             ]) ==
               0

      assert {:ok, %Result{operation: :delete, affected_rows: 1}} =
               Adapter.execute_write(
                 fixture.conn,
                 command!(:delete,
                   assignments: [],
                   predicate:
                     {:and,
                      [
                        {:eq, {:field, :id}, {:literal, item_id}},
                        {:eq, {:field, :tenant_id}, {:literal, 45}}
                      ]}
                 ),
                 []
               )
    end)
  end

  defp with_fixture(fun) do
    {:ok, conn} = Adapter.connect(connection_options())

    database =
      "selecto_mariadb_write_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    execute!(conn, "CREATE DATABASE `#{database}`")

    try do
      execute!(conn, "USE `#{database}`")
      execute!(conn, "CREATE TABLE `tenants` (`id` int NOT NULL PRIMARY KEY) ENGINE=InnoDB")

      execute!(conn, """
      CREATE TABLE `items` (
        `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `tenant_id` int NOT NULL,
        `external_id` varchar(80) NOT NULL,
        `name` varchar(120) NOT NULL,
        UNIQUE KEY `uq_items_tenant_external` (`tenant_id`, `external_id`),
        CONSTRAINT `fk_items_tenant` FOREIGN KEY (`tenant_id`) REFERENCES `tenants` (`id`)
      ) ENGINE=InnoDB
      """)

      execute!(conn, "INSERT INTO `tenants` (`id`) VALUES (45), (99)")
      fun.(%{conn: conn, database: database})
    after
      Adapter.execute(conn, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
      if Process.alive?(conn), do: GenServer.stop(conn)
    end
  end

  defp command!(operation, overrides) do
    defaults = %{
      operation: operation,
      relation: :items,
      assignments: [],
      predicate: nil,
      expected_cardinality: {:exactly, 1},
      returning: :none,
      metadata: %{}
    }

    {:ok, command} = Command.new(Map.merge(defaults, Map.new(overrides)))
    command
  end

  defp assignment(field, value), do: %{field: field, value: {:literal, value}}

  defp execute!(conn, sql) do
    assert {:ok, _result} = Adapter.execute(conn, sql, [], query_type: :text)
  end

  defp scalar!(conn, sql, params) do
    assert {:ok, %{rows: [[value]]}} = Adapter.execute(conn, sql, params, [])
    value
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
