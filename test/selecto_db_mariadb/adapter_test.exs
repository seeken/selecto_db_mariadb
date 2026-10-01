defmodule SelectoDBMariaDB.AdapterTest do
  use ExUnit.Case, async: true

  alias Selecto.Write.{Batch, Command, Error, Preview}

  test "adapter exposes the selecto adapter contract" do
    assert Code.ensure_loaded?(SelectoDBMariaDB.Adapter)
    assert function_exported?(SelectoDBMariaDB.Adapter, :name, 0)
    assert function_exported?(SelectoDBMariaDB.Adapter, :connect, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :execute, 4)
    assert function_exported?(SelectoDBMariaDB.Adapter, :placeholder, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :quote_identifier, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :supports?, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :transaction, 3)
  end

  test "mariadb adapter reports expected placeholder and quoting strategy" do
    assert SelectoDBMariaDB.Adapter.placeholder(3) == "?"
    assert SelectoDBMariaDB.Adapter.quote_identifier("order") == "`order`"
  end

  test "normalizes command results whose driver columns are nil" do
    assert SelectoDBMariaDB.Adapter.normalize_result(%{
             rows: nil,
             columns: nil,
             num_rows: 0,
             last_insert_id: nil
           }) == %{
             rows: [],
             columns: [],
             num_rows: 0,
             metadata: %{last_insert_id: nil}
           }
  end

  test "advertises the versioned flat-write contract without overclaiming returning or graphs" do
    capabilities =
      SelectoDBMariaDB.Adapter.write_capabilities(
        stub_connection(fn _, _, _ ->
          {:ok, %{rows: [["11.4.5-MariaDB"]], columns: ["VERSION()"]}}
        end)
      )

    assert capabilities.protocol_version == Selecto.Write.Capabilities.protocol_version()
    assert capabilities.insert
    assert capabilities.update
    assert capabilities.upsert
    assert capabilities.delete
    assert capabilities.atomic_batch
    refute capabilities.returning
    refute capabilities.generated_keys
    refute capabilities.write_graph
    refute capabilities.prepared_candidate_state
    assert capabilities.server_version == "11.4.5-MariaDB"
    assert SelectoDBMariaDB.Adapter.write_capabilities(:unused).server_version == nil
  end

  test "previews parameterized guarded writes and native upsert syntax" do
    insert =
      command!(:insert,
        assignments: [
          %{field: :tenant_id, value: {:context, :tenant_id}},
          %{field: :name, value: {:literal, "updated"}}
        ],
        metadata: %{
          foreign_key_guards: [
            %{field: :tenant_id, relation: :tenants, target_field: :id}
          ]
        }
      )

    assert {:ok, %Preview{statements: [%{text: insert_sql, params: [45, "updated", 45]}]}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, insert, context: %{tenant_id: 45})

    assert insert_sql =~ "INSERT INTO `items`"
    assert insert_sql =~ "EXISTS (SELECT 1 FROM `tenants` WHERE `id` = ?)"

    upsert =
      command!(:upsert,
        assignments: [
          %{field: :id, value: {:literal, 7}},
          %{field: :name, value: {:literal, "updated"}}
        ],
        metadata: %{
          conflict_target: [:id],
          declared_conflict_targets: [[:id]],
          upsert_update_fields: [:name]
        }
      )

    assert {:ok, %Preview{statements: [%{text: upsert_sql, params: [7, "updated"]}]}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, upsert, [])

    assert upsert_sql =~ "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"

    ambiguous = put_in(upsert.metadata.declared_conflict_targets, [[:id], [:external_id]])

    assert {:error, %Error{type: :write_capability_missing}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, ambiguous, [])
  end

  test "preflight rejects returning and graph semantics the adapter cannot preserve" do
    selecto = %Selecto{adapter: SelectoDBMariaDB.Adapter, connection: :unused}
    returning = %{command!(:insert) | returning: [:id]}

    assert {:error, %Error{type: :write_capability_missing}} =
             Selecto.Write.preview(selecto, returning)

    assert {:ok, batch} = Batch.new([command!(:insert), command!(:delete)])
    assert {:ok, %Preview{metadata: %{atomic?: true}}} = Selecto.Write.preview(selecto, batch)
  end

  test "upsert row normalization preserves a failed guard as zero" do
    assert SelectoDBMariaDB.Adapter.logical_affected_rows(:upsert, 0) == 0
    assert SelectoDBMariaDB.Adapter.logical_affected_rows(:upsert, 1) == 1
    assert SelectoDBMariaDB.Adapter.logical_affected_rows(:upsert, 2) == 1
  end

  test "mariadb adapter rejects invalid connection options" do
    assert SelectoDBMariaDB.Adapter.connect(123) == {:error, {:invalid_connection_options, 123}}
  end

  test "mariadb adapter returns a dependency or connection result" do
    result = SelectoDBMariaDB.Adapter.connect([])

    if Code.ensure_loaded?(MyXQL) do
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    else
      assert result == {:error, {:adapter_dependency_missing, :myxql}}
    end
  end

  test "mariadb adapter does not claim stream support" do
    refute SelectoDBMariaDB.Adapter.supports?(:stream)
  end

  test "mariadb adapter reports rollup support" do
    assert SelectoDBMariaDB.Adapter.supports?(:rollup)
  end

  test "mariadb adapter reports schema introspection support" do
    assert SelectoDBMariaDB.Adapter.supports?(:schema_introspection)
  end

  test "mariadb adapter lists tables for selecto_mix generators" do
    conn =
      stub_connection(fn query, params, opts ->
        cond do
          query =~ "SELECT DATABASE()" ->
            assert params == []
            assert opts == [prepared: false]
            {:ok, %{rows: [["shop_dev"]], columns: ["DATABASE()"]}}

          query =~ "information_schema.tables" ->
            assert params == ["shop_dev"]
            assert opts == [prepared: false]
            {:ok, %{rows: [["orders"], ["users"]], columns: ["table_name"]}}

          true ->
            flunk("unexpected query: #{query}")
        end
      end)

    assert {:ok, ["orders", "users"]} =
             SelectoDBMariaDB.Adapter.list_tables(conn, schema: "public")
  end

  test "mariadb adapter lists relations including views when requested" do
    conn =
      stub_connection(fn query, params, opts ->
        cond do
          query =~ "SELECT DATABASE()" ->
            assert params == []
            assert opts == [prepared: false]
            {:ok, %{rows: [["shop_dev"]], columns: ["DATABASE()"]}}

          query =~ "table_type IN ('BASE TABLE', 'VIEW')" ->
            assert params == ["shop_dev"]
            assert opts == [prepared: false]
            {:ok, %{rows: [["orders", "table"], ["active_orders", "view"]], columns: []}}

          true ->
            flunk("unexpected query: #{query}")
        end
      end)

    assert {:ok,
            [%{name: "orders", source_kind: :table}, %{name: "active_orders", source_kind: :view}]} =
             SelectoDBMariaDB.Adapter.list_relations(conn, schema: "public", include_views: true)
  end

  test "mariadb adapter introspects tables for selecto_mix generators" do
    conn =
      stub_connection(fn query, params, _opts ->
        cond do
          query =~ "FROM information_schema.columns" ->
            assert params == ["shop_dev", "orders"]

            {:ok,
             %{
               rows: [
                 ["id", "int", "int(11)", "NO", nil, nil, 10, 0, nil, 1],
                 ["customer_id", "int", "int(11)", "NO", nil, nil, 10, 0, nil, 2],
                 ["inserted_at", "datetime", "datetime", "YES", nil, nil, nil, nil, 6, 3]
               ],
               columns: []
             }}

          query =~ "constraint_name = 'PRIMARY'" ->
            {:ok, %{rows: [["id"]], columns: []}}

          query =~ "referenced_table_name IS NOT NULL" ->
            {:ok,
             %{
               rows: [["orders_customer_id_fkey", "customer_id", "shop_dev", "customers", "id"]],
               columns: []
             }}

          true ->
            flunk("unexpected query: #{query}")
        end
      end)

    assert {:ok, metadata} =
             SelectoDBMariaDB.Adapter.introspect_table(conn, "orders", schema: "shop_dev")

    assert metadata.table_name == "orders"
    assert metadata.schema == "shop_dev"
    assert metadata.fields == [:id, :customer_id, :inserted_at]
    assert metadata.field_types.id == :integer
    assert metadata.field_types.inserted_at == :naive_datetime
    assert metadata.primary_key == :id
    assert metadata.source == :mariadb

    assert metadata.associations == %{
             customer: %{
               association_type: :belongs_to,
               constraint_name: "orders_customer_id_fkey",
               field: :customer,
               is_through: false,
               join_type: :inner,
               owner_key: :customer_id,
               queryable: :customers,
               related_key: :id,
               related_module_name: "Customer",
               related_schema: "Customer",
               related_table: "customers",
               type: :belongs_to
             }
           }
  end

  test "mariadb rollup uses WITH ROLLUP syntax without postgres wrapper" do
    selecto =
      sales_domain()
      |> Selecto.configure(:mock_connection, adapter: SelectoDBMariaDB.Adapter, validate: false)
      |> Selecto.select(["region", {:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])
    normalized_sql = String.replace(sql, ~r/\s+/, " ")

    assert String.contains?(
             String.downcase(normalized_sql),
             "group by selecto_root.region with rollup"
           )

    refute String.contains?(normalized_sql, "select * from (")
    refute String.contains?(normalized_sql, ") as rollupfix")
    refute String.contains?(String.downcase(normalized_sql), "nulls")
  end

  defp sales_domain do
    %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }
  end

  test "a tenant foreign-key guard binds the referenced row's tenant behind an alias" do
    guard = %{
      field: :project_id,
      relation: :projects,
      target_field: :id,
      tenant_field: :tenant_id,
      tenant_value: 7
    }

    insert =
      command!(:insert,
        relation: :tasks,
        assignments: [%{field: :project_id, value: {:literal, 80}}],
        metadata: %{foreign_key_guards: [guard]}
      )

    assert {:ok, %Preview{statements: [%{text: sql, params: [80, 80, 7]}]}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, insert, [])

    assert sql ==
             "INSERT INTO `tasks` (`project_id`) SELECT ? WHERE EXISTS " <>
               "(SELECT 1 FROM `projects` AS `selecto_fk_parent` " <>
               "WHERE `selecto_fk_parent`.`id` = ? AND `selecto_fk_parent`.`tenant_id` = ?)"

    update =
      command!(:update,
        relation: :tasks,
        assignments: [%{field: :project_id, value: {:literal, 80}}],
        metadata: %{foreign_key_guards: [guard]}
      )

    assert {:ok, %Preview{statements: [%{text: update_sql, params: [80, 1, 80, 7]}]}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, update, [])

    assert update_sql =~
             "WHERE `id` = ? AND EXISTS (SELECT 1 FROM `projects` AS `selecto_fk_parent` " <>
               "WHERE `selecto_fk_parent`.`id` = ? AND `selecto_fk_parent`.`tenant_id` = ?)"

    base = Map.drop(guard, [:tenant_field, :tenant_value])

    for invalid <- [
          Map.put(base, :tenant_field, :tenant_id),
          Map.merge(base, %{tenant_field: :tenant_id, tenant_value: nil}),
          Map.merge(base, %{tenant_field: 7, tenant_value: 7}),
          Map.merge(base, %{tenant_field: nil, tenant_value: 7}),
          Map.merge(base, %{tenant_field: " ", tenant_value: 7})
        ] do
      command = %{insert | metadata: %{foreign_key_guards: [invalid]}}

      assert {:error, %Error{type: :invalid_foreign_key_guard}} =
               SelectoDBMariaDB.Adapter.preview_write(:unused, command, [])
    end
  end

  test "refuses upserts under any write scope because ON DUPLICATE KEY ignores the target" do
    upsert =
      command!(:upsert,
        assignments: [
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :sku, value: {:literal, "B"}},
          %{field: :name, value: {:literal, "updated"}}
        ],
        metadata: %{
          conflict_target: [:tenant_id, :sku],
          declared_conflict_targets: [[:tenant_id, :sku]],
          upsert_update_fields: [:name]
        }
      )

    assert {:ok, %Preview{}} = SelectoDBMariaDB.Adapter.preview_write(:unused, upsert, [])

    assert {:ok, %Preview{}} =
             SelectoDBMariaDB.Adapter.preview_write(:unused, upsert, context: %{actor: 3})

    scoped = [
      {upsert, [context: %{tenant_id: 7}], :tenant},
      {upsert, [context: %{"tenant" => 7}], :tenant},
      {upsert, [context: %{required_filters: [{"tenant_id", 7}]}], :tenant},
      {%{upsert | predicate: {:eq, {:field, :tenant_id}, {:literal, 7}}}, [], :predicate},
      {put_in(upsert.metadata[:query_enforcement], %{}), [], :query_enforcement}
    ]

    for {command, opts, scope} <- scoped do
      assert {:error, %Error{type: :unsupported_scope_predicate, details: %{scope: ^scope}}} =
               SelectoDBMariaDB.Adapter.preview_write(:unused, command, opts)
    end
  end

  test "execute/4 refuses non-binary protocols before contacting the server" do
    for query_type <- [:text, :binary_then_text] do
      assert {:error, %Selecto.Error{type: :validation_error, details: %{option: :query_type}}} =
               SelectoDBMariaDB.Adapter.execute(:unused, "SELECT 1; SELECT 2", [],
                 query_type: query_type
               )
    end
  end

  test "driver errors keep a stable category and drop server text and SQL" do
    duplicate = %MyXQL.Error{
      message: "Duplicate entry 'secret@example.test' for key 'people.email'",
      statement: "INSERT INTO `people` VALUES ('secret@example.test')",
      mysql: %{code: 1062, name: :ER_DUP_ENTRY}
    }

    connection = %DBConnection.ConnectionError{
      message: "tcp connect (db.internal:3306): connection refused - :econnrefused"
    }

    assert %Selecto.Error{
             type: :query_error,
             query: nil,
             params: [],
             details: %{adapter: :mariadb, category: :unique_violation, code: 1062}
           } = error = SelectoDBMariaDB.Adapter.normalize_error(duplicate)

    refute inspect(error) =~ "secret"
    refute inspect(error) =~ "people"

    assert %Selecto.Error{type: :connection_error, details: %{category: :connection_error}} =
             connection_error = SelectoDBMariaDB.Adapter.normalize_error(connection)

    refute inspect(connection_error) =~ "db.internal"

    for {code, category} <- [
          {1452, :foreign_key_violation},
          {1048, :not_null_violation},
          {4025, :check_violation},
          {1064, :database_error}
        ] do
      assert %Selecto.Error{details: %{category: ^category}} =
               SelectoDBMariaDB.Adapter.sanitize_error(%MyXQL.Error{
                 message: "x",
                 mysql: %{code: code, name: nil}
               })
    end
  end

  defp stub_connection(query_fun), do: %{query_fun: query_fun}

  defp command!(operation, overrides \\ []) do
    defaults = %{
      operation: operation,
      relation: :items,
      assignments: [%{field: :name, value: {:literal, "value"}}],
      predicate: if(operation in [:update, :delete], do: {:eq, {:field, :id}, {:literal, 1}}),
      expected_cardinality: {:exactly, 1},
      returning: :none,
      metadata: %{}
    }

    {:ok, command} = Command.new(Map.merge(defaults, Map.new(overrides)))
    command
  end
end
