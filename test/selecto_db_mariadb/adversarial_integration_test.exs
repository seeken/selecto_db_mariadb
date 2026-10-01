defmodule SelectoDBMariaDB.AdversarialIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.{Adapter, Dialect}
  alias Selecto.Dialect.Json.{ArrayContains, Contains, Operation}
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  # The server keeps its default sql_mode, so backslash escapes stay active in
  # quoted literals. That is the production setting these cases guard.
  @tricky_strings [
    "trailing backslash \\",
    "double \" quote",
    "literal \\u0041 escape",
    "it's quoted",
    ""
  ]

  describe "inlined JSON values" do
    test "json_build_array returns every value unchanged" do
      with_database(fn conn ->
        values = ["ends with a backslash \\", "second value"]

        assert select_json(conn, operation(:json_build_array, value: values)) == {:ok, values}

        assert select_json(conn, operation(:json_build_array, value: @tricky_strings)) ==
                 {:ok, @tricky_strings}
      end)
    end

    test "json_build_object returns every key and value unchanged" do
      with_database(fn conn ->
        pairs = [{"key \\", "value \\"}, {"next", "plain"}]

        assert select_json(conn, operation(:json_build_object, value: pairs)) ==
                 {:ok, %{"key \\" => "value \\", "next" => "plain"}}
      end)
    end

    test "json_set stores string values unchanged" do
      with_database(fn conn ->
        for value <- @tricky_strings do
          set =
            operation(:json_set,
              path: "$.a",
              value: value,
              options: %{column_sql: "JSON_OBJECT()"}
            )

          assert select_json(conn, set) == {:ok, %{"a" => value}}
        end
      end)
    end

    test "json containment candidates match the stored document exactly" do
      with_database(fn conn ->
        ddl!(conn, """
        CREATE TABLE `docs` (`id` int NOT NULL PRIMARY KEY, `body` json NOT NULL,
          `tags` json NOT NULL) ENGINE=InnoDB
        """)

        for {value, id} <- Enum.with_index(@tricky_strings, 1) do
          assert {:ok, _} =
                   Adapter.execute(
                     conn,
                     "INSERT INTO `docs` VALUES (?, JSON_OBJECT('name', ?), JSON_ARRAY(?))",
                     [id, value, value],
                     []
                   )
        end

        for {value, id} <- Enum.with_index(@tricky_strings, 1) do
          {:ok, contains} =
            Dialect.render_json_contains(
              %Contains{column: "body", value: %{"name" => value}},
              %{}
            )

          {:ok, member} =
            Dialect.render_json_array_contains(
              %ArrayContains{column: "tags", path: [], value: value},
              %{}
            )

          assert matching_ids(conn, contains) == {:ok, [id]}
          assert matching_ids(conn, member) == {:ok, [id]}
        end
      end)
    end
  end

  describe "query protocol" do
    test "execute/4 refuses the text protocol so stacked statements cannot run" do
      with_database(fn conn ->
        reset_marker!(conn)

        for query_type <- [:text, :binary_then_text] do
          result =
            attempt(fn ->
              Adapter.execute(conn, "SELECT 1; SET @adv_marker = 1", [], query_type: query_type)
            end)

          assert marker(conn) == 0
          assert {:ok, {:error, %Selecto.Error{type: :validation_error}}} = result
        end

        assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, "SELECT 1", [], query_type: :binary)
      end)
    end
  end

  describe "driver errors" do
    test "are reduced to a stable category without SQL, values or server text" do
      with_database(fn conn ->
        ddl!(conn, """
        CREATE TABLE `people` (`id` int NOT NULL PRIMARY KEY,
          `email` varchar(80) NOT NULL UNIQUE) ENGINE=InnoDB
        """)

        insert = "INSERT INTO `people` (`id`, `email`) VALUES (?, ?)"
        assert {:ok, _} = Adapter.execute(conn, insert, [1, "secret-value@example.test"], [])

        assert {:error, duplicate} =
                 Adapter.execute(conn, insert, [2, "secret-value@example.test"], [])

        assert {:error, syntax} =
                 Adapter.execute(
                   conn,
                   "SELECT `id` FROM `people` WHERE 'secret-marker' AND",
                   [],
                   []
                 )

        for {reason, category} <- [{duplicate, :unique_violation}, {syntax, :database_error}] do
          error = Adapter.normalize_error(reason)
          rendered = inspect(error, limit: :infinity, printable_limit: :infinity)

          refute rendered =~ "secret-value"
          refute rendered =~ "secret-marker"
          refute rendered =~ "people"
          assert %Selecto.Error{type: :query_error, details: %{category: ^category}} = error
        end
      end)
    end
  end

  describe "tenant-scoped upsert" do
    test "cannot update another tenant's row through a different unique key" do
      with_database(fn conn ->
        ddl!(conn, """
        CREATE TABLE `skus` (
          `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `tenant_id` int NOT NULL,
          `sku` varchar(40) NOT NULL,
          `email` varchar(120) NOT NULL,
          `name` varchar(120) NOT NULL,
          UNIQUE KEY `uq_skus_tenant_sku` (`tenant_id`, `sku`),
          UNIQUE KEY `uq_skus_email` (`email`)
        ) ENGINE=InnoDB
        """)

        ddl!(conn, """
        INSERT INTO `skus` (`id`, `tenant_id`, `sku`, `email`, `name`)
        VALUES (1, 8, 'A', 'shared@example.test', 'tenant-8 original')
        """)

        original = [8, "A", "shared@example.test", "tenant-8 original"]

        email_collision =
          upsert!(tenant_id: 7, sku: "B", email: "shared@example.test", name: "from tenant 7")

        pk_collision =
          upsert!(id: 1, tenant_id: 7, sku: "C", email: "t7@example.test", name: "from tenant 7")

        for command <- [email_collision, pk_collision] do
          result = Adapter.execute_write_unsafe(conn, command, context: %{tenant_id: 7})

          assert sku_row(conn, 1) == original
          assert {:error, %Error{type: :unsupported_scope_predicate}} = result
        end

        # An unscoped upsert on the same table keeps its native behavior.
        unscoped = upsert!(tenant_id: 7, sku: "D", email: "d@example.test", name: "unscoped")
        assert {:ok, %{affected_rows: 1}} = Adapter.execute_write_unsafe(conn, unscoped, [])
      end)
    end
  end

  describe "tenant foreign-key guard" do
    test "a tenant-7 write cannot reference tenant 8's parent" do
      with_database(fn conn ->
        ddl!(conn, """
        CREATE TABLE `projects` (`id` int NOT NULL PRIMARY KEY, `tenant_id` int NOT NULL)
        ENGINE=InnoDB
        """)

        ddl!(conn, """
        CREATE TABLE `tasks` (
          `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `tenant_id` int NOT NULL,
          `project_id` int NOT NULL,
          `name` varchar(40) NOT NULL,
          CONSTRAINT `fk_tasks_project` FOREIGN KEY (`project_id`) REFERENCES `projects` (`id`)
        ) ENGINE=InnoDB
        """)

        ddl!(conn, "INSERT INTO `projects` (`id`, `tenant_id`) VALUES (70, 7), (80, 8)")

        ddl!(
          conn,
          "INSERT INTO `tasks` (`id`, `tenant_id`, `project_id`, `name`) VALUES (1, 7, 70, 'seed')"
        )

        assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
                 Adapter.execute_write_unsafe(conn, task_insert!(80), [])

        assert {:error, %Error{type: :cardinality_mismatch, details: %{actual: 0}}} =
                 Adapter.execute_write_unsafe(conn, task_update!(80), [])

        assert task_rows(conn) == [[1, 7, 70, "seed"]]

        assert {:ok, %{affected_rows: 1}} =
                 Adapter.execute_write_unsafe(conn, task_insert!(70), [])

        assert {:ok, %{affected_rows: 1}} =
                 Adapter.execute_write_unsafe(conn, task_update!(70), [])

        assert task_rows(conn) |> Enum.map(&tl/1) == [[7, 70, "t"], [7, 70, "t"]]
      end)
    end
  end

  @tenant_guard %{
    field: :project_id,
    relation: :projects,
    target_field: :id,
    tenant_field: :tenant_id,
    tenant_value: 7
  }

  defp task_insert!(project_id) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: :tasks,
        assignments: [
          %{field: :tenant_id, value: {:literal, 7}},
          %{field: :project_id, value: {:literal, project_id}},
          %{field: :name, value: {:literal, "t"}}
        ],
        metadata: %{foreign_key_guards: [@tenant_guard]}
      })

    command
  end

  defp task_update!(project_id) do
    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: :tasks,
        assignments: [
          %{field: :project_id, value: {:literal, project_id}},
          %{field: :name, value: {:literal, "t"}}
        ],
        predicate:
          {:and,
           [{:eq, {:field, :id}, {:literal, 1}}, {:eq, {:field, :tenant_id}, {:literal, 7}}]},
        metadata: %{foreign_key_guards: [@tenant_guard]}
      })

    command
  end

  defp task_rows(conn) do
    {:ok, %{rows: rows}} =
      Adapter.execute(
        conn,
        "SELECT `id`, `tenant_id`, `project_id`, `name` FROM `tasks` ORDER BY `id`",
        [],
        []
      )

    rows
  end

  defp operation(kind, fields) do
    struct!(Operation, [operation: kind, clause: :select] ++ fields)
  end

  defp select_json(conn, %Operation{} = operation) do
    {:ok, sql} = Dialect.render_json_operation(operation, %{})

    case Adapter.execute(conn, ["SELECT ", sql] |> IO.iodata_to_binary(), [], []) do
      {:ok, %{rows: [[value]]}} when is_binary(value) -> {:ok, Jason.decode!(value)}
      {:ok, %{rows: [[value]]}} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp matching_ids(conn, predicate) do
    sql = IO.iodata_to_binary(["SELECT `id` FROM `docs` WHERE ", predicate, " ORDER BY `id`"])

    case Adapter.execute(conn, sql, [], []) do
      {:ok, %{rows: rows}} -> {:ok, List.flatten(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp upsert!(assignments) do
    {:ok, command} =
      Command.new(%{
        operation: :upsert,
        relation: :skus,
        assignments:
          Enum.map(assignments, fn {field, value} -> %{field: field, value: {:literal, value}} end),
        expected_cardinality: {:exactly, 1},
        metadata: %{
          conflict_target: [:tenant_id, :sku],
          declared_conflict_targets: [[:tenant_id, :sku]],
          upsert_update_fields: [:name]
        }
      })

    command
  end

  defp sku_row(conn, id) do
    {:ok, %{rows: [row]}} =
      Adapter.execute(
        conn,
        "SELECT `tenant_id`, `sku`, `email`, `name` FROM `skus` WHERE `id` = ?",
        [id],
        []
      )

    row
  end

  defp reset_marker!(conn), do: {:ok, _} = Adapter.execute(conn, "SET @adv_marker = 0", [], [])

  defp marker(conn) do
    {:ok, %{rows: [[value]]}} = Adapter.execute(conn, "SELECT @adv_marker", [], [])
    value
  end

  defp attempt(fun) do
    {:ok, fun.()}
  rescue
    exception -> {:raised, exception}
  catch
    :exit, reason -> {:exit, reason}
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "selecto_mariadb_adv_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    ddl!(admin, "CREATE DATABASE `#{database}`")

    try do
      # Binding the database to the connection keeps it across reconnects.
      {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

      try do
        fun.(conn)
      after
        Adapter.disconnect(conn)
      end
    after
      MyXQL.query(admin, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
      Adapter.disconnect(admin)
    end
  end

  # Test-owned DDL uses the driver directly; the adapter accepts only the
  # binary protocol.
  defp ddl!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
