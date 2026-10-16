defmodule SelectoDBMariaDB.ThroughScopeIntegrationTest do
  # A through association pairs root -> bridge -> target scope keys. When one
  # side may hold text (by its own declared type; an undeclared column is
  # text), MySQL compares INT with VARCHAR numerically and text under the
  # collation: tenant 7 would reach targets scoped '07', '7 ', ' 7' and '7e0',
  # tenant 't1' those of 'T1', 't1 ' and ' t1'. Each pair compares exactly.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 180_000

  # {root tenant type, bridge scope type}; targets always scope by VARCHAR.
  @sets %{s1: {:int, :varchar}, s2: {:int, :int}, s3: {:text, :varchar}}
  @targets %{
    int: ["7", "07", "7 ", " 7", "7e0", "8"],
    text: ["t1", "T1", "t1 ", " t1", "tı", "t2"]
  }

  for set <- [:s1, :s2, :s3], naming <- [:n1, :n2], declared <- [false, true] do
    test "#{set} #{naming} #{if declared, do: "declared", else: "undeclared"} bridge reaches only the own target" do
      with_database(fn conn ->
        {root, _bridge} = @sets[unquote(set)]
        domain = setup!(conn, unquote(set), unquote(naming), unquote(declared))

        bounds =
          if root == :int,
            do: [{:tenant, 7}, {:tenant, "7"}, {:required, 7}],
            else: [{:tenant, "t1"}, {:required, "t1"}]

        for bound <- bounds do
          base = Selecto.configure(domain, conn, adapter: Adapter, validate: false)

          base =
            case bound do
              {:tenant, value} -> Selecto.with_tenant(base, value)
              {:required, value} -> Selecto.Tenant.require_tenant_filter(base, "tenant_id", value)
            end

          join = base |> Selecto.select(["id", "targets.name"]) |> Selecto.filter([{"id", 1}])

          collection =
            base
            |> Selecto.select(["id"])
            |> Selecto.filter([{"id", 1}])
            |> Selecto.subselect([
              %{
                fields: ["name"],
                target_schema: :target,
                format: :json_agg,
                alias: "c",
                join_path: [:targets]
              }
            ])

          retarget =
            base
            |> Selecto.filter([{"id", 1}])
            |> Selecto.retarget(:targets)
            |> Selecto.select(["id", "name"])

          for {path, query} <- [join: join, collection: collection, retarget: retarget] do
            assert target_names(query) == ["tgt1"], "#{inspect(bound)} #{path}"
          end
        end
      end)
    end
  end

  defp target_names(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)

    ~r/tgt\d/
    |> Regex.scan(inspect(rows, limit: :infinity, printable_limit: :infinity))
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp setup!(conn, set, naming, declared) do
    {root, bridge_type} = @sets[set]

    {bridge_column, target_column} =
      if naming == :n1, do: {"tenant_id", "tenant_id"}, else: {"scope_code", "org_code"}

    root_sql = if root == :int, do: "INT", else: "VARCHAR(16)"
    bridge_sql = if bridge_type == :int, do: "INT", else: "VARCHAR(16)"
    {own, other} = if root == :int, do: {"7", "8"}, else: {"'t1'", "'t2'"}

    {bridge_own, bridge_other} =
      cond do
        bridge_type == :int -> {"7", "8"}
        root == :int -> {"'7'", "'8'"}
        true -> {"'t1'", "'t2'"}
      end

    for sql <- [
          "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id #{root_sql} NOT NULL, name VARCHAR(20))",
          "CREATE TABLE bridges (id INT PRIMARY KEY, order_id INT NOT NULL, target_id INT NOT NULL, #{bridge_column} #{bridge_sql} NOT NULL)",
          "CREATE TABLE targets (id INT PRIMARY KEY, #{target_column} VARCHAR(16) NOT NULL, name VARCHAR(30))",
          "INSERT INTO orders VALUES (1, #{own}, 'own'), (2, #{other}, 'other')"
        ],
        do: driver!(conn, sql)

    for {scope, index} <- Enum.with_index(@targets[root], 1) do
      MyXQL.query!(conn, "INSERT INTO targets VALUES (?, ?, ?)", [index, scope, "tgt#{index}"])
    end

    for index <- 1..5,
        do:
          driver!(conn, "INSERT INTO bridges VALUES (#{20 + index}, 1, #{index}, #{bridge_own})")

    driver!(conn, "INSERT INTO bridges VALUES (26, 2, 6, #{bridge_other})")

    root_type = if root == :int, do: :integer, else: :string
    bridge_field = String.to_atom(bridge_column)
    target_field = String.to_atom(target_column)

    schemas = %{
      target: %{
        source_table: "targets",
        primary_key: :id,
        fields: [:id, target_field, :name],
        redact_fields: [],
        columns: %{
          :id => %{type: :integer},
          target_field => %{type: :string},
          :name => %{type: :string}
        },
        associations: %{}
      }
    }

    schemas =
      if declared,
        do:
          Map.put(schemas, :bridge, %{
            source_table: "bridges",
            primary_key: :id,
            tenant_field: bridge_field,
            fields: [:id, :order_id, :target_id, bridge_field],
            redact_fields: [],
            columns: %{
              :id => %{type: :integer},
              :order_id => %{type: :integer},
              :target_id => %{type: :integer},
              bridge_field => %{type: if(bridge_type == :int, do: :integer, else: :string)}
            },
            associations: %{}
          }),
        else: schemas

    %{
      name: "Thru",
      source: %{
        source_table: "orders",
        primary_key: :id,
        tenant_field: :tenant_id,
        fields: [:id, :tenant_id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, tenant_id: %{type: root_type}, name: %{type: :string}},
        associations: %{
          targets: %{
            queryable: :target,
            field: :targets,
            owner_key: :id,
            related_key: :id,
            cardinality: :many,
            through: %{
              table: "bridges",
              owner_key: :order_id,
              related_key: :target_id,
              source_scope_key: :tenant_id,
              through_scope_key: bridge_field,
              target_scope_key: target_field
            }
          }
        }
      },
      schemas: schemas,
      joins: %{targets: %{type: :left, name: "targets"}}
    }
  end

  defp driver!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_thru_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    driver!(admin, "CREATE DATABASE `#{database}`")

    try do
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

  defp connection_options do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end
end
