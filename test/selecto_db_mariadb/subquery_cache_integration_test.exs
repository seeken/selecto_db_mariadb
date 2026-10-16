defmodule SelectoDBMariaDB.SubqueryCacheIntegrationTest do
  # MariaDB's subquery cache keys a correlated subquery's outer references
  # under the column's collation: an outer 'T7' (or 't7 ' under PAD SPACE)
  # reused the result computed for 't7', whatever exact comparison the SQL
  # used. Every statement the adapter sends turns the cache off for itself.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  @variants [{"t7", "mine"}, {"t8", "SECRET-t8"}, {"T7", "SECRET-T7"}, {"t7 ", "SECRET-sp"}]

  test "a correlated scalar subquery compares the exact outer value" do
    with_database(fn conn ->
      ddl!(conn, "CREATE TABLE t (id INT PRIMARY KEY, org_key VARCHAR(16))")
      ddl!(conn, "INSERT INTO t VALUES (1, 't7'), (2, 't7'), (3, 'T7')")

      bytes = &"CAST(CONVERT(#{&1}.org_key USING utf8mb4) AS BINARY)"

      assert {:ok, %{rows: rows}} =
               Adapter.execute(
                 conn,
                 "SELECT id, (SELECT COUNT(*) FROM t rs WHERE #{bytes.("rs")} = #{bytes.("t")}) " <>
                   "FROM t ORDER BY id",
                 [],
                 []
               )

      assert rows == [[1, 2], [2, 2], [3, 1]]
    end)
  end

  for semijoin <- ["on", "off"] do
    test "scope-key retargets, collections and through joins stay exact (semijoin=#{semijoin})" do
      with_database(fn conn ->
        setup!(conn)
        ddl!(conn, "SET SESSION optimizer_switch='semijoin=#{unquote(semijoin)}'")

        for bound <- [:engine, :required] do
          scoped = fn domain ->
            base = Selecto.configure(domain, conn, adapter: Adapter, validate: false)

            if bound == :engine,
              do: Selecto.with_tenant(base, "t7"),
              else: Selecto.Tenant.require_tenant_filter(base, "tenant_key", "t7")
          end

          chain = scoped.(chain_domain())

          for strategy <- [:in, :exists],
              {target, expected} <- [{:a, ["mine"]}, {"a.reg", ["reg-mine"]}] do
            query =
              chain
              |> Selecto.retarget(target, strategy: strategy)
              |> Selecto.select(["id", "label"])

            assert labels(query) == expected, "#{bound} #{strategy} #{target}"
          end

          collection =
            chain
            |> Selecto.select(["id"])
            |> Selecto.subselect([
              %{
                fields: ["label"],
                target_schema: :acc,
                format: :json_agg,
                alias: "c",
                join_path: [:a]
              }
            ])

          assert labels(collection) == ["mine"], "#{bound} collection"

          through = scoped.(through_domain())

          assert labels(Selecto.select(through, ["id", "th.label"])) == ["mine"],
                 "#{bound} through join"

          for strategy <- [:in, :exists] do
            query =
              through
              |> Selecto.retarget(:th, strategy: strategy)
              |> Selecto.select(["id", "label"])

            assert labels(query) == ["mine"], "#{bound} through #{strategy}"
          end
        end
      end)
    end
  end

  defp labels(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)

    ~r/(SECRET[-\w]*|reg-mine|mine)/
    |> Regex.scan(inspect(rows, limit: :infinity, printable_limit: :infinity), capture: :first)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp setup!(conn) do
    ddl!(
      conn,
      "CREATE TABLE rs_items (id INT PRIMARY KEY, tenant_key VARCHAR(16) NOT NULL, account_id INT, name VARCHAR(20))"
    )

    ddl!(
      conn,
      "CREATE TABLE rs_acc (id INT NOT NULL, org_key VARCHAR(16) NOT NULL, region_id INT, label VARCHAR(40))"
    )

    ddl!(
      conn,
      "CREATE TABLE rs_reg (id INT NOT NULL, org_key VARCHAR(16) NOT NULL, label VARCHAR(40))"
    )

    ddl!(
      conn,
      "CREATE TABLE th_bridge (item_id INT NOT NULL, acc_id INT NOT NULL, org_key VARCHAR(16) NOT NULL)"
    )

    ddl!(
      conn,
      "INSERT INTO rs_items VALUES (1, 't7', 101, 'own'), (2, 't8', 101, 'other'), (3, 'T7', 101, 'caps')"
    )

    for {tenant, label} <- @variants do
      MyXQL.query!(conn, "INSERT INTO rs_acc VALUES (101, ?, 201, ?)", [tenant, label])

      MyXQL.query!(conn, "INSERT INTO rs_reg VALUES (201, ?, ?)", [
        tenant,
        label |> String.replace("mine", "reg-mine") |> String.replace("SECRET", "SECRET-reg")
      ])
    end

    ddl!(conn, "INSERT INTO th_bridge VALUES (1, 101, 't7'), (2, 101, 't8'), (3, 101, 'T7')")
  end

  defp relation(table, columns, associations \\ %{}) do
    %{
      source_table: table,
      primary_key: :id,
      fields: Keyword.keys(columns),
      redact_fields: [],
      columns: Map.new(columns, fn {name, type} -> {name, %{type: type}} end),
      associations: associations
    }
  end

  defp items(associations) do
    relation(
      "rs_items",
      [id: :integer, tenant_key: :string, account_id: :integer, name: :string],
      associations
    )
    |> Map.put(:tenant_field, :tenant_key)
  end

  defp chain_domain do
    %{
      name: "Items",
      source:
        items(%{
          a: %{
            queryable: :acc,
            field: :a,
            owner_key: :account_id,
            related_key: :id,
            cardinality: :one,
            source_scope_key: :tenant_key,
            target_scope_key: :org_key
          }
        }),
      schemas: %{
        acc:
          relation(
            "rs_acc",
            [id: :integer, org_key: :string, region_id: :integer, label: :string],
            %{
              reg: %{
                queryable: :reg,
                field: :reg,
                owner_key: :region_id,
                related_key: :id,
                cardinality: :one,
                source_scope_key: :org_key,
                target_scope_key: :org_key
              }
            }
          ),
        reg: relation("rs_reg", id: :integer, org_key: :string, label: :string)
      },
      joins: %{a: %{type: :left, name: "a", joins: %{reg: %{type: :left, name: "reg"}}}}
    }
  end

  defp through_domain do
    %{
      name: "Th",
      source:
        items(%{
          th: %{
            queryable: :acc,
            field: :th,
            owner_key: :id,
            related_key: :id,
            cardinality: :many,
            through: %{
              table: "th_bridge",
              owner_key: :item_id,
              related_key: :acc_id,
              source_scope_key: :tenant_key,
              through_scope_key: :org_key,
              target_scope_key: :org_key
            }
          }
        }),
      schemas: %{
        acc:
          relation("rs_acc", id: :integer, org_key: :string, region_id: :integer, label: :string)
      },
      joins: %{th: %{type: :left, name: "th"}}
    }
  end

  defp ddl!(conn, sql), do: MyXQL.query!(conn, sql, [], query_type: :text)

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_sqcache_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    ddl!(admin, "CREATE DATABASE `#{database}`")

    try do
      # One connection, so the session's optimizer_switch applies to every call.
      {:ok, conn} =
        Adapter.connect(Keyword.merge(connection_options(), database: database, pool_size: 1))

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
