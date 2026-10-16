defmodule SelectoDBMariaDB.RetargetScopeKeysIntegrationTest do
  # A retarget onto a relation confined only by scope keys identifies the
  # target row by its primary key AND those keys, the keys compared exactly:
  # ids repeat across scopes, and 'T7' must not pass for 't7'.
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  # The retarget scope-key fixture (intprobe4). Accounts and regions are
  # confined only by scope keys and repeat their ids across tenants:
  # v1 names the keys tenant_key -> org_key -> org_key, v3 tenant_key all the
  # way. Tenant t7 must never reach t8's SECRET rows, nor a case variant
  # 'T7' on a case-insensitive collation (the physical key is (id, label)).
  @variants [:v1, :v3]

  defp retarget_statements(variant) do
    {prefix, key} = if variant == :v1, do: {"rs", "org_key"}, else: {"rs2", "tenant_key"}

    [
      "CREATE TABLE #{prefix}_items (id INT PRIMARY KEY, tenant_key VARCHAR(16) NOT NULL, account_id INT, name VARCHAR(20))",
      "CREATE TABLE #{prefix}_acc (id INT NOT NULL, #{key} VARCHAR(16) NOT NULL, region_id INT, label VARCHAR(40), PRIMARY KEY (id, label))",
      "CREATE TABLE #{prefix}_reg (id INT NOT NULL, #{key} VARCHAR(16) NOT NULL, label VARCHAR(40), PRIMARY KEY (id, label))",
      "INSERT INTO #{prefix}_items VALUES (1, 't7', 101, 'own'), (2, 't8', 101, 'other')",
      "INSERT INTO #{prefix}_acc VALUES (101, 't7', 201, 'mine'), (101, 't8', 201, 'SECRET-t8'), (101, 'T7', 201, 'SECRET-T7')",
      "INSERT INTO #{prefix}_reg VALUES (201, 't7', 'reg-mine'), (201, 't8', 'SECRET-reg-t8'), (201, 'T7', 'SECRET-reg-T7')"
    ]
  end

  defp retarget_domain(variant) do
    {prefix, key} = if variant == :v1, do: {"rs", :org_key}, else: {"rs2", :tenant_key}

    relation = fn table, columns, associations ->
      %{
        source_table: table,
        primary_key: :id,
        fields: Keyword.keys(columns),
        redact_fields: [],
        columns: Map.new(columns, fn {name, type} -> {name, %{type: type}} end),
        associations: associations
      }
    end

    %{
      name: "Items",
      source:
        relation.(
          "#{prefix}_items",
          [id: :integer, tenant_key: :string, account_id: :integer, name: :string],
          %{
            a: %{
              queryable: :acc,
              field: :a,
              owner_key: :account_id,
              related_key: :id,
              cardinality: :one,
              source_scope_key: :tenant_key,
              target_scope_key: key
            }
          }
        )
        |> Map.put(:tenant_field, :tenant_key),
      schemas: %{
        acc:
          relation.(
            "#{prefix}_acc",
            [{:id, :integer}, {key, :string}, {:region_id, :integer}, {:label, :string}],
            %{
              reg: %{
                queryable: :reg,
                field: :reg,
                owner_key: :region_id,
                related_key: :id,
                cardinality: :one,
                source_scope_key: key,
                target_scope_key: key
              }
            }
          ),
        reg: relation.("#{prefix}_reg", [{:id, :integer}, {key, :string}, {:label, :string}], %{})
      },
      joins: %{a: %{type: :left, name: "a", joins: %{reg: %{type: :left, name: "reg"}}}}
    }
  end

  # Every retarget (in and exists, one and two hops) under the engine tenant
  # and under a required filter: {label, query, expected labels}.
  defp retarget_cases(base) do
    for bound <- [:engine, :required],
        strategy <- [:in, :exists],
        {target, expected} <- [{:a, ["mine"]}, {"a.reg", ["reg-mine"]}] do
      bounded =
        if bound == :engine,
          do: Selecto.with_tenant(base, "t7"),
          else: Selecto.Tenant.require_tenant_filter(base, "tenant_key", "t7")

      query =
        bounded
        |> Selecto.retarget(target, strategy: strategy)
        |> Selecto.select(["id", "label"])

      {"#{bound} #{strategy} #{target}", query, expected}
    end
  end

  defp retarget_labels(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    rows |> Enum.map(&List.last/1) |> Enum.sort()
  end

  for variant <- @variants do
    test "#{variant}: every retarget reaches only the own scope's rows" do
      {:ok, admin} = SelectoDBMariaDB.Adapter.connect(connection_options())

      database =
        "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_retarget_" <>
          "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

      MyXQL.query!(admin, "CREATE DATABASE `#{database}`", [], query_type: :text)

      try do
        {:ok, conn} =
          SelectoDBMariaDB.Adapter.connect(Keyword.put(connection_options(), :database, database))

        try do
          for statement <- retarget_statements(unquote(variant)),
              do: MyXQL.query!(conn, statement, [], query_type: :text)

          base =
            Selecto.configure(retarget_domain(unquote(variant)), conn,
              adapter: SelectoDBMariaDB.Adapter,
              validate: false
            )

          for {label, query, expected} <- retarget_cases(base) do
            assert retarget_labels(query) == expected, label
          end
        after
          SelectoDBMariaDB.Adapter.disconnect(conn)
        end
      after
        MyXQL.query(admin, "DROP DATABASE IF EXISTS `#{database}`", [], query_type: :text)
        SelectoDBMariaDB.Adapter.disconnect(admin)
      end
    end
  end

  test "a through target scope key the target does not expose is refused, never key-only" do
    domain = retarget_domain(:v1)

    through =
      put_in(domain, [:source, :associations, :a, :through], %{
        table: "bridges",
        owner_key: :id,
        related_key: :acc_id,
        source_scope_key: :tenant_key,
        through_scope_key: :org_key,
        target_scope_key: :org_key
      })
      |> update_in([:schemas, :acc, :fields], &List.delete(&1, :org_key))

    assert_raise ArgumentError, ~r/target scope field :org_key is not declared/, fn ->
      Selecto.configure(
        through,
        Selecto.Runtime.Context.new(SelectoDBMariaDB.Adapter, :compile_only),
        validate: false
      )
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
