defmodule SelectoDBMariaDB.HostScopeBoundaryIntegrationTest do
  # Host scope decides identity wherever the host installs it: a host
  # Selecto.filter accepted at the read boundary, a co-domain lookup's host
  # scope, and column pairs (host column-vs-column predicates, join keys
  # beside a tenant column). Under the server's default collation 't1' would
  # also match 'T1' and 't1 ', and an INT compared with a VARCHAR compares
  # numerically ('07', '7 ', '7e0' all equal 7). Each must read as on
  # PostgreSQL.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  describe "a host Selecto.filter accepted at the read boundary" do
    test "compares exactly on a tenantless domain and as a site pin; request filters stay plain" do
      with_database(fn conn ->
        scope_tables!(conn)

        for {domain, column, value, bind} <- [
              {hs_domain(), "org_key", "t1", & &1},
              {site_domain(), "site", "s1", &Selecto.with_tenant(&1, 1)}
            ] do
          {:ok, query} =
            domain
            |> Selecto.configure(conn, adapter: Adapter, validate: false)
            |> bind.()
            |> Selecto.filter([{column, value}])
            |> Selecto.Tenant.require_read_boundary()

          assert ids(Selecto.select(query, ["id"])) == [1], column

          # A request filter applied after the boundary is the caller's: it
          # keeps the collated comparison ('A ALPHA' matches 'a alpha').
          assert ids(query |> Selecto.filter([{"name", "A ALPHA"}]) |> Selecto.select(["id"])) ==
                   [1],
                 column
        end
      end)
    end
  end

  describe "a canned page's dataset filters" do
    test "compare exactly in the result, total and facet queries" do
      with_database(fn conn ->
        scope_tables!(conn)

        for {domain, column, value, bind} <- [
              {hs_domain(), "org_key", "t1", & &1},
              {site_domain(), "site", "s1", &Selecto.with_tenant(&1, 1)}
            ] do
          base = domain |> Selecto.configure(conn, adapter: Adapter, validate: false) |> bind.()

          page =
            Selecto.CannedPage.new!(Selecto.filter(base, [{column, value}]),
              id: "scope",
              views: [%{id: "detail", kind: :detail, query: Selecto.select(base, ["id", "name"])}],
              controls: [%{id: "name", kind: :facet, field: "name"}]
            )

          {:ok, plan} = Selecto.CannedPage.plan(page, base, %{})

          assert plan.query |> rows() |> Enum.map(&hd/1) == [1], column
          assert rows(plan.total_query) == [[1]], column
          assert plan.facets["name"].options |> rows() |> Enum.map(&hd/1) == ["a alpha"], column
        end
      end)
    end
  end

  describe "a co-domain lookup's host scope" do
    test "compares exactly on a tenantless target and as a site pin beside a tenant" do
      with_database(fn conn ->
        scope_tables!(conn)
        ddl!(conn, "CREATE FULLTEXT INDEX hs_name ON hs (name)")
        ddl!(conn, "CREATE FULLTEXT INDEX hs_site_name ON hs_site (name)")

        for {domain, scope} <- [
              {hs_domain(), {"org_key", "t1"}},
              {site_domain(), [{"tenant_id", 1}, {"site", "s1"}]}
            ] do
          domain =
            put_in(domain[:query_library], %{projections: %{identity: %{fields: [:id, :name]}}})

          source =
            Map.put(domain, :co_domains, %{
              lookup: %{
                domain: :lookup,
                projection: :identity,
                search: %{fields: [:name], mode: :plain},
                result: %{value_field: :id, label_field: :name}
              }
            })

          target = Selecto.configure(domain, conn, adapter: Adapter, validate: false)

          assert {:ok, %{results: results}} =
                   Selecto.CoDomain.lookup(source, target, :lookup, "alpha", scope: scope)

          assert results |> Enum.map(& &1.value) |> Enum.sort() == ["1"],
                 inspect({scope, results})
        end
      end)
    end
  end

  describe "column pairs decide from both sides" do
    test "a host column-vs-column predicate is exact when either side may be text" do
      with_database(fn conn ->
        ddl!(conn, "CREATE TABLE pr (id INT PRIMARY KEY, n INT, s VARCHAR(5), name VARCHAR(5))")

        ddl!(
          conn,
          "INSERT INTO pr VALUES (1,7,'7','a'),(2,7,'07','b'),(3,7,'7 ','c'),(4,7,' 7','d'),(5,7,'7e0','e'),(6,NULL,NULL,'f')"
        )

        for {n_type, s_type} <- [{:integer, :string}, {:integer, nil}, {nil, :string}, {nil, nil}],
            filter <- [
              {"n", {:=, {:ref, "s"}}},
              {"s", {:=, {:ref, "n"}}},
              {:not, {"n", {:!=, {:ref, "s"}}}}
            ] do
          query =
            %{
              name: "PR",
              source: %{
                source_table: "pr",
                primary_key: :id,
                fields: [:id, :n, :s, :name],
                redact_fields: [],
                columns: %{
                  id: %{type: :integer},
                  n: %{type: n_type},
                  s: %{type: s_type},
                  name: %{type: :string}
                },
                associations: %{}
              },
              schemas: %{},
              joins: %{},
              required_filters: [filter]
            }
            |> Selecto.configure(conn, adapter: Adapter, validate: false)
            |> Selecto.select(["id"])

          assert ids(query) == [1], inspect({n_type, s_type, filter})
        end
      end)
    end

    test "a join key pair beside an INT tenant column compares by bytes, not numerically" do
      with_database(fn conn ->
        ddl!(conn, "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id INT NOT NULL)")
        ddl!(conn, "CREATE TABLE owners (id INT PRIMARY KEY, code VARCHAR(5))")
        ddl!(conn, "INSERT INTO orders VALUES (1, 7)")
        ddl!(conn, "INSERT INTO owners VALUES (1, '7'), (2, '07'), (3, '7 '), (4, '7e0')")

        query =
          %{
            name: "Orders",
            source: %{
              source_table: "orders",
              primary_key: :id,
              fields: [:id, :tenant_id],
              redact_fields: [],
              tenant_field: :tenant_id,
              columns: %{id: %{type: :integer}, tenant_id: %{type: :integer}},
              associations: %{
                owner: %{
                  queryable: :owners,
                  field: :owner,
                  owner_key: :tenant_id,
                  related_key: :code,
                  cardinality: :many
                }
              }
            },
            schemas: %{
              owners: %{
                source_table: "owners",
                primary_key: :id,
                fields: [:id, :code],
                redact_fields: [],
                columns: %{id: %{type: :integer}, code: %{type: :string}},
                associations: %{}
              }
            },
            joins: %{owner: %{type: :left, name: "owner"}}
          }
          |> Selecto.configure(conn, adapter: Adapter, validate: false)
          |> Selecto.with_tenant(7)
          |> Selecto.select(["id", "owner.id"])

        assert rows(query) == [[1, 1]]
      end)
    end
  end

  defp scope_tables!(conn) do
    ddl!(conn, "CREATE TABLE hs (id INT PRIMARY KEY, org_key VARCHAR(16), name VARCHAR(20))")

    ddl!(
      conn,
      "INSERT INTO hs VALUES (1, 't1', 'a alpha'), (2, 'T1', 'b alpha'), (3, 't1 ', 'c alpha'), (4, 'tı', 'd alpha')"
    )

    ddl!(
      conn,
      "CREATE TABLE hs_site (id INT PRIMARY KEY, tenant_id INT NOT NULL, site VARCHAR(16), name VARCHAR(20))"
    )

    ddl!(
      conn,
      "INSERT INTO hs_site VALUES (1, 1, 's1', 'a alpha'), (2, 1, 'S1', 'b alpha'), (3, 1, 's1 ', 'c alpha')"
    )
  end

  defp hs_domain do
    %{
      name: "HS",
      source: relation("hs", [id: :integer, org_key: :string, name: :string], nil),
      schemas: %{},
      joins: %{}
    }
  end

  defp site_domain do
    %{
      name: "Site",
      source:
        relation(
          "hs_site",
          [id: :integer, tenant_id: :integer, site: :string, name: :string],
          :tenant_id
        ),
      schemas: %{},
      joins: %{}
    }
  end

  defp relation(table, columns, tenant) do
    %{
      source_table: table,
      primary_key: :id,
      fields: Keyword.keys(columns),
      redact_fields: [],
      columns: Map.new(columns, fn {name, type} -> {name, %{type: type}} end),
      associations: %{}
    }
    |> then(&if(tenant, do: Map.put(&1, :tenant_field, tenant), else: &1))
  end

  defp ids(query), do: query |> rows() |> Enum.map(&hd/1) |> Enum.sort()

  defp rows(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_hostscope_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    ddl!(admin, "CREATE DATABASE `#{database}`")

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
