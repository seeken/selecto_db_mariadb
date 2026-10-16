defmodule SelectoDBMariaDB.TenantExactCompareIntegrationTest do
  # Tenant and site identity compares by the stored bytes. Under MariaDB's
  # default collation (utf8mb4_uca1400_ai_ci, PAD SPACE) tenant 't1' also
  # matches 'T1' and 't1 '; under utf8mb4_uca1400_nopad_ai_ci it matches 'T1'.
  # Every tenant comparison here must touch only the exact 't1' rows, as on
  # PostgreSQL. The tables keep the server DEFAULT collation unless a case
  # names another one; nothing forces utf8mb4_bin.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter
  alias Selecto.Write.{Command, Error}

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  # nil keeps the server default collation; the others name a COLLATE or a
  # CHARACTER SET for the tenant columns. utf16 and latin1 columns are
  # converted to utf8mb4 before their bytes are compared.
  @collations [
    nil,
    "COLLATE utf8mb4_uca1400_nopad_ai_ci",
    "COLLATE utf8mb4_general_ci",
    "COLLATE utf8mb4_unicode_ci",
    "CHARACTER SET utf16",
    "CHARACTER SET latin1"
  ]

  # The rows of tenant 't1'; the others belong to 'T1', 't1 ' and 'tı' (latin1: 'tx').
  @t1_orders [10, 14]
  @t1_notes [100]

  for collation <- @collations do
    describe "tenant columns #{collation || "with the server default collation"}" do
      @describetag collation: collation

      test "an engine-tenant read returns only the exact tenant's rows", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          query = conn |> configure() |> Selecto.with_tenant("t1") |> Selecto.select(["id"])

          assert ids(query) == @t1_orders
        end)
      end

      test "a required tenant predicate and an IN list compare exactly", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          required =
            conn
            |> configure()
            |> Selecto.Tenant.require_tenant_filter("tenant_id", "t1")
            |> Selecto.select(["id"])

          assert ids(required) == @t1_orders

          listed =
            conn
            |> configure()
            |> Selecto.filter([{"tenant_id", {:in, ["t1", "t9"]}}])
            |> Selecto.select(["id"])

          assert ids(listed) == @t1_orders

          excluded =
            conn
            |> configure()
            |> Selecto.filter([{"tenant_id", {"!=", "t1"}}])
            |> Selecto.select(["id"])

          assert ids(excluded) == [11, 12, 13]
        end)
      end

      test "a join through a tenant-declaring relation pairs tenants exactly", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          query =
            conn
            |> configure()
            |> Selecto.with_tenant("t1")
            |> Selecto.select(["id", "customer.name"])

          # Order 10 references customer 2, which belongs to 'T1'.
          assert rows(query) == [[10, nil], [14, "t1 customer"]]
        end)
      end

      test "a tenant relation reached across a shared hop carries the exact tenant", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          query =
            conn
            |> configure()
            |> Selecto.with_tenant("t1")
            |> Selecto.select(["id", "bin.notes.id"])
            |> Selecto.filter([{"bin.notes.id", :not_null}])

          assert rows(query) |> Enum.map(&List.last/1) |> Enum.uniq() == @t1_notes
        end)
      end

      test "a collection subselect pairs tenants exactly", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          query =
            conn
            |> configure(%{})
            |> Selecto.with_tenant("t1")
            |> Selecto.select(["id"])
            |> Selecto.subselect([
              %{
                fields: ["name"],
                target_schema: :customers,
                format: :count,
                alias: "customers",
                join_path: [:customer]
              }
            ])

          assert rows(query) == [[10, 0], [14, 1]]
        end)
      end

      test "a retarget onto a tenant relation binds the exact tenant", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          query =
            conn
            |> configure()
            |> Selecto.with_tenant("t1")
            |> Selecto.retarget(:customer)
            |> Selecto.select(["name"])

          assert rows(query) |> List.flatten() |> Enum.sort() == ["t1 customer"]
        end)
      end

      test "update and delete predicates on the tenant column touch only its rows", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          update =
            command(:update,
              assignments: [%{field: "total", value: {:literal, 99}}],
              predicate: {:eq, {:field, "tenant_id"}, {:literal, "t1"}},
              expected_cardinality: :many
            )

          assert {:ok, %{affected_rows: 2}} = Adapter.execute_write_unsafe(conn, update, [])
          assert column(conn, "SELECT id FROM orders WHERE total = 99 ORDER BY id") == @t1_orders

          listed =
            command(:update,
              assignments: [%{field: "total", value: {:literal, 98}}],
              predicate: {:in, {:field, "tenant_id"}, [{:literal, "t1"}, {:literal, "t9"}]},
              expected_cardinality: :many
            )

          assert {:ok, %{affected_rows: 2}} = Adapter.execute_write_unsafe(conn, listed, [])
          assert column(conn, "SELECT id FROM orders WHERE total = 98 ORDER BY id") == @t1_orders

          delete =
            command(:delete,
              predicate: {:eq, {:field, "tenant_id"}, {:literal, "t1"}},
              expected_cardinality: :many
            )

          assert {:ok, %{affected_rows: 2}} = Adapter.execute_write_unsafe(conn, delete, [])
          assert column(conn, "SELECT id FROM orders ORDER BY id") == [11, 12, 13]
        end)
      end

      test "a guarded insert re-reads the stored row under the exact tenant", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          insert = fn id, tenant ->
            command(:insert,
              assignments: [
                %{field: "id", value: {:literal, id}},
                %{field: "tenant_id", value: {:literal, tenant}},
                %{field: "total", value: {:literal, 1}}
              ],
              metadata: %{
                insert_admission: %{
                  storage_backend: :mariadb,
                  primary_key: "id",
                  predicate: {:eq, {:field, "tenant_id"}, {:literal, "t1"}}
                }
              }
            )
          end

          assert {:ok, %{affected_rows: 1}} =
                   Adapter.execute_write_unsafe(conn, insert.(20, "t1"), [])

          for {id, tenant} <- [{21, "T1"}, {22, "t1 "}] do
            assert {:error, %Error{details: %{code: :query_rule_violation}}} =
                     Adapter.execute_write_unsafe(conn, insert.(id, tenant), [])
          end

          assert column(conn, "SELECT id FROM orders WHERE id >= 20") == [20]
        end)
      end

      test "a reference guard admits only a parent of the exact tenant", ctx do
        with_tenant_tables(ctx.collation, fn conn ->
          insert = fn id, customer_id ->
            command(:insert,
              assignments: [
                %{field: "id", value: {:literal, id}},
                %{field: "tenant_id", value: {:literal, "t1"}},
                %{field: "customer_id", value: {:literal, customer_id}},
                %{field: "total", value: {:literal, 1}}
              ],
              metadata: %{
                foreign_key_guards: [
                  %{
                    field: "customer_id",
                    relation: "customers",
                    target_field: "id",
                    tenant_field: "tenant_id",
                    tenant_value: "t1"
                  }
                ]
              }
            )
          end

          assert {:ok, %{affected_rows: 1}} =
                   Adapter.execute_write_unsafe(conn, insert.(30, 1), [])

          # Customers 2 and 3 belong to 'T1' and 't1 '.
          for {id, customer_id} <- [{31, 2}, {32, 3}] do
            assert {:error, %Error{type: :cardinality_mismatch}} =
                     Adapter.execute_write_unsafe(conn, insert.(id, customer_id), [])
          end

          assert column(conn, "SELECT id FROM orders WHERE id >= 30") == [30]
        end)
      end
    end
  end

  describe "host-installed predicates" do
    # A host scope decides tenant identity even on a domain without
    # tenant_field (org_key) and for a site pin beside an integer tenant.
    test "required filters and trusted-context filters compare exactly at any depth" do
      with_database(fn conn ->
        ddl!(conn, "CREATE TABLE hs (id INT PRIMARY KEY, org_key VARCHAR(16), name VARCHAR(20))")

        ddl!(
          conn,
          "INSERT INTO hs VALUES (1, 't1', 'a'), (2, 'T1', 'b'), (3, 't1 ', 'c'), (4, 'tı', 'd')"
        )

        ddl!(
          conn,
          "CREATE TABLE hs_site (id INT PRIMARY KEY, tenant_id INT NOT NULL, site VARCHAR(16), name VARCHAR(20))"
        )

        ddl!(
          conn,
          "INSERT INTO hs_site VALUES (1, 1, 's1', 'a'), (2, 1, 'S1', 'b'), (3, 1, 's1 ', 'c')"
        )

        relation = fn table, columns, tenant ->
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

        hs = %{
          name: "HS",
          source: relation.("hs", [id: :integer, org_key: :string, name: :string], nil),
          schemas: %{},
          joins: %{}
        }

        site = %{
          name: "Site",
          source:
            relation.(
              "hs_site",
              [id: :integer, tenant_id: :integer, site: :string, name: :string],
              :tenant_id
            ),
          schemas: %{},
          joins: %{}
        }

        for {domain, column, value, bind} <- [
              {hs, "org_key", "t1", & &1},
              {site, "site", "s1", &Selecto.with_tenant(&1, 1)}
            ] do
          configure = fn domain ->
            domain |> Selecto.configure(conn, adapter: Adapter, validate: false) |> bind.()
          end

          # Any depth of the host predicate tree: =, <>, IN, NOT IN under
          # AND/OR/NOT.
          shapes = [
            {column, value},
            {:or, [{column, value}, {column, "zz"}]},
            {:and, [{"id", {:gte, 0}}, {:or, [{column, value}, {column, "zz"}]}]},
            {:or, [{column, value}, {"id", {:lt, 0}}]},
            {:not, {column, {"!=", value}}},
            {:not, {column, {:not_in, [value]}}}
          ]

          for shape <- shapes,
              {label, query} <- [
                {"domain required_filters",
                 configure.(Map.put(domain, :required_filters, [shape]))},
                {"require_tenant_filter",
                 configure.(domain) |> Selecto.Tenant.require_tenant_filter(shape)},
                {"trusted context",
                 domain
                 |> Selecto.configure(conn, adapter: Adapter, validate: false)
                 |> Selecto.with_tenant(
                   if(column == "site",
                     do: %{tenant_id: 1, required_filters: [shape]},
                     else: %{required_filters: [shape]}
                   )
                 )}
              ] do
            assert ids(Selecto.select(query, ["id"])) == [1],
                   "#{column} #{label} #{inspect(shape)}"
          end
        end
      end)
    end
  end

  describe "host predicates on non-text columns" do
    # Declared temporal columns keep the plain comparison, as on PostgreSQL:
    # their text form ('...10:00:00.000') differs from the bound value.
    test "eq, <> and NOT IN on TIME(6), TIMESTAMP(3) and DATE match PostgreSQL" do
      with_database(fn conn ->
        ddl!(
          conn,
          "CREATE TABLE tq (id INT PRIMARY KEY, at TIMESTAMP(3) NULL, d DATE NULL, " <>
            "tu TIME(6) NULL, name VARCHAR(20))"
        )

        ddl!(
          conn,
          "INSERT INTO tq VALUES (1, '2024-01-01 10:00:00', '2024-01-01', '10:00:00', 'a'), " <>
            "(2, '2024-01-01 10:00:00', '2024-01-01', '10:00:00', 'b'), " <>
            "(3, '2024-01-01 10:00:00.5', '2024-01-02', '10:00:00.5', 'c')"
        )

        domain = %{
          name: "TQ",
          source: %{
            source_table: "tq",
            primary_key: :id,
            fields: [:id, :at, :d, :tu, :name],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              at: %{type: :naive_datetime},
              d: %{type: :date},
              tu: %{type: :time_usec},
              name: %{type: :string}
            },
            associations: %{}
          },
          schemas: %{},
          joins: %{}
        }

        for {filter, expected} <- [
              {{"tu", ~T[10:00:00]}, [1, 2]},
              {{"tu", "10:00:00"}, [1, 2]},
              {{"d", ~D[2024-01-01]}, [1, 2]},
              {{"d", {"!=", ~D[2024-01-01]}}, [3]},
              {{"d", {:not_in, [~D[2024-01-01]]}}, [3]},
              {{"at", ~N[2024-01-01 10:00:00]}, [1, 2]},
              {{"at", {"!=", ~N[2024-01-01 10:00:00]}}, [3]},
              {{"at", {:not_in, [~N[2024-01-01 10:00:00]]}}, [3]}
            ] do
          query =
            domain
            |> Map.put(:required_filters, [filter])
            |> Selecto.configure(conn, adapter: Adapter, validate: false)
            |> Selecto.select(["id"])

          {sql, _params} = Selecto.to_sql(query)
          refute sql =~ "BINARY", inspect(filter)
          assert ids(query) == expected, inspect(filter)
        end
      end)
    end
  end

  describe "type classification" do
    defp classified_ids(conn, table, columns, filter) do
      %{
        name: "C",
        source: %{
          source_table: table,
          primary_key: :id,
          fields: Keyword.keys(columns),
          redact_fields: [],
          # Selecto requires a :type key; an undeclared column carries nil.
          columns:
            Map.new(columns, fn
              {name, :undeclared} -> {name, %{type: nil}}
              {name, type} -> {name, %{type: type}}
            end),
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        required_filters: [filter]
      }
      |> Selecto.configure(conn, adapter: Adapter, validate: false)
      |> Selecto.select(["id"])
      |> ids()
    end

    test "undeclared DATETIME(3) and DECIMAL columns are server-decided, as on PostgreSQL" do
      with_database(fn conn ->
        ddl!(
          conn,
          "CREATE TABLE ud (id INT PRIMARY KEY, at DATETIME(3), amt DECIMAL(10,2), org_key VARCHAR(16))"
        )

        ddl!(
          conn,
          "INSERT INTO ud VALUES (1, '2024-01-01 10:00:00', 10.00, 't1'), " <>
            "(2, '2024-01-01 10:00:00', 10.00, 'T1'), (3, '2024-01-01 10:00:00.5', 10.50, 't1 ')"
        )

        columns = [id: :integer, at: :undeclared, amt: :undeclared, org_key: :undeclared]

        for {filter, expected} <- [
              {{"at", "2024-01-01 10:00:00"}, [1, 2]},
              {{"at", {"!=", "2024-01-01 10:00:00"}}, [3]},
              {{"at", {:not_in, ["2024-01-01 10:00:00"]}}, [3]},
              {{"amt", "10"}, [1, 2]},
              {{"amt", {"!=", "10"}}, [3]},
              {{"amt", {:not_in, ["10"]}}, [3]},
              {{"org_key", "t1"}, [1]},
              {{"org_key", {"!=", "t1"}}, [2, 3]}
            ] do
          assert classified_ids(conn, "ud", columns, filter) == expected, inspect(filter)
        end

        # A write whose command carries no field types is server-decided too.
        for {predicate, expected} <- [
              {{:neq, {:field, "amt"}, {:literal, "10"}}, [3]},
              {{:not, {:in, {:field, "at"}, [{:literal, "2024-01-01 10:00:00"}]}}, [3]},
              {{:eq, {:field, "org_key"}, {:literal, "t1"}}, [1]}
            ] do
          ddl!(conn, "UPDATE ud SET amt = amt")

          update = %Command{
            operation: :update,
            relation: "ud",
            assignments: [%{field: "org_key", value: {:field, "org_key"}}],
            predicate: predicate,
            returning: :none,
            expected_cardinality: :many,
            metadata: %{tenant_fields: ["amt", "at", "org_key"]}
          }

          assert {:ok, %{affected_rows: count}} = Adapter.execute_write_unsafe(conn, update, [])
          assert count == length(expected), inspect(predicate)
        end
      end)
    end

    test "custom names that merely contain char or text are guarded, as on PostgreSQL" do
      with_database(fn conn ->
        ddl!(
          conn,
          "CREATE TABLE cn (id INT PRIMARY KEY, amt DECIMAL(10,2), ts DATETIME(3), name VARCHAR(8))"
        )

        ddl!(
          conn,
          "INSERT INTO cn VALUES (1, 10.00, '2024-01-01 10:00:00', 'a'), " <>
            "(2, 10.00, '2024-01-01 10:00:00', 'b'), (3, 10.50, '2024-01-01 10:00:00.5', 'c')"
        )

        columns = [id: :integer, amt: "charge_amount", ts: "context_ts", name: :string]

        for {filter, expected} <- [
              {{"amt", "10"}, [1, 2]},
              {{"amt", {"!=", "10"}}, [3]},
              {{"amt", {:not_in, ["10"]}}, [3]},
              {{"ts", "2024-01-01 10:00:00"}, [1, 2]},
              {{"ts", {"!=", "2024-01-01 10:00:00"}}, [3]},
              {{"ts", {:not_in, ["2024-01-01 10:00:00"]}}, [3]}
            ] do
          assert classified_ids(conn, "cn", columns, filter) == expected, inspect(filter)
        end

        for {predicate, expected} <- [
              {{:eq, {:field, "amt"}, {:literal, "10"}}, 2},
              {{:neq, {:field, "amt"}, {:literal, "10"}}, 1},
              {{:not, {:in, {:field, "amt"}, [{:literal, "10"}]}}, 1},
              {{:eq, {:field, "ts"}, {:literal, "2024-01-01 10:00:00"}}, 2},
              {{:neq, {:field, "ts"}, {:literal, "2024-01-01 10:00:00"}}, 1},
              {{:not, {:in, {:field, "ts"}, [{:literal, "2024-01-01 10:00:00"}]}}, 1}
            ] do
          update = %Command{
            operation: :update,
            relation: "cn",
            assignments: [%{field: "name", value: {:field, "name"}}],
            predicate: predicate,
            returning: :none,
            expected_cardinality: :many,
            metadata: %{
              tenant_fields: ["amt", "ts"],
              field_types: %{"amt" => "charge_amount", "ts" => "context_ts"}
            }
          }

          assert {:ok, %{affected_rows: ^expected}} =
                   Adapter.execute_write_unsafe(conn, update, []),
                 inspect(predicate)
        end
      end)
    end

    test "parameterised, synonym and custom text types compare exactly" do
      with_database(fn conn ->
        ddl!(conn, "CREATE TABLE tt (id INT PRIMARY KEY, org_key VARCHAR(16))")
        ddl!(conn, "INSERT INTO tt VALUES (1, 't1'), (2, 'T1'), (3, 't1 ')")

        for type <- [
              "varchar(255)",
              "VARCHAR(16)",
              "char(16)",
              "character varying",
              "char varying",
              "national varchar",
              "citext",
              "varchar(16) character set utf8mb4 collate utf8mb4_0900_ai_ci",
              :"Elixir.MyApp.TenantKey",
              {:parameterized, {Ecto.Enum, %{}}}
            ] do
          assert classified_ids(conn, "tt", [id: :integer, org_key: type], {"org_key", "t1"}) ==
                   [1],
                 inspect(type)
        end
      end)
    end
  end

  describe "UUID tenant columns" do
    @uuid "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

    test "stored as text (VARCHAR), writes touch only the exact UUID's rows" do
      with_database(fn conn ->
        ddl!(
          conn,
          "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id VARCHAR(40) NOT NULL, total INT) ENGINE=InnoDB"
        )

        ddl!(
          conn,
          "INSERT INTO orders VALUES (1, '#{@uuid}', 1), (2, '#{String.upcase(@uuid)}', 1), " <>
            "(3, '#{@uuid} ', 1), (4, '#{@uuid}', 1)"
        )

        update =
          command(:update,
            assignments: [%{field: "total", value: {:literal, 9}}],
            predicate: {:eq, {:field, "tenant_id"}, {:literal, @uuid}},
            expected_cardinality: :many,
            field_types: %{"tenant_id" => :uuid}
          )

        # A UUID column is server-decided: text storage by its bytes.
        {:ok, %{text: text}} = SelectoDBMariaDB.WriteCompiler.compile(update, [])

        assert text =~
                 "(COLLATION(`tenant_id`) = 'binary' OR CAST(CONVERT(`tenant_id` USING utf8mb4) AS BINARY) = CAST(? AS BINARY))"

        assert {:ok, %{affected_rows: 2}} = Adapter.execute_write_unsafe(conn, update, [])
        assert column(conn, "SELECT id FROM orders WHERE total = 9 ORDER BY id") == [1, 4]

        delete =
          command(:delete,
            predicate: {:eq, {:field, "tenant_id"}, {:literal, @uuid}},
            expected_cardinality: :many,
            field_types: %{"tenant_id" => :uuid}
          )

        assert {:ok, %{affected_rows: 2}} = Adapter.execute_write_unsafe(conn, delete, [])
        assert column(conn, "SELECT id FROM orders ORDER BY id") == [2, 3]
      end)
    end

    test "stored as text, reads bind the string form and match the exact UUID" do
      with_database(fn conn ->
        ddl!(
          conn,
          "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id VARCHAR(40) NOT NULL, " <>
            "customer_id INT, bin_id INT, total INT) ENGINE=InnoDB"
        )

        ddl!(
          conn,
          "INSERT INTO orders VALUES (1, '#{@uuid}', NULL, NULL, 1), " <>
            "(2, '#{String.upcase(@uuid)}', NULL, NULL, 1), (3, '#{@uuid} ', NULL, NULL, 1), " <>
            "(4, '#{@uuid}', NULL, NULL, 1)"
        )

        domain = put_in(domain(), [:source, :columns, :tenant_id], %{type: :uuid})

        query =
          Selecto.configure(domain, conn, adapter: Adapter, validate: false)
          |> Selecto.with_tenant(@uuid)
          |> Selecto.select(["id"])

        {sql, params} = Selecto.to_sql(query)

        assert sql =~
                 "(COLLATION(selecto_root.tenant_id) = 'binary' OR CAST(CONVERT(selecto_root.tenant_id USING utf8mb4) AS BINARY) = CAST(? AS BINARY))"

        assert params == [@uuid, @uuid]
        assert ids(query) == [1, 4]
      end)
    end
  end

  describe "integer tenant columns" do
    test "keep the plain comparison in reads and writes" do
      domain = put_in(domain(), [:source, :columns, :tenant_id], %{type: :integer})

      domain =
        Enum.reduce([:customers, :notes], domain, fn schema, acc ->
          put_in(acc, [:schemas, schema, :columns, :tenant_id], %{type: :integer})
        end)

      context = Selecto.Runtime.Context.new(Adapter, :compile_only)

      {sql, _params} =
        domain
        |> Selecto.configure(context)
        |> Selecto.with_tenant(7)
        |> Selecto.select(["id", "customer.name"])
        |> Selecto.to_sql()

      refute sql =~ "BINARY"

      {:ok, %{text: text}} =
        SelectoDBMariaDB.WriteCompiler.compile(
          command(:update,
            assignments: [%{field: "total", value: {:literal, 1}}],
            predicate: {:eq, {:field, "tenant_id"}, {:literal, 7}},
            field_types: %{"tenant_id" => :integer}
          ),
          []
        )

      assert text == "UPDATE `orders` SET `total` = ? WHERE `tenant_id` = ?"
    end
  end

  defp configure(conn, joins \\ nil) do
    domain = if joins, do: Map.put(domain(), :joins, joins), else: domain()
    Selecto.configure(domain, conn, adapter: Adapter, validate: false)
  end

  defp relation(table, columns, associations, tenant_field) do
    %{
      source_table: table,
      primary_key: :id,
      fields: Keyword.keys(columns),
      redact_fields: [],
      columns: Map.new(columns, fn {name, type} -> {name, %{type: type}} end),
      associations: associations,
      tenant_field: tenant_field
    }
  end

  defp domain do
    %{
      name: "Orders",
      source:
        relation(
          "orders",
          [
            id: :integer,
            tenant_id: :string,
            customer_id: :integer,
            bin_id: :integer,
            total: :integer
          ],
          %{
            customer: %{
              queryable: :customers,
              field: :customer,
              owner_key: :customer_id,
              related_key: :id,
              cardinality: :one
            },
            bin: %{
              queryable: :bins,
              field: :bin,
              owner_key: :bin_id,
              related_key: :id,
              cardinality: :one
            }
          },
          :tenant_id
        ),
      schemas: %{
        customers:
          relation(
            "customers",
            [id: :integer, tenant_id: :string, name: :string],
            %{},
            :tenant_id
          ),
        bins:
          relation(
            "bins",
            [id: :integer, label: :string],
            %{
              notes: %{
                queryable: :notes,
                field: :notes,
                owner_key: :id,
                related_key: :bin_id,
                cardinality: :many
              }
            },
            nil
          ),
        notes:
          relation(
            "notes",
            [id: :integer, tenant_id: :string, bin_id: :integer, body: :string],
            %{},
            :tenant_id
          )
      },
      joins: %{
        customer: %{type: :left, name: "customer"},
        bin: %{type: :left, name: "bin", joins: %{notes: %{type: :left, name: "notes"}}}
      }
    }
  end

  defp command(operation, opts) do
    metadata =
      Map.merge(
        %{
          tenant_fields: ["tenant_id"],
          field_types: Keyword.get(opts, :field_types, %{"tenant_id" => :string})
        },
        Keyword.get(opts, :metadata, %{})
      )

    %Command{
      operation: operation,
      relation: "orders",
      assignments: Keyword.get(opts, :assignments, []),
      predicate: Keyword.get(opts, :predicate),
      returning: :none,
      expected_cardinality: Keyword.get(opts, :expected_cardinality, {:exactly, 1}),
      metadata: metadata
    }
  end

  defp ids(query), do: query |> rows() |> Enum.map(&hd/1) |> Enum.sort()

  defp rows(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  defp column(conn, sql) do
    {:ok, %{rows: rows}} = Adapter.execute(conn, sql, [], [])
    Enum.map(rows, &hd/1)
  end

  defp with_tenant_tables(collation, fun) do
    with_database(fn conn ->
      text = if collation, do: "VARCHAR(20) #{collation}", else: "VARCHAR(20)"
      # latin1 cannot store the dotless i.
      other = if collation == "CHARACTER SET latin1", do: "tx", else: "tı"

      for statement <- [
            "CREATE TABLE customers (id INT PRIMARY KEY, tenant_id #{text} NOT NULL, name VARCHAR(50)) ENGINE=InnoDB",
            "CREATE TABLE bins (id INT PRIMARY KEY, label VARCHAR(20)) ENGINE=InnoDB",
            "CREATE TABLE notes (id INT PRIMARY KEY, tenant_id #{text} NOT NULL, bin_id INT, body VARCHAR(50)) ENGINE=InnoDB",
            "CREATE TABLE orders (id INT PRIMARY KEY, tenant_id #{text} NOT NULL, customer_id INT, bin_id INT, total INT) ENGINE=InnoDB",
            "INSERT INTO customers VALUES (1, 't1', 't1 customer'), (2, 'T1', 'T1 customer'), (3, 't1 ', 'padded customer'), (4, '#{other}', 'dotless customer')",
            "INSERT INTO bins VALUES (1, 'shared')",
            "INSERT INTO notes VALUES (100, 't1', 1, 't1 note'), (101, 'T1', 1, 'T1 note'), (102, 't1 ', 1, 'padded note'), (103, '#{other}', 1, 'dotless note')",
            "INSERT INTO orders VALUES (10, 't1', 2, 1, 1), (11, 'T1', 2, 1, 1), (12, 't1 ', 3, 1, 1), (13, '#{other}', 4, 1, 1), (14, 't1', 1, 1, 1)"
          ],
          do: ddl!(conn, statement)

      fun.(conn)
    end)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_exact_" <>
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
