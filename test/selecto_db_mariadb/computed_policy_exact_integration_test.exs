defmodule SelectoDBMariaDB.ComputedPolicyExactIntegrationTest do
  # Host scope through a computed predicate and through association `where`
  # policies. Under the server's default collation 't1' also matches 'T1'
  # and 't1 ', and 's1' matches 'S1' and 's1 '. A required filter on a
  # computed predicate, a policy on a join, a collection, a through bridge
  # and a retarget onto a where-confined association must each read as on
  # PostgreSQL.
  use ExUnit.Case, async: false

  alias SelectoDBMariaDB.Adapter

  @moduletag :requires_db
  @moduletag :mariadb
  @moduletag timeout: 120_000

  test "a required filter on a computed predicate compares its body exactly" do
    with_database(fn conn ->
      ddl!(conn, "CREATE TABLE hs (id INT PRIMARY KEY, org_key VARCHAR(16), name VARCHAR(20))")
      ddl!(conn, "INSERT INTO hs VALUES (1,'t1','a'),(2,'T1','b'),(3,'t1 ','c'),(4,'tı','d')")

      for expression <- [
            ["eq", "org_key", "t1"],
            ["or", [["eq", "org_key", "t1"], ["eq", "org_key", "zz"]]],
            ["not", ["neq", "org_key", "t1"]],
            ["in", "org_key", ["t1", "zz"]]
          ] do
        query =
          %{
            name: "HS",
            source: %{
              source_table: "hs",
              primary_key: :id,
              fields: [:id, :org_key, :name, :is_t1],
              redact_fields: [],
              columns: %{
                id: %{type: :integer},
                org_key: %{type: :string},
                name: %{type: :string},
                is_t1: %{type: :boolean, computed: %{kind: :predicate, expression: expression}}
              },
              associations: %{}
            },
            schemas: %{},
            joins: %{},
            required_filters: [{"is_t1", true}]
          }
          |> Selecto.configure(conn, adapter: Adapter, validate: false)
          |> Selecto.select(["id"])

        assert ids(query) == [1], inspect(expression)
      end
    end)
  end

  test "association where policies confine joins, collections, bridges and retargets exactly" do
    with_database(fn conn ->
      ddl!(
        conn,
        "CREATE TABLE wp_items (id INT PRIMARY KEY, tenant_key VARCHAR(16), account_id INT, name VARCHAR(20))"
      )

      ddl!(conn, "CREATE TABLE wp_acc (id INT NOT NULL, site VARCHAR(16), label VARCHAR(40))")
      ddl!(conn, "CREATE TABLE wp_bridge (item_id INT, acc_id INT, site VARCHAR(16))")
      ddl!(conn, "INSERT INTO wp_items VALUES (1, 't7', 101, 'own')")

      ddl!(
        conn,
        "INSERT INTO wp_acc VALUES (101,'s1','mine'),(101,'s2','SECRET-s2'),(101,'S1','SECRET-S1')," <>
          "(101,'s1 ','SECRET-pad'),(102,'s1','SECRET-viaS1bridge'),(103,'s1','SECRET-viapadbridge')"
      )

      ddl!(conn, "INSERT INTO wp_bridge VALUES (1,101,'s1'),(1,102,'S1'),(1,103,'s1 ')")

      for site_type <- [:string, "site_code"], bind <- [&Selecto.with_tenant(&1, "t7"), & &1] do
        base =
          site_type
          |> wp_domain()
          |> Selecto.configure(conn, adapter: Adapter, validate: false)
          |> bind.()

        label = inspect(site_type)

        assert rows(Selecto.select(base, ["id", "a.label"])) == [[1, "mine"]], label

        collection =
          base
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

        assert [[1, labels]] = rows(collection)
        assert decode(labels) == ["mine"], label

        for strategy <- [:in, :exists] do
          retarget =
            base |> Selecto.retarget("a", strategy: strategy) |> Selecto.select(["id", "label"])

          assert rows(retarget) == [[101, "mine"]], "#{label} #{strategy}"

          through = base |> Selecto.retarget("th", strategy: strategy) |> Selecto.select(["id"])

          assert through |> rows() |> Enum.map(&hd/1) |> Enum.uniq() == [101],
                 "#{label} #{strategy}"
        end

        assert base
               |> Selecto.select(["id", "th.id"])
               |> rows()
               |> Enum.map(&List.last/1)
               |> Enum.uniq() ==
                 [101],
               label
      end
    end)
  end

  test "a host predicate on a computed value or a joined computed predicate compares exactly" do
    with_database(fn conn ->
      ddl!(
        conn,
        "CREATE TABLE cb_root (id INT PRIMARY KEY, org_key VARCHAR(16), acc_id INT, name VARCHAR(10))"
      )

      ddl!(conn, "CREATE TABLE cb_acc (id INT PRIMARY KEY, site VARCHAR(16), label VARCHAR(20))")

      ddl!(
        conn,
        "INSERT INTO cb_root VALUES (1,'t1',1,'a'),(2,'T1',2,'b'),(3,'t1 ',3,'c'),(4,'zz',1,'d')"
      )

      ddl!(
        conn,
        "INSERT INTO cb_acc VALUES (1,'s1','mine'),(2,'S1','SECRET-S1'),(3,'s1 ','SECRET-pad')"
      )

      for field <- ["flag", "flag_in", "flag_nested"] do
        query =
          cb_domain([{field, "y"}])
          |> Selecto.configure(conn, adapter: Adapter, validate: false)
          |> Selecto.select(["id"])

        assert ids(query) == [1], field
      end

      joined =
        cb_domain([{"a.is_s1", true}])
        |> Selecto.configure(conn, adapter: Adapter, validate: false)
        |> Selecto.select(["id", "a.label"])

      assert rows(joined) == [[1, "mine"], [4, "mine"]]
    end)
  end

  defp cb_domain(required) do
    case_of = fn condition ->
      ["case", [condition, ["literal", "y"]], ["else", ["literal", "n"]]]
    end

    %{
      name: "CB",
      source: %{
        source_table: "cb_root",
        primary_key: :id,
        fields: [:id, :org_key, :acc_id, :name, :is_t1, :flag, :flag_in, :flag_nested],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          org_key: %{type: :string},
          acc_id: %{type: :integer},
          name: %{type: :string},
          is_t1: %{
            type: :boolean,
            computed: %{kind: :predicate, expression: ["eq", "org_key", "t1"]}
          },
          flag: %{
            type: :string,
            computed: %{kind: :expression, expression: case_of.(["eq", "org_key", "t1"])}
          },
          flag_in: %{
            type: :string,
            computed: %{kind: :expression, expression: case_of.(["in", "org_key", ["t1", "q"]])}
          },
          flag_nested: %{
            type: :string,
            computed: %{
              kind: :expression,
              expression: ["coalesce", case_of.(["eq", "is_t1", true]), ["literal", "n"]]
            }
          }
        },
        associations: %{
          a: %{
            queryable: :acc,
            field: :a,
            owner_key: :acc_id,
            related_key: :id,
            cardinality: :one
          }
        }
      },
      schemas: %{
        acc: %{
          source_table: "cb_acc",
          primary_key: :id,
          fields: [:id, :site, :label, :is_s1],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            site: %{type: :string},
            label: %{type: :string},
            is_s1: %{
              type: :boolean,
              computed: %{kind: :predicate, expression: ["eq", "site", "s1"]}
            }
          },
          associations: %{}
        }
      },
      joins: %{a: %{type: :left, name: "a"}},
      required_filters: required
    }
  end

  defp wp_domain(site_type) do
    %{
      name: "WP",
      source: %{
        source_table: "wp_items",
        primary_key: :id,
        fields: [:id, :tenant_key, :account_id, :name],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          tenant_key: %{type: :string},
          account_id: %{type: :integer},
          name: %{type: :string}
        },
        tenant_field: :tenant_key,
        associations: %{
          a: %{
            queryable: :acc,
            field: :a,
            owner_key: :account_id,
            related_key: :id,
            cardinality: :one,
            where: %{site: "s1"}
          },
          th: %{
            queryable: :acc,
            field: :th,
            owner_key: :id,
            related_key: :id,
            cardinality: :many,
            through: %{
              table: "wp_bridge",
              owner_key: :item_id,
              related_key: :acc_id,
              where: %{site: "s1"}
            }
          }
        }
      },
      schemas: %{
        acc: %{
          source_table: "wp_acc",
          primary_key: :id,
          fields: [:id, :site, :label],
          redact_fields: [],
          columns: %{id: %{type: :integer}, site: %{type: site_type}, label: %{type: :string}},
          associations: %{}
        }
      },
      joins: %{a: %{type: :left, name: "a"}, th: %{type: :left, name: "th"}}
    }
  end

  defp decode(value) when is_binary(value), do: Jason.decode!(value)
  defp decode(value), do: value

  defp ids(query), do: query |> rows() |> Enum.map(&hd/1) |> Enum.sort()

  defp rows(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    Enum.sort(rows)
  end

  defp with_database(fun) do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_cmppol_" <>
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
