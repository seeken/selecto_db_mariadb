defmodule SelectoDBMariaDB.ComputedShorthandTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.ComputedValue
  alias SelectoDBMariaDB.Adapter, as: Adapter
  alias SelectoDBMariaDB.Dialect, as: Dialect

  defp sql(iodata), do: iodata |> List.wrap() |> List.flatten() |> Enum.map_join(&part/1)
  defp part({:param, value}), do: "<#{value}>"
  defp part(part), do: to_string(part)

  test "days_between is DATEDIFF of the two dates" do
    fragment = %ComputedValue{
      operation: :days_between,
      expression: ["selecto_root.purchased_at", {:param, "2026-06-30"}],
      type: "integer"
    }

    assert {:ok, rendered} = Dialect.render_computed_value(fragment, %{})

    assert sql(rendered) ==
             "DATEDIFF(CAST(<2026-06-30> AS DATE), CAST(selecto_root.purchased_at AS DATE))"

    for invalid <- [
          %{fragment | expression: ["a"]},
          %{fragment | expression: ["a", ""]},
          %{fragment | type: nil}
        ] do
      assert {:error, _} = Dialect.render_computed_value(invalid, %{})
    end
  end

  @tag :requires_db
  @tag :mariadb
  test "temporal_delta and bucket columns run end to end" do
    {:ok, admin} = Adapter.connect(connection_options())

    database =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_cs_" <>
        "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{database}`", [], query_type: :text)

    try do
      {:ok, conn} = Adapter.connect(Keyword.put(connection_options(), :database, database))

      try do
        for statement <- [
              "CREATE TABLE events (id INT PRIMARY KEY, starts_on DATE NULL)",
              "CREATE TABLE tickets (id INT PRIMARY KEY, event_id INT NULL, purchased_at DATETIME NULL)",
              "INSERT INTO events VALUES (1, '2026-06-30'), (2, NULL)",
              "INSERT INTO tickets VALUES (1, 1, '2026-06-30 23:59:00'), (2, 1, '2026-06-24 00:00:01'), (5, 1, '2026-07-02 09:00:00'), (6, 2, '2026-03-01 09:00:00'), (10, NULL, '2026-06-01 00:00:00')"
            ],
            do: MyXQL.query!(conn, statement, [], query_type: :text)

        query =
          domain()
          |> Selecto.configure(conn, adapter: Adapter)
          |> Selecto.select(["id", "lead_days", "lead_bucket"])
          |> Selecto.order_by("id")

        assert {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)

        assert rows == [
                 [1, 0, "soon"],
                 [2, 6, "soon"],
                 [5, -2, "after"],
                 [6, nil, "Unknown"],
                 [10, nil, "Unknown"]
               ]
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

  defp domain do
    %{
      name: "Tickets",
      source: %{
        source_table: "tickets",
        primary_key: :id,
        fields: [:id, :event_id, :purchased_at, :lead_days, :lead_bucket],
        columns: %{
          id: %{type: :integer},
          event_id: %{type: :integer},
          purchased_at: %{type: :naive_datetime},
          lead_days: %{
            type: :integer,
            computed: %{kind: :temporal_delta, from: "purchased_at", to: "event.starts_on"}
          },
          lead_bucket: %{
            type: :string,
            computed: %{
              kind: :bucket,
              field: "lead_days",
              otherwise: "Unknown",
              buckets: [%{label: "after", max: -1}, %{label: "soon", min: 0, max: 6}]
            }
          }
        },
        associations: %{
          event: %{queryable: :event, field: :event, owner_key: :event_id, related_key: :id}
        },
        redact_fields: []
      },
      schemas: %{
        event: %{
          source_table: "events",
          primary_key: :id,
          fields: [:id, :starts_on],
          columns: %{id: %{type: :integer}, starts_on: %{type: :date}},
          associations: %{},
          redact_fields: []
        }
      },
      joins: %{event: %{type: :left}}
    }
  end
end
