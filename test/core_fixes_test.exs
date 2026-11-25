defmodule CoreFixesMariadbTest do
  use ExUnit.Case, async: true
  alias SelectoDBMariaDB.Adapter, as: Adapter

  defp domain(type, extra \\ %{}) do
    %{
      name: "Clock fixes",
      source: %{
        source_table: "fix_events",
        primary_key: :id,
        fields: [:id, :at, :age],
        associations: %{},
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          at: Map.merge(%{type: type}, extra),
          age: %{
            type: :integer,
            computed: %{kind: :expression, expression: ["days_between", ["field", "at"], ["now"]]}
          }
        }
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp base(conn, domain) do
    Selecto.configure(domain, Selecto.Runtime.Context.new(Adapter, conn), adapter: Adapter)
  end

  test "period casts bind offset-free datetime text" do
    q =
      base(:compile_only, domain(:utc_datetime))
      |> Selecto.timezone("Etc/UTC")
      |> Selecto.select(["id"])
      |> Selecto.filter([Selecto.Expr.on("at", "2026-03-15")])

    {sql, _, params} = Selecto.gen_sql(q, [])
    assert sql =~ "AS DATETIME"
    assert params == ["2026-03-15 00:00:00", "2026-03-16 00:00:00"]
  end
end
