defmodule SelectoDBMariaDB.DialectTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
  alias Selecto.Dialect.Json.{ArrayContains, Contains, Operation}
  alias Selecto.Dialect.Predicate.Comparison
  alias SelectoDBMariaDB.Dialect

  test "renders date formatting and case-insensitive matching in MariaDB syntax" do
    datetime = %DateTimeOperation{
      operation: :format,
      clause: :select,
      expression: "`created_at`",
      options: %{format: "YYYY-MM", epoch_storage: nil}
    }

    comparison = %Comparison{
      operation: :case_insensitive_like,
      left: "`title`",
      right: {:param, "%office%"}
    }

    assert {:ok, formatted} = Dialect.render_datetime_operation(datetime, %{})
    assert IO.iodata_to_binary(formatted) == "DATE_FORMAT(`created_at`, '%Y-%m')"
    assert {:ok, compared} = Dialect.render_comparison(comparison, %{})
    assert compared == ["LOWER(", "`title`", ") ", "LIKE", " LOWER(", {:param, "%office%"}, ")"]
  end

  test "inlines JSON strings as sql_mode-independent hexadecimal literals" do
    hostile = "it's \\"

    renders = [
      Dialect.render_json_operation(json_op(:json_build_array, value: [hostile, 1]), %{}),
      Dialect.render_json_operation(
        json_op(:json_build_object, value: [{hostile, hostile}]),
        %{}
      ),
      Dialect.render_json_operation(
        json_op(:json_set, column: "doc", path: "$.a", value: %{"k" => hostile}),
        %{}
      ),
      Dialect.render_json_contains(%Contains{column: "doc", value: %{"k" => hostile}}, %{}),
      Dialect.render_json_array_contains(
        %ArrayContains{column: "doc", path: ["tags"], value: hostile},
        %{}
      )
    ]

    for {:ok, iodata} <- renders do
      sql = IO.iodata_to_binary(iodata)
      refute sql =~ "it"
      refute sql =~ "\\"
      assert sql =~ "_utf8mb4 X'"
    end

    assert {:ok, array} =
             Dialect.render_json_operation(json_op(:json_build_array, value: ["a\\", ""]), %{})

    assert IO.iodata_to_binary(array) == "JSON_ARRAY(_utf8mb4 X'615C', _utf8mb4 X'')"

    assert_raise ArgumentError, fn ->
      Dialect.render_json_operation(json_op(:json_build_array, value: [<<0xFF>>]), %{})
    end
  end

  test "rejects unimplemented timezone conversion explicitly" do
    datetime = %DateTimeOperation{
      operation: :format,
      clause: :select,
      expression: "`created_at`",
      options: %{format: "YYYY", timezone: "America/Denver"}
    }

    assert {:error, %Selecto.Error{details: %{unsupported_feature: :datetime_operation}}} =
             Dialect.render_datetime_operation(datetime, %{})
  end

  defp json_op(kind, fields), do: struct!(Operation, [operation: kind, clause: :select] ++ fields)
end
