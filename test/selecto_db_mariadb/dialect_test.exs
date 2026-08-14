defmodule SelectoDBMariaDB.DialectTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.DateTime.Operation, as: DateTimeOperation
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
end
