defmodule SelectoDBMariaDB.ExplorerBucketTest do
  use ExUnit.Case, async: true

  alias Selecto.Dialect.Bucket.Expression

  test "step buckets floor per row and label start-end, NULL as Other" do
    assert {:ok, sql} =
             SelectoDBMariaDB.Dialect.render_bucket(
               %Expression{kind: :numeric_increment, expression: "amount", increment: 20},
               %{}
             )

    assert IO.iodata_to_binary(sql) ==
             "CASE WHEN amount IS NULL THEN 'Other' ELSE CONCAT(CAST((FLOOR(amount / 20) * 20) AS SIGNED), '-', CAST((FLOOR(amount / 20) * 20) + 19 AS SIGNED)) END"

    assert {:ok, year} =
             SelectoDBMariaDB.Dialect.render_bucket(
               %Expression{kind: :year_increment, expression: "d", increment: 1},
               %{}
             )

    assert IO.iodata_to_binary(year) =~ "FLOOR(YEAR(d) / 1) * 1"
  end

  test "other bucket kinds stay unsupported" do
    assert {:error, %Selecto.Error{}} =
             SelectoDBMariaDB.Dialect.render_bucket(
               %Expression{kind: :numeric_ranges, expression: "q", ranges: [{1, 2, "a"}]},
               %{}
             )

    assert {:error, %Selecto.Error{}} =
             SelectoDBMariaDB.Dialect.render_bucket(
               %Expression{
                 kind: :year_increment,
                 expression: "d",
                 increment: 1,
                 temporal_options: %{timezone: "America/Denver"}
               },
               %{}
             )
  end
end
