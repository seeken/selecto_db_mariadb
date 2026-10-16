defmodule SelectoDBMariaDB.StoredValues do
  @moduledoc false

  # MariaDB can hold temporal values PostgreSQL cannot: zero dates
  # ('0000-00-00', which MyXQL returns as :zero_date / :zero_datetime), zero
  # months or days ('2024-00-10', returned as invalid Date structs) and year 0,
  # all stored while sql_mode was not strict. Such a value would crash any
  # later formatting or encoding, so a read that returns one fails as a typed
  # query error instead (SQLSTATE 22008, category datetime_field_overflow),
  # naming no value.

  @doc false
  def check(rows) when is_list(rows) do
    if Enum.any?(rows, &invalid_row?/1), do: out_of_range(), else: :ok
  end

  def check(_rows), do: :ok

  defp invalid_row?(row) when is_list(row), do: Enum.any?(row, &invalid?/1)
  defp invalid_row?(_row), do: false

  defp invalid?(value) when value in [:zero_date, :zero_datetime], do: true
  defp invalid?(%Date{year: year, month: month, day: day}), do: not valid_date?(year, month, day)

  defp invalid?(%NaiveDateTime{year: year, month: month, day: day}),
    do: not valid_date?(year, month, day)

  defp invalid?(%DateTime{year: year, month: month, day: day}),
    do: not valid_date?(year, month, day)

  defp invalid?(_value), do: false

  defp valid_date?(year, month, day),
    do: year in 1..9999 and Calendar.ISO.valid_date?(year, month, day)

  defp out_of_range do
    {:error,
     Selecto.Error.query_error(
       "MariaDB returned a temporal value outside the supported range",
       nil,
       [],
       %{
         adapter: :mariadb,
         category: :datetime_field_overflow,
         sqlstate: "22008"
       }
     )}
  end
end
