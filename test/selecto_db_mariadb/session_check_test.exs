defmodule SelectoDBMariaDB.SessionCheckTest do
  use ExUnit.Case, async: true

  alias SelectoDBMariaDB.Session

  @strict "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
  @utf8 ["utf8mb4", "utf8mb4", "utf8mb4"]

  defp row(zone, system, charsets \\ @utf8, sql_mode \\ @strict),
    do: [zone, system] ++ charsets ++ [sql_mode]

  defp refused(row) do
    case Session.check(row) do
      {:error, %Selecto.Error{type: :connection_error, details: details}} ->
        assert details.category == :invalid_session
        details.setting

      :ok ->
        :ok
    end
  end

  test "UTC session zones, and SYSTEM on a UTC server, are accepted" do
    for zone <- ["UTC", "+00:00", "Etc/UTC"], do: assert(refused(row(zone, "CET")) == :ok)
    for system <- ["UTC", "+00:00", "Etc/UTC"], do: assert(refused(row("SYSTEM", system)) == :ok)
    assert refused(row("+00:00", "UTC", @utf8, "STRICT_ALL_TABLES")) == :ok
  end

  test "GMT and other zones are refused, also behind SYSTEM" do
    for zone <- ["GMT", "+01:00", "-00:30", "Europe/London", "SYSTEM", "", nil] do
      assert refused(row(zone, "GMT")) == :time_zone
    end

    for system <- ["GMT", "BST", "CET", "EST", nil] do
      assert refused(row("SYSTEM", system)) == :time_zone
    end
  end

  test "every connection character set must be utf8mb4" do
    for charsets <- [
          ["latin1", "utf8mb4", "utf8mb4"],
          ["utf8mb4", "utf8mb3", "utf8mb4"],
          ["utf8mb4", "utf8mb4", nil],
          ["utf8mb4", "utf8mb4", "binary"]
        ] do
      assert refused(row("UTC", "UTC", charsets)) == :character_set
    end
  end

  test "sql_mode must be strict" do
    for mode <- ["", "NO_ENGINE_SUBSTITUTION", "STRICT", "TRADITIONAL_LIKE", nil] do
      assert refused(row("UTC", "UTC", @utf8, mode)) == :sql_mode
    end
  end

  test "an unexpected row shape is refused" do
    assert refused(["UTC"]) == :session
  end
end
