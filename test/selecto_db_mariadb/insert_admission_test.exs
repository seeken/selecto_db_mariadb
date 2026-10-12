defmodule SelectoDBMariaDB.InsertAdmissionTest do
  use ExUnit.Case, async: true

  alias SelectoDBMariaDB.InsertAdmission
  alias Selecto.Write.Command

  defp command(assignments) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: "items",
        assignments: assignments,
        metadata: %{
          insert_admission: %{
            storage_backend: :mariadb,
            predicate: {:lt, {:field, :subject}, {:literal, "16"}},
            primary_key: "id"
          }
        }
      })

    command
  end

  defp assigned(field, value), do: %{field: field, value: {:literal, value}}

  defp executor(responses) do
    fn _conn, sql, params, _opts ->
      send(self(), {:sql, sql, params})

      Enum.find_value(responses, {:error, :unexpected}, fn {pattern, result} ->
        if String.contains?(sql, pattern), do: result
      end)
    end
  end

  defp code({:error, %{details: %{code: code}}}), do: code

  test "a non-strict session or an INSERT trigger is not evaluable" do
    command = command([assigned(:id, 1), assigned(:subject, "10.5")])

    loose = executor([{"sql_mode", {:ok, %{rows: [["NO_ENGINE_SUBSTITUTION"]]}}}])

    assert code(InsertAdmission.before_insert(:conn, command, loose, [])) ==
             :query_rule_not_evaluable

    triggered =
      executor([
        {"sql_mode", {:ok, %{rows: [["STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"]]}}},
        {"TRIGGERS", {:ok, %{rows: [[1]]}}}
      ])

    assert code(InsertAdmission.before_insert(:conn, command, triggered, [])) ==
             :query_rule_not_evaluable

    clean =
      executor([
        {"sql_mode", {:ok, %{rows: [["STRICT_TRANS_TABLES"]]}}},
        {"TRIGGERS", {:ok, %{rows: [[0]]}}}
      ])

    assert :ok = InsertAdmission.before_insert(:conn, command, clean, [])
    assert_received {:sql, _, []}
    assert_received {:sql, trigger_sql, ["items"]}
    assert trigger_sql =~ "EVENT_OBJECT_SCHEMA = DATABASE()"
  end

  test "the stored row is re-read by its key under the predicate" do
    command = command([assigned(:id, 7), assigned(:subject, "15.999")])

    rounded = executor([{"SELECT COUNT(*) FROM `items`", {:ok, %{rows: [[0]]}}}])

    assert code(InsertAdmission.after_insert(:conn, command, %{}, rounded, [])) ==
             :query_rule_violation

    assert_received {:sql, sql, [7, "16"]}
    assert sql == "SELECT COUNT(*) FROM `items` WHERE `id` = ? AND (`subject` < ?)"

    stored = executor([{"SELECT COUNT(*)", {:ok, %{rows: [[1]]}}}])
    assert :ok = InsertAdmission.after_insert(:conn, command, %{}, stored, [])
  end

  test "a generated key is used only when MySQL generated one" do
    command = command([assigned(:subject, "10")])
    stored = executor([{"SELECT COUNT(*)", {:ok, %{rows: [[1]]}}}])

    assert :ok =
             InsertAdmission.after_insert(
               :conn,
               command,
               %{metadata: %{last_insert_id: 42}},
               stored,
               []
             )

    assert_received {:sql, _, [42, "16"]}

    assert code(
             InsertAdmission.after_insert(
               :conn,
               command,
               %{metadata: %{last_insert_id: 0}},
               stored,
               []
             )
           ) == :query_rule_not_evaluable
  end

  test "strict-mode conversion failures are not evaluable" do
    command = command([assigned(:id, 1), assigned(:subject, "0x10")])
    failed = {:error, %Selecto.Error{type: :query_error, message: "x", details: %{code: 1366}}}
    assert code(InsertAdmission.storage_error(command, failed)) == :query_rule_not_evaluable

    other = {:error, %Selecto.Error{type: :query_error, message: "x", details: %{code: 1062}}}
    assert InsertAdmission.storage_error(command, other) == other
  end
end
