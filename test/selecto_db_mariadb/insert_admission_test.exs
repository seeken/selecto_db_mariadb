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

  # Responses are matched in order by a substring of the SQL.
  defp executor(responses) do
    fn _conn, sql, params, _opts ->
      send(self(), {:sql, sql, params})

      Enum.find_value(responses, {:error, :unexpected}, fn {pattern, result} ->
        if String.contains?(sql, pattern), do: result
      end)
    end
  end

  defp code({:error, %{details: %{code: code}}}), do: code

  defp flush_sql do
    receive do
      {:sql, _, _} -> flush_sql()
    after
      0 -> :ok
    end
  end

  @strict {"sql_mode", {:ok, %{rows: [["STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"]]}}}
  @innodb {"ENGINE", {:ok, %{rows: [["InnoDB"]]}}}
  @mdl {"LIMIT 0", {:ok, %{rows: []}}}

  defp key(type, extra), do: {"COLUMN_KEY", {:ok, %{rows: [["id", type, extra]]}}}

  test "a non-strict session or a non-transactional engine is not evaluable; triggers are re-read" do
    command = command([assigned(:id, 1), assigned(:subject, "10.5")])
    refused = :query_rule_not_evaluable

    # Without the table's metadata lock nothing is evaluable.
    assert code(InsertAdmission.before_insert(:conn, command, executor([]), [])) == refused

    loose = executor([@mdl, {"sql_mode", {:ok, %{rows: [["NO_ENGINE_SUBSTITUTION"]]}}}])
    assert code(InsertAdmission.before_insert(:conn, command, loose, [])) == refused

    myisam = executor([@mdl, @strict, {"ENGINE", {:ok, %{rows: [["MyISAM"]]}}}])
    assert code(InsertAdmission.before_insert(:conn, command, myisam, [])) == refused

    # A BEFORE INSERT trigger can rewrite the row and its key: refused.
    triggered = executor([@mdl, @strict, @innodb, {"TRIGGERS", {:ok, %{rows: [[1]]}}}])
    flush_sql()
    assert code(InsertAdmission.before_insert(:conn, command, triggered, [])) == refused
    # The metadata lock comes before every catalog read.
    assert {:sql, "SELECT 1 FROM `items` LIMIT 0", []} = receive(do: (message -> message))
    assert_received {:sql, "SELECT COUNT(*) FROM information_schema.TRIGGERS" <> sql, _}
    assert sql =~ "ACTION_TIMING = 'BEFORE'"

    clean = executor([@mdl, @strict, @innodb, {"TRIGGERS", {:ok, %{rows: [[0]]}}}])
    assert :ok = InsertAdmission.before_insert(:conn, command, clean, [])
  end

  test "the stored row is re-read by its typed key, alone and under the predicate" do
    command = command([assigned(:id, 7), assigned(:subject, "15.999")])
    guarded = "WHERE `id` = ? AND (`subject` < ?)"

    rounded =
      executor([
        key("int", ""),
        {"TRIGGERS", {:ok, %{rows: [[0]]}}},
        {guarded, {:ok, %{rows: [[0]]}}},
        {"WHERE `id` = ?", {:ok, %{rows: [[1]]}}}
      ])

    assert code(InsertAdmission.after_insert(:conn, command, %{}, rounded, [])) ==
             :query_rule_violation

    # A row trigger rewrote the stored row: the candidate is not evaluable.
    triggered =
      executor([
        key("int", ""),
        {"TRIGGERS", {:ok, %{rows: [[1]]}}},
        {guarded, {:ok, %{rows: [[0]]}}},
        {"WHERE `id` = ?", {:ok, %{rows: [[1]]}}}
      ])

    assert code(InsertAdmission.after_insert(:conn, command, %{}, triggered, [])) ==
             :query_rule_not_evaluable

    assert_received {:sql, "SELECT COUNT(*) FROM `items` WHERE `id` = ?", [7]}
    assert_received {:sql, "SELECT COUNT(*) FROM `items` " <> ^guarded, [7, "16"]}

    stored =
      executor([
        key("int", ""),
        {guarded, {:ok, %{rows: [[1]]}}},
        {"`id` = ?", {:ok, %{rows: [[1]]}}}
      ])

    assert :ok = InsertAdmission.after_insert(:conn, command, %{}, stored, [])

    # No row, or several, carry the key: never decided by the predicate.
    missing =
      executor([
        key("int", ""),
        {guarded, {:ok, %{rows: [[1]]}}},
        {"`id` = ?", {:ok, %{rows: [[0]]}}}
      ])

    assert code(InsertAdmission.after_insert(:conn, command, %{}, missing, [])) ==
             :query_rule_not_evaluable
  end

  test "LAST_INSERT_ID names the row only when the key column is AUTO_INCREMENT" do
    command = command([assigned(:subject, "10")])
    generated = %{metadata: %{last_insert_id: 42}}
    count_one = [{"`id` = ?", {:ok, %{rows: [[1]]}}}]

    auto = executor([key("bigint", "auto_increment") | count_one])
    assert :ok = InsertAdmission.after_insert(:conn, command, generated, auto, [])
    assert_received {:sql, _, [42]}

    # The repro: a UUID primary key beside an AUTO_INCREMENT sequence column.
    uuid = executor([key("varchar", "DEFAULT_GENERATED") | count_one])

    assert code(InsertAdmission.after_insert(:conn, command, generated, uuid, [])) ==
             :query_rule_not_evaluable

    assert code(
             InsertAdmission.after_insert(
               :conn,
               command,
               %{metadata: %{last_insert_id: 0}},
               auto,
               []
             )
           ) == :query_rule_not_evaluable
  end

  test "a key is compared in its column type and a composite key is refused" do
    count_one = [{"`id` = ?", {:ok, %{rows: [[1]]}}}]
    string_key = executor([key("varchar", "") | count_one])

    assert code(
             InsertAdmission.after_insert(
               :conn,
               command([assigned(:id, 101), assigned(:subject, "1")]),
               %{},
               string_key,
               []
             )
           ) == :query_rule_not_evaluable

    composite =
      executor([{"COLUMN_KEY", {:ok, %{rows: [["id", "int", ""], ["tenant_id", "int", ""]]}}}])

    assert code(
             InsertAdmission.after_insert(
               :conn,
               command([assigned(:id, 1), assigned(:subject, "1")]),
               %{},
               composite,
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
