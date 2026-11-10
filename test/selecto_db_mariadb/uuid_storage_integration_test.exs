defmodule SelectoDBMariaDB.UuidStorageIntegrationTest do
  @moduledoc """
  A :uuid field over BINARY(16) storage (as Ecto's MyXQL adapter keeps UUIDs)
  reads and writes its 16 raw bytes as given; over CHAR(36) storage, text.
  """
  use ExUnit.Case, async: false

  @moduletag :requires_db
  @moduletag :mariadb

  @adapter SelectoDBMariaDB.Adapter

  defp opts do
    [
      hostname: System.get_env("SELECTO_MARIADB_HOST", "127.0.0.1"),
      port: System.get_env("SELECTO_MARIADB_PORT", "3306") |> String.to_integer(),
      username: System.get_env("SELECTO_MARIADB_USER", "root"),
      password: System.fetch_env!("SELECTO_MARIADB_PASSWORD")
    ]
  end

  @u "550e8400-e29b-41d4-a716-446655440000"

  setup_all do
    {:ok, admin} = MyXQL.start_link(opts())

    db =
      "#{System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")}_uuidst_" <>
        "#{System.unique_integer([:positive])}"

    MyXQL.query!(admin, "CREATE DATABASE `#{db}`", [], query_type: :text)
    {:ok, raw} = MyXQL.start_link(Keyword.put(opts(), :database, db))

    MyXQL.query!(
      raw,
      "CREATE TABLE ux (id INT PRIMARY KEY, ub BINARY(16), ut CHAR(36))",
      [],
      query_type: :text
    )

    {:ok, conn} = @adapter.connect(Keyword.put(opts(), :database, db))

    on_exit(fn ->
      {:ok, a} = MyXQL.start_link(opts())
      MyXQL.query!(a, "DROP DATABASE IF EXISTS `#{db}`", [], query_type: :text)
    end)

    %{conn: conn, raw: raw}
  end

  defp domain do
    %{
      name: "ux",
      schemas: %{},
      joins: %{},
      source: %{
        source_table: "ux",
        primary_key: :id,
        fields: [:id, :ub, :ut],
        redact_fields: [],
        columns: %{id: %{type: :integer}, ub: %{type: :uuid}, ut: %{type: :uuid}},
        associations: %{}
      }
    }
  end

  test "16 raw bytes of a :uuid field read and write as given", ctx do
    raw16 = UUID.string_to_binary!(@u)

    {:ok, insert} =
      Selecto.Write.Command.new(%{
        operation: :insert,
        relation: :ux,
        assignments: [
          %{field: :id, value: {:literal, 1}},
          %{field: :ub, value: {:literal, raw16}},
          %{field: :ut, value: {:literal, @u}}
        ],
        metadata: %{field_types: %{id: :integer, ub: :uuid, ut: :uuid}}
      })

    assert {:ok, _} = @adapter.execute_write_unsafe(ctx.conn, insert, [])

    assert %MyXQL.Result{rows: [[^raw16, @u]]} =
             MyXQL.query!(ctx.raw, "SELECT ub, ut FROM ux WHERE id = 1", [])

    read = fn filter ->
      {:ok, {rows, _, _}} =
        Selecto.configure(domain(), ctx.conn, adapter: @adapter, validate: false)
        |> Selecto.filter(filter)
        |> Selecto.select(["id"])
        |> Selecto.execute()

      rows
    end

    assert read.({"ub", raw16}) == [[1]]
    assert read.({"ub", {:in, [raw16]}}) == [[1]]
    assert read.({"ub", {:!=, raw16}}) == []
    assert read.({"ut", @u}) == [[1]]

    # A context value is checked as a literal is.
    {:ok, from_context} =
      Selecto.Write.Command.new(%{
        operation: :insert,
        relation: :ux,
        assignments: [
          %{field: :id, value: {:literal, 2}},
          %{field: :ut, value: {:context, :owner}}
        ],
        metadata: %{field_types: %{id: :integer, ut: :uuid}}
      })

    assert {:error, %Selecto.Write.Error{type: :invalid_value}} =
             @adapter.execute_write_unsafe(ctx.conn, from_context, context: %{owner: "abc"})

    assert {:ok, _} =
             @adapter.execute_write_unsafe(ctx.conn, from_context, context: %{owner: @u})
  end
end
