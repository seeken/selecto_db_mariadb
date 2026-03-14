defmodule SelectoDBMariaDB.AdapterTest do
  use ExUnit.Case, async: true

  test "adapter exposes the selecto adapter contract" do
    assert Code.ensure_loaded?(SelectoDBMariaDB.Adapter)
    assert function_exported?(SelectoDBMariaDB.Adapter, :name, 0)
    assert function_exported?(SelectoDBMariaDB.Adapter, :connect, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :execute, 4)
    assert function_exported?(SelectoDBMariaDB.Adapter, :placeholder, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :quote_identifier, 1)
    assert function_exported?(SelectoDBMariaDB.Adapter, :supports?, 1)
  end

  test "mariadb adapter reports expected placeholder and quoting strategy" do
    assert SelectoDBMariaDB.Adapter.placeholder(3) == "?"
    assert SelectoDBMariaDB.Adapter.quote_identifier("order") == "`order`"
  end

  test "mariadb adapter rejects invalid connection options" do
    assert SelectoDBMariaDB.Adapter.connect(123) == {:error, {:invalid_connection_options, 123}}
  end

  test "mariadb adapter returns a dependency or connection result" do
    result = SelectoDBMariaDB.Adapter.connect([])

    if Code.ensure_loaded?(MyXQL) do
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    else
      assert result == {:error, {:adapter_dependency_missing, :myxql}}
    end
  end

  test "mariadb adapter does not claim stream support" do
    refute SelectoDBMariaDB.Adapter.supports?(:stream)
  end

  test "mariadb adapter reports rollup support" do
    assert SelectoDBMariaDB.Adapter.supports?(:rollup)
  end
end
