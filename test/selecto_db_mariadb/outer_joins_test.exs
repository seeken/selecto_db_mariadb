defmodule SelectoDBMariaDB.OuterJoinsTest do
  use ExUnit.Case, async: true

  alias SelectoDBMariaDB.Adapter

  test "MariaDB has RIGHT JOIN and no FULL JOIN" do
    assert Adapter.supports?(:right_join)
    refute Adapter.supports?(:full_join)
  end
end
