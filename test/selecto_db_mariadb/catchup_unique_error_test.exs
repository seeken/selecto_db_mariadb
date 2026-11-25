defmodule SelectoDBMariadb.CatchupUniqueErrorTest do
  use ExUnit.Case, async: true

  test "uniqueness metadata names the key without retaining conflicting values" do
    native = %MyXQL.Error{
      mysql: %{code: 1062},
      message: "Duplicate entry 'secret for key 'fake'' for key 'orders.cert_owner_name'"
    }

    error = SelectoDBMariaDB.Adapter.normalize_error(native)
    assert error.details.category == :unique_violation
    assert error.details.constraint == "cert_owner_name"
    refute inspect(error) =~ "secret"
    refute inspect(error) =~ "fake"
  end
end
