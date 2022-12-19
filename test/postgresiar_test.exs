defmodule PostgresiarTest do
  use ExUnit.Case
  doctest Postgresiar

  test "greets the pong" do
    assert Postgresiar.ping() == :pong
  end
end
