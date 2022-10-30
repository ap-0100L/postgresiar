defmodule PostgresiarTest do
  use ExUnit.Case
  doctest Postgresiar

  test "greets the world" do
    assert Postgresiar.ping() == :pong
  end
end
