defmodule Postgresiar.Application do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  ## Module
  """
  use Application
  use Utils

  alias Postgresiar, as: PostgresiarWorker

  @supervisor_name Postgresiar.Supervisor

  ####################################################################################################################
  @doc """
  ### get_opts.
  """
  def get_opts do
    result = [
      strategy: :one_for_one,
      name: @supervisor_name
    ]

    {:ok, result}
  end

  ####################################################################################################################
  @doc """
  ### get_children
  """
  defp get_children do
    {:ok, repos} = Postgresiar.build_child_spec_list()

    result =
      repos ++
        [
          {PostgresiarWorker, strategy: :one_for_one, restart: :permanent, name: PostgresiarWorker.get_genserver_name()}
        ]

    {:ok, result}
  end

  ####################################################################################################################
  @doc """
  ### Start application.
  """
  @impl true
  def start(_type, _args) do
    {:ok, children} = get_children()
    {:ok, opts} = get_opts()

    Supervisor.start_link(children, opts)
  end

  ####################################################################################################################
  ####################################################################################################################
end
