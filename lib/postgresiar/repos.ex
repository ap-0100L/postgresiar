defmodule ApiCore.Db.Persistent.Repos do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  Persistent repository
  """

  @repos Application.get_env(:postgresiar, :repos)

  for {repo_rw, repo_ro} <- @replicas do
    defmodule repo_rw do
      use Postgresiar.DistributedRepo,
        otp_app: :my_app,
        adapter: Ecto.Adapters.Postgres,
        read_only: false
    end

    defmodule repo_ro do
      use Postgresiar.DistributedRepo,
        otp_app: :my_app,
        adapter: Ecto.Adapters.Postgres,
        read_only: true
    end
  end

  use Utils

  ####################################################################################################################
  ####################################################################################################################
end
