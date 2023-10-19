defmodule Postgresiar do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  Documentation for `Postgresiar`.
  """

  use GenServer
  use Utils

  alias Postgresiar.Schema, as: PostgresiarSchema

  @genserver_name Postgresiar.Worker

  ##############################################################################
  @doc """
  ## Function
  """
  def get_genserver_name() do
    @genserver_name
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def build_child_spec_list() do
    {:ok, repos} = get_app_env(:repos)

    result =
      Enum.reduce(
        repos,
        [],
        fn {
             {repo_rw, rw_opts},
             {repo_ro, ro_opts}
           } = _item,
           accum ->
          item = Supervisor.child_spec({repo_rw, rw_opts[:child_opts] || []}, id: repo_rw)
          accum = accum ++ [item]

          item = Supervisor.child_spec({repo_ro, ro_opts[:child_opts] || []}, id: repo_ro)
          accum = accum ++ [item]

          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Repo RW added to children [#{inspect(repo_rw)}]")
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Repo RO added to children [#{inspect(repo_ro)}]")

          accum
        end
      )
    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  @impl true
  def init(state) do
    UniError.rescue_error!(
      (
        Utils.ensure_all_started!([:inets, :ssl])

        PostgresiarSchema.create_tables(:api_core, "Elixir.ApiCore.Db.Persistent.Dao")

        Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Postgresiar started successfully")
      )
    )

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I completed init part")
    {:ok, state}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def info!() do
    # {:ok, api_core_info} = ApiCore.info!()
    #

    {:ok,
     %{
       # api_core_info: api_core_info,
       #
     }}
  end

  ##############################################################################
  @doc """
  Hello pong.

  #### Examples

      iex> Postgresiar.ping()
      :pong

  """
  def ping() do
    :pong
  end

  ##############################################################################
  ##############################################################################
end
