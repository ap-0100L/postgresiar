defmodule Postgresiar do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  Documentation for `Postgresiar`.
  """

  ##############################################################################
  @doc """
  Hello world.

  ## Examples

      iex> Postgresiar.ping()
      :world

  """
  def ping() do
    :pong
  end

  ##############################################################################
  @doc """

  """
  def info!() do
    {:ok, api_core_info} = ApiCore.info!()
    {:ok, libcluster_config} = Utils.get_app_all_env!(:libcluster)
    {:ok, producer_info} = Producer.info!()
    #

    {:ok,
      %{
        api_core_info: api_core_info,
        libcluster_config: libcluster_config,
        producer_info: producer_info,
        #

      }}
  end

  ##############################################################################
  ##############################################################################
end
