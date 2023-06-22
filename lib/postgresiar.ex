defmodule Postgresiar do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  Documentation for `Postgresiar`.
  """

  use Utils

  @supervisor_name Postgresiar.Repos.Supervisor

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
  @doc """
  list = [
    {repo_module, conf, id}
  ]
  """
  def build_child_spec_list!(list)
      when not is_list(list),
      do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["list cannot be nil; list must be a list"])

  def build_child_spec_list!(list) do
    Enum.reduce(
      list,
      [],
      fn {id, repo_module, conf} = _item, accum ->
        item = Supervisor.child_spec({repo_module, conf}, id: id)

        accum ++ [item]
      end
    )
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
  ##############################################################################
end
