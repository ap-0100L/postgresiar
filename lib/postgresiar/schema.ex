defmodule Postgresiar.Schema do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  ## Module
  """

  use Utils

  ####################################################################################################################
  @doc """
  ## Function
  """
  @callback insert_changeset(model :: any, params :: any) :: term
  @callback update_changeset(model :: any, params :: any) :: term

  @optional_callbacks insert_changeset: 2, update_changeset: 2

  ####################################################################################################################
  @doc """
  ## Function
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      import Ecto.Query, only: [from: 2, where: 3, limit: 3, offset: 3, order_by: 3]
      import Ecto.Query.API, only: [fragment: 1]

      @repo_modes [:RW, :RO]

      @repos Application.get_env(:postgresiar, :repos)

      use Utils
      require Postgresiar.Schema
      alias Ecto.Query.Builder

      alias Utils, as: Utils
      alias Postgresiar.Schema, as: PostgresiarSchema

      @behaviour PostgresiarSchema

      ####################################################################################################################
      @doc """
      ### Function
      """
      def select_repo_by_uuid(uuid, mode)
          when is_nil(uuid) or not is_atom(mode) or mode not in @repo_modes,
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["uuid, mode cannot be nil; mode must be one of #{@repo_modes}"])

      def select_repo_by_uuid(uuid, mode) do
        [_a, {:binary, b}, _c, _d, _e] = UUID.info!(uuid)

        <<time_low::32, time_mid::16, _uuid_v1::4, time_hi::12, _variant10::2, clock_seq_hi::6, _clock_seq_low::8, _node::48>> = b
        timestamp = <<time_hi::12, time_mid::16, time_low::32>>
        <<timestamp::60>> = timestamp

        epoch = (timestamp - 122_192_928_000_000_000) / 10
        d = trunc(epoch) |> DateTime.from_unix!(:microsecond)
        s = "#{d.year}_#{String.pad_leading("#{d.month}", 2, "0")}"

        clock_seq_hi = clock_seq_hi - trunc(clock_seq_hi / 100) * 100

        repo =
          if clock_seq_hi <= 49 do
            [repo_a, _repo_b] = @repos
            repo_a
          else
            [_repo_a, repo_b] = @repos
            repo_b
          end

        {:ok, {repo, s}}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def exec_query(query, params \\ [], opts \\ [], repo \\ @readonly_repo)

      def exec_query(query, params, opts, repo) do
        # @readonly_repo.exec_query(query, params, opts)
        apply(repo, :exec_query, [query, params, opts])
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def transaction!(fun_or_multi, opts \\ [], repo \\ @repo)

      def transaction!(fun_or_multi, opts, repo) do
        # @readonly_repo.exec_query(query, params, opts)
        apply(repo, :transaction!, [fun_or_multi, opts])
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def find_by_query(query, opts \\ [], repo \\ @readonly_repo)

      def find_by_query(query, opts, repo) do
        # @readonly_repo.find_by_query(query, opts)
        apply(repo, :find_by_query, [query, opts])
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [], repo \\ @readonly_repo)

      def preload!(struct_or_structs_or_nil, preloads, opts, repo) do
        # @readonly_repo.find_by_query(query, opts)
        apply(repo, :preload!, [struct_or_structs_or_nil, preloads, opts])
      end

      ###########################################################################
      @doc """
      Insert
      """
      def insert!(obj, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil, repo \\ @repo)

      def insert!(obj, async, rescue_func, rescue_func_args, module, repo) do

        ###########
        obj = Ecto.put_meta(obj, source: "source")

        changeset = __MODULE__.insert_changeset(%__MODULE__{}, obj)

        if async do
          # @repo.insert_record_async(changeset, rescue_func, rescue_func_args, module)
          apply(repo, :insert_record_async, [changeset, rescue_func, rescue_func_args, module])
        else
          # @repo.insert_record!(changeset)
          apply(repo, :insert_record!, [changeset])
        end
      end

      ###########################################################################
      @doc """
      Update
      """
      def update!(obj, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil, repo \\ @repo)

      def update!(obj, async, rescue_func, rescue_func_args, module, repo) do
        changeset = __MODULE__.update_changeset(%__MODULE__{id: obj.id}, obj)

        if async do
          # @repo.update_record_async(changeset, rescue_func, rescue_func_args, module)
          apply(repo, :update_record_async, [changeset, rescue_func, rescue_func_args, module])
        else
          # @repo.update_record!(changeset)
          apply(repo, :update_record!, [changeset])
        end
      end

#      ###########################################################################
#      @doc """
#      Find by id
#      """
#      def find_by_id(id, opts \\ [])
#
#      def find_by_id(id, opts) do
#        query =
#          from(
#            o in __MODULE__,
#            where: o.id == ^id,
#            limit: 1,
#            select: o
#          )
#
#        result = find_by_query(query, opts)
#
#        result
#      end
    end
  end

  ####################################################################################################################
  ####################################################################################################################
end
