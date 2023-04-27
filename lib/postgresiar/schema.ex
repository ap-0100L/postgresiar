defmodule Postgresiar.Schema do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  ## Module
  """

  use Utils

  ##############################################################################
  @doc """
  ## Function
  """
  @callback insert_changeset(model :: any, params :: any) :: term
  @callback update_changeset(model :: any, params :: any) :: term

  @optional_callbacks insert_changeset: 2, update_changeset: 2

  ##############################################################################
  @doc """
  ## Function
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      import Ecto.Query, only: [from: 2, where: 3, limit: 3, offset: 3, order_by: 3]
      import Ecto.Query.API, only: [fragment: 1]

      @filter_operations [:find, :delete, :replace]
      @logical_operators [:and, :or]

      @repo Keyword.fetch!(opts, :repo)
      @readonly_repo Keyword.get(opts, :readonly_repo, @repo)

      use Utils
      require Postgresiar.Schema
      alias Ecto.Query.Builder

      alias Utils, as: Utils
      alias Postgresiar.Schema, as: PostgresiarSchema

      alias __MODULE__, as: SelfModule

      @behaviour PostgresiarSchema

      ##############################################################################
      @doc """
      ### Function
      """
      def get_repo!() do
        if is_nil(@repo) do
          UniError.raise_error!(
            :CODE_REPO_IS_NIL_ERROR,
            ["Repo is nil"]
          )
        else
          {:ok, @repo}
        end
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def get_readonly_repo!() do
        if is_nil(@repo) do
          UniError.raise_error!(
            :CODE_READONLY_REPO_IS_NIL_ERROR,
            ["Readonly repo is nil"]
          )
        else
          {:ok, @readonly_repo}
        end
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def exec_query(query, params \\ [], opts \\ [], repo \\ @readonly_repo)

      def exec_query(query, params, opts, repo) do
        # @readonly_repo.exec_query(query, params, opts)
        apply(repo, :exec_query, [query, params, opts])
      end

      ##############################################################################
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
      def get_by_query!(query, opts \\ [], repo \\ @readonly_repo)

      def get_by_query!(query, opts, repo) do
        # @readonly_repo.get_by_query!(query, opts)
        apply(repo, :get_by_query!, [query, opts])
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [], repo \\ @readonly_repo)

      def preload!(struct_or_structs_or_nil, preloads, opts, repo) do
        # @readonly_repo.get_by_query!(query, opts)
        apply(repo, :preload!, [struct_or_structs_or_nil, preloads, opts])
      end

      ###########################################################################
      @doc """
      Insert
      """
      def insert!(obj, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil, repo \\ @repo)

      def insert!(obj, async, rescue_func, rescue_func_args, module, repo) do
        changeset = SelfModule.insert_changeset(%SelfModule{}, obj)

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
        changeset = SelfModule.update_changeset(%SelfModule{id: obj.id}, obj)

        if async do
          # @repo.update_record_async(changeset, rescue_func, rescue_func_args, module)
          apply(repo, :update_record_async, [changeset, rescue_func, rescue_func_args, module])
        else
          # @repo.update_record!(changeset)
          apply(repo, :update_record!, [changeset])
        end
      end

      defoverridable PostgresiarSchema
    end
  end

  ##############################################################################
  ##############################################################################
end
