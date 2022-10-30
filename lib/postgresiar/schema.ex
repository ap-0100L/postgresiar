defmodule Postgresiar.Schema do
  ##############################################################################
  ##############################################################################
  @moduledoc """

  """

  use Utils

  ##############################################################################
  @doc """

  """
  @callback insert_changeset(model :: any, params :: any) :: term
  @callback update_changeset(model :: any, params :: any) :: term

  @optional_callbacks insert_changeset: 2, update_changeset: 2

  ##############################################################################
  @doc """

  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @repo Keyword.fetch!(opts, :repo)
      @readonly_repo Keyword.get(opts, :readonly_repo, @repo)

      use Utils
      require Postgresiar.Schema

      alias Utils, as: Utils
      alias Postgresiar.Schema, as: PostgresiarSchema

      alias __MODULE__, as: SelfModule

      @behaviour PostgresiarSchema

      ##############################################################################
      @doc """

      """
      def exec_query!(query, params \\ [], opts \\ [])

      def exec_query!(query, params, opts) do
        @readonly_repo.exec_query!(query, params, opts)
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def get_by_query!(query, opts \\ [])

      def get_by_query!(query, opts) do
        @readonly_repo.get_by_query!(query, opts)
      end

      ###########################################################################
      @doc """
      Insert
      """
      def insert!(obj, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def insert!(obj, async, rescue_func, rescue_func_args, module) do
        changeset = SelfModule.insert_changeset(%SelfModule{}, obj)

        if async do
          @repo.insert_record_async(changeset, rescue_func, rescue_func_args, module)
        else
          @repo.insert_record!(changeset)
        end
      end

      ###########################################################################
      @doc """
      Update
      """
      def update!(obj, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def update!(obj, async, rescue_func, rescue_func_args, module) do
        changeset = SelfModule.update_changeset(%SelfModule{id: obj.id}, obj)

        if async do
          @repo.update_record_async(changeset, rescue_func, rescue_func_args, module)
        else
          @repo.update_record!(changeset)
        end
      end

      defoverridable PostgresiarSchema
    end
  end

  ##############################################################################
  ##############################################################################
end
