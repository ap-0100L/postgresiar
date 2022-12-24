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
      import Ecto.Query, only: [from: 2, where: 3, limit: 3, offset: 3]
      import Ecto.Query.API, only: [fragment: 1]

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

      """
      def exec_query!(query, params \\ [], opts \\ [], repo \\ @readonly_repo)

      def exec_query!(query, params, opts, repo) do
        # @readonly_repo.exec_query!(query, params, opts)
        apply(repo, :exec_query!, [query, params, opts])
      end

      ##############################################################################
      @doc """

      """
      def transaction!(fun_or_multi, opts \\ [], repo \\ @repo)

      def transaction!(fun_or_multi, opts, repo) do
        # @readonly_repo.exec_query!(query, params, opts)
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

      defmacro where_clause_(logical_operator, query, binding \\ [], expr)

      defmacro where_clause_(logical_operator, query, binding, expr) do
        Builder.Filter.build(:where, logical_operator, query, binding, expr, __CALLER__)
      end

      ###########################################################################
      @doc """
      ### Function
      """
      def apply_where_clause!(query, logical_operator, filters)
          when is_nil(query) or logical_operator not in @logical_operators or not is_list(filters),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query, logical_operator, filters can not be nil; filters must be a list; logical_operator must be one of #{inspect(@logical_operators)}"]
            )

      def apply_where_clause!(query, logical_operator, filters) do
        query =
          Enum.reduce(
            filters,
            query,
            fn filter, accum ->
              case filter do
                {name, op, val} ->
                  case op do
                    :gte ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) >= ^val)

                    :gt ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) > ^val)

                    :lt ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) < ^val)

                    :lte ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) <= ^val)

                    :eq ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) == ^val)

                    :ilike ->
                      where_clause_(logical_operator, accum, [o], ilike(type(field(o, ^name), :string), ^val))

                    :like ->
                      where_clause_(logical_operator, accum, [o], like(type(field(o, ^name), :string), ^val))

                    :between ->
                      {from, to} = val
                      {:ok, from, _} = DateTime.from_iso8601(from)
                      {:ok, to, _} = DateTime.from_iso8601(to)

                      where_clause_(logical_operator, accum, [o], fragment("? between ? and ?", field(o, ^name), ^from, ^to))

                    :in ->
                      where_clause_(logical_operator, accum, [o], field(o, ^name) in ^val)

                    _ ->
                      UniError.raise_error!(
                        :CODE_UNKNOWN_OPERATOR_IN_WHERE_CLAUSE_ERROR,
                        ["Unknown operator in where clause"],
                        op: op
                      )
                  end

                _ ->
                  {:ok, accum} = apply_where_clause!(accum, filter)

                  accum
              end
            end
          )

        {:ok, query}
      end

      ###########################################################################
      @doc """


      %{
        "or" => [{name, op, val}, {name, op, val}, ...],
        "and" => [{name, op, val}, {name, op, val}, ...],

        "or" => [%{"or" => [...], "and" => [...]}]
      }

      """
      def apply_where_clause!(query, filters)
          when is_nil(query) or (not is_map(filters) and not is_list(filters)),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query, filters can not be nil; filters must be a map or a list"],
              query: query,
              filters: filters
            )

      def apply_where_clause!(query, filters)
          when is_list(filters) do
        apply_where_clause!(query, :and, filters)
      end

      def apply_where_clause!(query, %{or: filters} = f) do
        {:ok, query} = apply_where_clause!(query, :or, filters)
        limit = Map.get(f, :limit, nil)
        offset = Map.get(f, :offset, nil)

        {:ok, query}
      end

      def apply_where_clause!(query, %{and: filters}) do
        apply_where_clause!(query, :and, filters)
      end

      def apply_where_clause!(_query, filters),
        do:
          UniError.raise_error!(
            :CODE_WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            filters: filters
          )

      defoverridable PostgresiarSchema
    end
  end

  ##############################################################################
  ##############################################################################
end
