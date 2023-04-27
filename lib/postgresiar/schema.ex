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

      ###########################################################################
      @doc """
      ### Macros
      """
      defmacro where_clause(logical_operator, query, binding \\ [], expr)

      defmacro where_clause(logical_operator, query, binding, expr) do
        Builder.Filter.build(:where, logical_operator, query, binding, expr, __CALLER__)
      end

      ###########################################################################
      @doc """
      ### Function
      """
      def simple_where_filter!(query, logical_operator, filters)
          when is_nil(query) or logical_operator not in @logical_operators or not is_list(filters),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query, logical_operator, filters can not be nil; filters must be a list; logical_operator must be one of #{inspect(@logical_operators)}"]
            )

      def simple_where_filter!(query, logical_operator, filters) do
        query =
          Enum.reduce(
            filters,
            query,
            fn filter, accum ->
              case filter do
                {name, op, val} ->
                  case op do
                    :gte ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) >= ^val)

                    :gt ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) > ^val)

                    :lt ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) < ^val)

                    :lte ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) <= ^val)

                    :eq ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) == ^val)

                    :ilike ->
                      where_clause(logical_operator, accum, [..., o], ilike(type(field(o, ^name), :string), ^val))

                    :like ->
                      where_clause(logical_operator, accum, [..., o], like(type(field(o, ^name), :string), ^val))

                    :between ->
                      {from, to} = val
                      # {:ok, from, _} = DateTime.from_iso8601(from)
                      # {:ok, to, _} = DateTime.from_iso8601(to)

                      where_clause(logical_operator, accum, [..., o], fragment("? between ? and ?", field(o, ^name), ^from, ^to))

                    :between_dates ->
                      {from, to} = val
                      {:ok, from, _} = DateTime.from_iso8601(from)
                      {:ok, to, _} = DateTime.from_iso8601(to)

                      where_clause(logical_operator, accum, [..., o], fragment("? between ? and ?", field(o, ^name), ^from, ^to))

                    :in ->
                      where_clause(logical_operator, accum, [..., o], field(o, ^name) in ^val)

                    _ ->
                      UniError.raise_error!(
                        :CODE_UNKNOWN_OPERATOR_IN_WHERE_CLAUSE_ERROR,
                        ["Unknown operator in where clause"],
                        op: op
                      )
                  end

                _ ->
                  {:ok, accum} = simple_where_filter!(accum, filter)

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
      def simple_where_filter!(query, filters)
          when is_nil(query) or (not is_nil(filters) and not is_map(filters) and not is_list(filters)),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query can not be nil; filters must be nil or a map or a list"],
              query: query,
              filters: filters
            )

      def simple_where_filter!(query, nil) do
        query
      end

      def simple_where_filter!(query, filters)
          when is_list(filters) do
        simple_where_filter!(query, :and, filters)
      end

      def simple_where_filter!(query, %{or: filters} = _f) do
        {:ok, query} = simple_where_filter!(query, :or, filters)
      end

      def simple_where_filter!(query, %{and: filters}) do
        simple_where_filter!(query, :and, filters)
      end

      def simple_where_filter!(_query, filters),
        do:
          UniError.raise_error!(
            :CODE_WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            filters: filters
          )

      ###########################################################################
      @doc """
      ### Function
      """
      def find_filters!(filters, field_opts)
          when (not is_map(filters) and not is_list(filters)) or (not is_atom(field_opts) and not is_tuple(field_opts)),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["filters, field_opts, accum, operation can not be nil; filters must a map or a list; field_opts must be an atom or a tuple"],
              filters: filters,
              field_opts: field_opts
            )

      def find_filters!(filters, field_opts) do
        change_filters!(filters, field_opts, [], :find)
      end

      def replace_filters!(filters, field_opts) do
        change_filters!(filters, field_opts, [], :replace)
      end

      def delete_filters!(filters, field_opts) do
        change_filters!(filters, field_opts, [], :delete)
      end

      ###########################################################################
      @doc """
      ### Function
      """
      def change_filters!(filters, field_opts, accum, operation)
          when (not is_map(filters) and not is_list(filters)) or not is_list(accum) or (not is_atom(field_opts) and not is_tuple(field_opts)) or operation not in @filter_operations,
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["filters, field_opts, accum, operation can not be nil; filters must a map or a list; accum must be a list; field_opts must be an atom or a tuple; operation must be one of #{inspect(@filter_operations)}"],
              filters: filters,
              field_opts: field_opts,
              accum: accum,
              operation: operation
            )

      ###########################################################################
      # Find
      def change_filters!(filters, field_opts, accum, :find)
          when is_list(filters) do
        result =
          Enum.reduce(
            filters,
            accum,
            fn filter, accum2 ->
              case filter do
                {name, op, val} ->
                  if name == field_opts do
                    accum2 ++ [filter]
                  else
                    accum2
                  end

                _ ->
                  {:ok, accum2} = change_filters!(filter, field_opts, accum2, :find)

                  accum2
              end
            end
          )

        {:ok, result}
      end

      def change_filters!(%{and: filters} = _f, field_opts, accum, :find) do
        change_filters!(filters, field_opts, accum, :find)
      end

      def change_filters!(%{or: filters} = _f, field_opts, accum, :find) do
        change_filters!(filters, field_opts, accum, :find)
      end

      ###########################################################################
      # replace
      def change_filters!(filters, {field_name, _op, _val} = field_opts, _accum, :replace)
          when is_list(filters) do
        IO.inspect(:list, label: "in list")

        result =
          Enum.reduce(
            filters,
            [],
            fn filter, accum2 ->
              case filter do
                {name, _op, _val} ->
                  IO.inspect(filter, label: "filter")

                  if name == field_name do
                    accum2 ++ [field_opts]
                  else
                    accum2 ++ [filter]
                  end

                _ ->
                  {:ok, filters} = change_filters!(filter, field_opts, accum2, :replace)

                  filters =
                    if is_list(filters) do
                      filters
                    else
                      [filters]
                    end

                  accum2 ++ filters
              end
            end
          )

        {:ok, result}
      end

      def change_filters!(%{and: filters} = _f, field_opts, accum, :replace) do
        IO.inspect(:and, label: "in and")

        {:ok, filters} = change_filters!(filters, field_opts, accum, :replace)

        {:ok, %{and: filters}}
      end

      def change_filters!(%{or: filters} = _f, field_opts, accum, :replace) do
        IO.inspect(:or, label: "in or")

        {:ok, filters} = change_filters!(filters, field_opts, accum, :replace)

        {:ok, %{or: filters}}
      end

      ###########################################################################
      # delete
      def change_filters!(filters, field_opts, _accum, :delete)
          when is_list(filters) and is_atom(field_opts) do
        IO.inspect(:list, label: "in list")

        result =
          Enum.reduce(
            filters,
            [],
            fn filter, accum2 ->
              case filter do
                {name, _op, _val} ->
                  IO.inspect(filter, label: "filter")

                  if name == field_opts do
                    accum2
                  else
                    accum2 ++ [filter]
                  end

                _ ->
                  {:ok, filters} = change_filters!(filter, field_opts, accum2, :delete)

                  filters =
                    if is_list(filters) do
                      filters
                    else
                      [filters]
                    end

                  accum2 ++ filters
              end
            end
          )

        {:ok, result}
      end

      def change_filters!(%{and: filters} = _f, field_opts, accum, :delete) do
        IO.inspect(:and, label: "in and")

        {:ok, filters} = change_filters!(filters, field_opts, accum, :delete)

        {:ok, %{and: filters}}
      end

      def change_filters!(%{or: filters} = _f, field_opts, accum, :delete) do
        IO.inspect(:or, label: "in or")

        {:ok, filters} = change_filters!(filters, field_opts, accum, :delete)

        {:ok, %{or: filters}}
      end

      def change_filters!(filters, field_opts, _accum, operation),
          do:
            UniError.raise_error!(
              :CODE_WRONG_ARGUMENT_COMBINATION_ERROR,
              ["Wrong argument combination"],
              filters: filters,
              field_opts: field_opts,
              operation: operation
            )

      ###########################################################################
      @doc """
      ### Function
      """
      def paginate!(query, page, per_page_count)
          when is_nil(query) or ((not is_nil(page) and not is_integer(page)) or page < 0) or ((not is_nil(per_page_count) and not is_integer(per_page_count)) or per_page_count <= 0),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query can not be nil; page, per_page_count if not nil must be an integer; page must be greater then 0; per_page_count must be equal or greater then 0"]
            )

      def paginate!(query, page, per_page_count) do
        query =
          if is_nil(page) or is_nil(per_page_count) do
            query
          else
            offset = page * per_page_count

            from(query,
              limit: ^per_page_count,
              offset: ^offset
            )
          end

        {:ok, query}
      end

      ###########################################################################
      @doc """
      ### Function
      """
      def order_by!(query, orders)
          when is_nil(query) or not is_list(orders),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["query, orders can not be nil; orders must be a list"]
            )

      def order_by!(query, orders) do
        query = order_by(query, [..., o], ^orders)

        {:ok, query}
      end

      defoverridable PostgresiarSchema
    end
  end

  ##############################################################################
  ##############################################################################
end
