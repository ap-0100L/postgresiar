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

      @is_db_distributed opts[:is_db_distributed] || false
      @is_table_distributed opts[:is_table_distributed] || false

      @sql_create_table opts[:sql_create_table] || nil

      @auto_create opts[:auto_create] || ((@is_db_distributed or @is_table_distributed) and not is_nil(@sql_create_table))

      @repo_modes [:RW, :RO]

      @repos Application.get_env(:postgresiar, :repos)

      @default_rw_repo (
                         [{{repo_rw, _}, _} | _] = @repos
                         repo_rw
                       )

      @default_ro_repo (
                         [{_, {repo_ro, _}} | _] = @repos
                         repo_ro
                       )

      @sql_find_table_by_name ~s"""
      SELECT EXISTS (
      SELECT FROM
        pg_tables
      WHERE
        schemaname = 'germes' AND
        tablename  = $1
      );
      """

      use Utils
      require Postgresiar.Schema
      alias Ecto.Query.Builder

      alias Utils, as: Utils
      alias Postgresiar.Repo, as: PostgresiarRepo
      alias Postgresiar.Schema, as: PostgresiarSchema

      @behaviour PostgresiarSchema

      ####################################################################################################################
      @doc """
      ### Function
      """
      def is_db_distributed(),
        do: @is_db_distributed

      ####################################################################################################################
      @doc """
      ### Function
      """
      def is_table_distributed(),
        do: @is_table_distributed

      ####################################################################################################################
      @doc """
      ### Function
      """
      def create_table(repo, table_name)
          when not is_atom(repo) or not is_bitstring(table_name),
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["repo, table_name cannot be nil; repo must be an atom; table_name must be a string"])

      def create_table(repo, table_name) do
        {:ok, [[table_exists]]} = apply(repo, :exec_query, [@sql_find_table_by_name, [table_name]])

        if table_exists do
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table [#{table_name}] in repo RW [#{inspect(repo)}] does exists")
        else
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table [#{table_name}] in repo RW [#{inspect(repo)}] does not exists")
          sql_create_table = String.replace(@sql_create_table, "{#}", table_name)

          sqls = String.split(sql_create_table, ";", trim: true)

          for sql <- sqls do
            {:ok, result} = apply(repo, :exec_query, [sql, []])
          end

          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table [#{table_name}] in repo RW [#{inspect(repo)}] created successfully")
        end

        {:ok, true}
      end

      def create_table(repo, table_name),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              repo: repo,
              table_name: table_name
            }
          )

      ####################################################################################################################
      @doc """
      ### Function
      """
      def create_tables() when not @auto_create do
        table_name = Ecto.get_meta(%__MODULE__{}, :source)

        Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Skip create for table [#{table_name}] in repo RW")

        {:ok, true}
      end

      def create_tables() when @auto_create do
        ids = 0..255
        table_name = Ecto.get_meta(%__MODULE__{}, :source)

        cond do
          @is_db_distributed and @is_table_distributed ->
            # All repos, many tables
            for id <- ids do
              table_name_postfix = "_#{String.pad_leading("#{id}", 3, "0")}"

              for {{repo_rw, _}, _} <- @repos do
                table_name = table_name <> table_name_postfix
                {:ok, true} = create_table(repo_rw, table_name)
              end
            end

          not @is_db_distributed and @is_table_distributed ->
            # One repo, many tables
            for id <- ids do
              table_name_postfix = "_#{String.pad_leading("#{id}", 3, "0")}"

              table_name = table_name <> table_name_postfix
              {:ok, true} = create_table(@default_rw_repo, table_name)
            end

          @is_db_distributed and not @is_table_distributed ->
            # All repos, one table
            for {{repo_rw, _}, _} <- @repos do
              {:ok, true} = create_table(repo_rw, table_name)
            end

          not @is_db_distributed and not @is_table_distributed ->
            {:ok, true} = create_table(@default_rw_repo, table_name)
        end

        {:ok, true}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def get_repo_table_name_by_uuid(id, mode)

      def get_repo_table_name_by_uuid(id, mode)
          when not is_bitstring(id) or not is_atom(mode) or mode not in @repo_modes,
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["id, mode cannot be nil; mode must be an atom; mode must be one of #{@repo_modes}"])

      def get_repo_table_name_by_uuid(id, mode) do
        table_name = Ecto.get_meta(%__MODULE__{}, :source)
        {:ok, postfix} = PostgresiarRepo.get_table_postfix_by_uuid(id)
        table_name = "#{table_name}#{postfix}"

        {:ok, repo} = PostgresiarRepo.select_repo_by_uuid(id, mode)

        {:ok, {repo, table_name}}
      end

      def get_repo_table_name_by_uuid(id, mode),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              id: id,
              mode: mode
            }
          )

      ####################################################################################################################
      @doc """
      ### Function
      """
      def prepare_model(%__MODULE__{} = model, uuid) do
        model =
          if @is_table_distributed do
            {:ok, postfix} = PostgresiarRepo.get_table_postfix_by_uuid(uuid)

            source = Ecto.get_meta(model, :source)
            model = Ecto.put_meta(model, source: "#{source}#{postfix}")

          else
            model
          end

        {:ok, model}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def exec_query(query, params \\ [], opts \\ [], repo \\ @default_ro_repo)

      def exec_query(query, params, opts, repo) do
        # @readonly_repo.exec_query(query, params, opts)
        apply(repo, :exec_query, [query, params, opts])
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def transaction!(fun_or_multi, opts \\ [], repo \\ @default_rw_repo)

      def transaction!(fun_or_multi, opts, repo) do
        # @readonly_repo.exec_query(query, params, opts)
        apply(repo, :transaction!, [fun_or_multi, opts])
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def find_by_query(query, opts \\ [], repo \\ @default_ro_repo)

      def find_by_query(query, opts, repo) do
        # @readonly_repo.find_by_query(query, opts)
        apply(repo, :find_by_query, [query, opts])
      end

      ###########################################################################
      @doc """
      Get by query
      """
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [], repo \\ @default_ro_repo)

      def preload!(struct_or_structs_or_nil, preloads, opts, repo) when is_struct(struct_or_structs_or_nil) do
        apply(repo, :preload!, [struct_or_structs_or_nil, preloads, opts])
      end

      def preload!(struct_or_structs_or_nil, preloads, opts, repo),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              struct_or_structs_or_nil: struct_or_structs_or_nil,
              preloads: preloads,
              repo: repo,
              opts: opts
            }
          )

      ###########################################################################
      @doc """
      Insert
      """
      def insert(obj, opts \\ [], async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def insert(obj, opts, async, rescue_func, rescue_func_args, module) do
        key = opts[:uuid_key] || :id

        uuid = Map.fetch!(obj, key)
        {:ok, repo} = PostgresiarRepo.select_repo_by_uuid(uuid, :RW)

        {:ok, model} = prepare_model(%__MODULE__{}, uuid)
        IO.inspect(model, label: "[QQQQQQQQQQQQQQQQQQ03][model]")

        changeset = __MODULE__.insert_changeset(model, obj)

        if async do
          # @repo.insert_record_async(changeset, rescue_func, rescue_func_args, module)
          apply(repo, :insert_record_async, [changeset, opts, rescue_func, rescue_func_args, module])
        else
          # @repo.insert_record!(changeset)
          apply(repo, :insert_record!, [changeset, opts])
        end
      end

      ###########################################################################
      @doc """
      Update
      """
      def update(obj, opts \\ [], async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def update(obj, opts, async, rescue_func, rescue_func_args, module) do
        key = opts[:uuid_key] || :id

        uuid = Map.fetch!(obj, key)
        {:ok, repo} = PostgresiarRepo.select_repo_by_uuid(uuid, :RW)

        {:ok, model} = prepare_model(%__MODULE__{id: obj.id}, uuid)

        changeset = __MODULE__.update_changeset(model, obj)

        if async do
          # @repo.update_record_async(changeset, rescue_func, rescue_func_args, module)
          apply(repo, :update_record_async, [changeset, opts, rescue_func, rescue_func_args, module])
        else
          # @repo.update_record!(changeset)
          apply(repo, :update_record!, [changeset, opts])
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
  @doc """
  ### Function
  """
  def create_tables(app_with_dbo, dbo_modules_prefix)
      when not is_atom(app_with_dbo) or not is_bitstring(dbo_modules_prefix),
      do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["app_with_dbo, dbo_modules_prefix cannot be nil; app_with_dbo must be an atom; dbo_modules_prefix must be a string"])

  def create_tables(app_with_dbo, dbo_modules_prefix) do
    {:ok, modules_list} = :application.get_key(app_with_dbo, :modules)

    dbo_modules =
      Enum.reduce(
        modules_list,
        [],
        fn module, accum ->
          if String.starts_with?("#{module}", dbo_modules_prefix) do
            accum ++ [module]
          else
            accum
          end
        end
      )

    result =
      Enum.reduce(
        dbo_modules,
        [],
        fn module, accum ->
          accum ++ [apply(module, :create_tables, [])]
        end
      )
  end

  def create_tables(app_with_dbo, dbo_modules_prefix),
    do:
      UniError.raise_error!(
        :WRONG_ARGUMENT_COMBINATION_ERROR,
        ["Wrong argument combination"],
        arguments: %{
          app_with_dbo: app_with_dbo,
          dbo_modules_prefix: dbo_modules_prefix
        }
      )

  ####################################################################################################################
  ####################################################################################################################
end
