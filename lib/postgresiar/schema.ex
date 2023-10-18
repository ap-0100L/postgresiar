defmodule Postgresiar.Schema do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  ## Module
  """

  use Utils
  use Bitwise, only_operators: true

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
      def select_repo_by_uuid(_uuid, mode)
          when not is_atom(mode) or mode not in @repo_modes,
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["mode cannot be nil; mode must be one of #{@repo_modes}"])

      def select_repo_by_uuid(uuid, mode) do
        {repo_rw, repo_ro} =
          if @is_db_distributed do
            [_a, {:binary, b}, _c, _d, _e] = UUID.info!(uuid)

            <<_hi_part::64, lo_part::64>> = b

            part = lo_part &&& 0xFF

            {repo_rw, repo_ro} =
              if part <= 127 do
                [{{repo_a_rw, _}, {repo_a_ro, _}}, _repo_b] = @repos
                {repo_a_rw, repo_a_ro}
              else
                [_repo_a, {{repo_b_rw, _}, {repo_b_ro, _}}] = @repos
                {repo_b_rw, repo_b_ro}
              end

            {repo_rw, repo_ro}
          else
            [{{repo_a_rw, _}, {repo_a_ro, _}}, _repo_b] = @repos

            {repo_a_rw, repo_a_ro}
          end

        repo =
          if mode == :RW do
            repo_rw
          else
            repo_ro
          end

        {:ok, repo}
      end

      def select_repo_by_uuid(uuid, mode),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              uuid: uuid,
              mode: mode
            }
          )

      ####################################################################################################################
      @doc """
      ### Function
      """
      def get_table_postfix_by_uuid(uuid)
          when not is_bitstring(uuid),
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["uuid cannot be nil; uuid must be a string"])

      def get_table_postfix_by_uuid(uuid) do
        s =
          if @is_table_distributed do
            [_a, {:binary, b}, _c, _d, _e] = UUID.info!(uuid)

            <<hi_part::64, _lo_part::64>> = b

            part = hi_part &&& 0xFF

            "_#{String.pad_leading("#{part}", 3, "0")}"
          else
            ""
          end

        {:ok, s}
      end

      def get_table_postfix_by_uuid(uuid),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              uuid: uuid
            }
          )

      ####################################################################################################################
      @doc """
      ### Function
      """
      def prepare_struct(struct, mode, key \\ :id)

      def prepare_struct(struct, mode, key)
          when not is_struct(struct) or not is_atom(mode) or mode not in @repo_modes or not is_atom(key),
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["struct, mode, key cannot be nil; mode, key must be an atom; struct must be a struct; mode must be one of #{@repo_modes}"])

      def prepare_struct(struct, mode, key) do
        id = Map.get(key, struct, nil)

        {:ok, repo} = select_repo_by_uuid(id, mode)
        {:ok, postfix} = get_table_postfix_by_uuid(id)

        source = Ecto.get_meta(struct, :source)
        struct = Ecto.put_meta(struct, :source, "#{source}#{postfix}")

        {:ok, {repo, struct}}
      end

      def prepare_struct(struct, mode, key),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              struct: struct,
              mode: mode,
              key: key
            }
          )

      ####################################################################################################################
      @doc """
      ### Function
      """
      def get_repo_table_name(id, mode)

      def get_repo_table_name(id, mode)
          when not is_bitstring(id) or not is_atom(mode) or mode not in @repo_modes,
          do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["id, mode cannot be nil; mode must be an atom; mode must be one of #{@repo_modes}"])

      def get_repo_table_name(id, mode) do
        table_name = Ecto.get_meta(%__MODULE__{}, :source)
        {:ok, postfix} = get_table_postfix_by_uuid(id)
        table_name = "#{table_name}#{postfix}"

        {:ok, repo} = select_repo_by_uuid(id, mode)

        {:ok, {repo, table_name}}
      end

      def get_repo_table_name(id, mode),
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
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [])

      def preload!(struct_or_structs_or_nil, preloads, opts) when is_struct(struct_or_structs_or_nil) do
        key = opts[:uuid_key] || :id
        {:ok, {repo, struct_or_structs_or_nil}} = prepare_struct(struct_or_structs_or_nil, :RO, key)

        apply(repo, :preload!, [struct_or_structs_or_nil, preloads, opts])
      end

      def preload!(struct_or_structs_or_nil, preloads, opts),
        do:
          UniError.raise_error!(
            :WRONG_ARGUMENT_COMBINATION_ERROR,
            ["Wrong argument combination"],
            arguments: %{
              struct_or_structs_or_nil: struct_or_structs_or_nil,
              preloads: preloads,
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
        {:ok, {repo, obj}} = prepare_struct(obj, :RW, key)

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
      def update(obj, opts \\ [], async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def update(obj, opts, async, rescue_func, rescue_func_args, module) do
        key = opts[:uuid_key] || :id
        {:ok, {repo, obj}} = prepare_struct(obj, :RW, key)

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
