defmodule Postgresiar.Repo do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  ## Module
  """

  use Utils
  use Bitwise, only_operators: true

  @repo_modes [:RW, :RO]

  @repos Application.get_env(:postgresiar, :repos)

  ####################################################################################################################
  @doc """
  ## Function
  """
  @callback init(context :: any, config :: any) :: term

  @optional_callbacks init: 2

  ####################################################################################################################
  @doc """
  ## Function
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @otp_app Keyword.fetch!(opts, :otp_app)
      @read_only Keyword.get(opts, :read_only, false)
      @disable_rpc Keyword.get(opts, :disable_rpc, nil) || Application.get_env(:postgresiar, :disable_rpc, nil) || true

      use Ecto.Repo,
        otp_app: @otp_app,
        adapter: Ecto.Adapters.Postgres,
        read_only: @read_only

      @repo_modes [:RW, :RO]

      use Utils

      alias Postgresiar.Repo, as: PostgresiarRepo

      @behaviour PostgresiarRepo

      ####################################################################################################################
      @doc """
      ### Function
      """
      @impl true
      def init(context, config) do
        {:ok, log_config} = Utils.get_app_env(:postgresiar, :log_config)

        if log_config do
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I completed #{__MODULE__} init part, context: #{inspect(context)}, config: #{inspect(config)}")
        end

        {:ok, put_in(config, [:parameters, :application_name], Node.self() |> to_string)}
      end

      ####################################################################################################################
      @doc """
      after_connect: {PersistentDb.Repo, :set_search_path, ["public"]}
      """
      def set_search_path(conn, path) do
        {:ok, _result} = Postgrex.query(conn, "SET search_path=#{path}", [])
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def exec_query(query, params \\ [], opts \\ [])

      def exec_query(query, params, opts) do
        # query("select get_ch_part_actions($1, $2, $3, $4)", ["notification_bot", "rest_api_ch_part", "message", "send"])

        result =
          UniError.rescue_error!(
            if @disable_rpc do
              # apply(__MODULE__, :query, [query, params, opts])
              query(query, params, opts)
            else
              {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :query, [query, params, opts])
            end,
            false,
            false
          )

        result =
          case result do
            {:ok, %{rows: []}} ->
              :NOT_FOUND

            {:ok, %{rows: [[]]}} ->
              :NOT_FOUND

            {:ok, %{rows: [[nil]]}} ->
              :NOT_FOUND

            {:ok, %{rows: result}} ->
              result

            {:error, reason} ->
              UniError.raise_error!(
                :EXEC_QUERY_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :EXEC_QUERY_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def transaction!(fun_or_multi, opts \\ [])

      def transaction!(fun_or_multi, opts) do
        result =
          UniError.rescue_error!(
            if @disable_rpc do
              # apply(__MODULE__, :transaction, [fun_or_multi, opts])
              transaction(fun_or_multi, opts)
            else
              {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :transaction, [fun_or_multi, opts])
            end,
            false,
            false
          )

        result =
          case result do
            {:ok, result} ->
              result

            {:error, reason} ->
              UniError.raise_error!(
                :TRANSACTION_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :TRANSACTION_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def find_by_query(query, opts \\ [])

      def find_by_query(query, opts) do
        result =
          UniError.rescue_error!(
            if @disable_rpc do
              # apply(__MODULE__, :all, [query, opts])
              all(query, opts)
            else
              {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)
              RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :all, [query, opts])
            end,
            false,
            false
          )

        result =
          case result do
            [] ->
              :NOT_FOUND

            {:error, reason} ->
              UniError.raise_error!(
                :GET_BY_QUERY_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            result ->
              result
          end

        {:ok, result}
      end

      ####################################################################################################################
      @doc """
      ### Function
      """
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [])

      def preload!(struct_or_structs_or_nil, preloads, opts) do
        result =
          UniError.rescue_error!(
            if @disable_rpc do
              # apply(__MODULE__, :preload, [struct_or_structs_or_nil, preloads, opts])
              preload(struct_or_structs_or_nil, preloads, opts)
            else
              {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)
              RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :preload, [struct_or_structs_or_nil, preloads, opts])
            end,
            false,
            false
          )

        result =
          case result do
            {:error, reason} ->
              UniError.raise_error!(
                :PRELOAD_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            result ->
              result
          end

        {:ok, result}
      end

      if not @read_only do
        ####################################################################################################################
        @doc """
        Insert
        """
        def insert_record!(obj, opts \\ [])

        def insert_record!(obj, opts) do
          result =
            UniError.rescue_error!(
              if @disable_rpc do
                # apply(__MODULE__, :insert, [obj, opts])
                insert(obj, opts)
              else
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :insert, [obj, opts])
              end,
              false,
              false
            )

          result =
            case result do
              {:ok, item} ->
                item

              {:error, reason} ->
                UniError.raise_error!(
                  :INSERT_PERSISTENT_DB_ERROR,
                  ["Error occurred while process operation persistent DB"],
                  previous: reason
                )

              unexpected ->
                UniError.raise_error!(
                  :INSERT_PERSISTENT_DB_UNEXPECTED_ERROR,
                  ["Unexpected error occurred while process operation persistent DB"],
                  previous: unexpected
                )
            end

          {:ok, result}
        end

        ####################################################################################################################
        @doc """
        Insert async
        """
        def insert_record_async(obj, opts \\ [], rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

        def insert_record_async(obj, opts, rescue_func, rescue_func_args, module) do
          func = fn ->
            # :timer.sleep(20000)
            {reraise, log_error} =
              if is_nil(rescue_func) do
                {true, true}
              else
                {false, false}
              end

            UniError.rescue_error!(__MODULE__.insert_record!(obj, opts), reraise, log_error, rescue_func, rescue_func_args, module)
          end

          pid = spawn(func)

          {:ok, pid}
        end

        ####################################################################################################################
        @doc """
        Update
        """
        def update_record!(obj, opts \\ [])

        def update_record!(obj, opts) do
          result =
            UniError.rescue_error!(
              if @disable_rpc do
                # apply(__MODULE__, :update, [obj])
                update(obj, opts)
              else
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :update, [obj, opts])
              end,
              false,
              false
            )

          result =
            case result do
              {:ok, item} ->
                item

              {:error, reason} ->
                UniError.raise_error!(
                  :UPDATE_PERSISTENT_DB_ERROR,
                  ["Error occurred while process operation persistent DB"],
                  previous: reason
                )

              unexpected ->
                UniError.raise_error!(
                  :UPDATE_PERSISTENT_DB_UNEXPECTED_ERROR,
                  ["Unexpected error occurred while process operation persistent DB"],
                  previous: unexpected
                )
            end

          {:ok, result}
        end

        ####################################################################################################################
        @doc """
        Insert async
        """
        def update_record_async(obj, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

        def update_record_async(obj, rescue_func, rescue_func_args, module) do
          func = fn ->
            {reraise, log_error} =
              if is_nil(rescue_func) do
                {true, true}
              else
                {false, false}
              end

            UniError.rescue_error!(__MODULE__.update_record!(obj), reraise, log_error, rescue_func, rescue_func_args, module)
          end

          pid = spawn(func)

          {:ok, pid}
        end

        defoverridable PostgresiarRepo
      end
    end
  end

  ####################################################################################################################
  @doc """
  ### Function
  """
  def select_repo_by_uuid(_uuid, mode)
      when not is_atom(mode) or mode not in @repo_modes,
      do: UniError.raise_error!(:WRONG_FUNCTION_ARGUMENT_ERROR, ["mode cannot be nil; mode must be one of #{@repo_modes}"])

  def select_repo_by_uuid(uuid, mode) do
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
    [_a, {:binary, b}, _c, _d, _e] = UUID.info!(uuid)

    <<hi_part::64, _lo_part::64>> = b

    part = hi_part &&& 0xFF

    s = "_#{String.pad_leading("#{part}", 3, "0")}"

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
  ####################################################################################################################
end
