defmodule Postgresiar.Repo do
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
  @callback init(context :: any, config :: any) :: term

  @optional_callbacks init: 2

  ##############################################################################
  @doc """
  ## Function
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @otp_app Keyword.fetch!(opts, :otp_app)
      @read_only Keyword.get(opts, :read_only, false)

      use Ecto.Repo,
        otp_app: @otp_app,
        adapter: Ecto.Adapters.Postgres,
        read_only: @read_only

      use Utils

      alias Utils, as: Utils
      alias Postgresiar.Repo, as: PostgresiarRepo
      alias __MODULE__, as: SelfModule

      @behaviour PostgresiarRepo

      ##############################################################################
      @doc """
      ### Function
      """
      @impl true
      def init(context, config) do
        {:ok, log_config} = Utils.get_app_env!(:postgresiar, :log_config)

        if log_config do
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I completed #{__MODULE__} init part, context: #{inspect(context)}, config: #{inspect(config)}")
        end

        {:ok, config}
      end

      ##############################################################################
      @doc """
      after_connect: {PersistentDb.Repo, :set_search_path, ["public"]}
      """
      def set_search_path(conn, path) do
        {:ok, _result} = Postgrex.query(conn, "SET search_path=#{path}", [])
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def exec_query!(query, params \\ [], opts \\ [])

      def exec_query!(query, params, opts) do
        # query("select get_ch_part_actions($1, $2, $3, $4)", ["notification_bot", "rest_api_ch_part", "message", "send"])

        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :query, [query, params, opts])

              # SelfModule.all(query, opts)
            )
          )

        result =
          case result do
            {:ok, %{rows: []}} ->
              :CODE_NOTHING_FOUND

            {:ok, %{rows: result}} ->
              result

            {:error, reason} ->
              UniError.raise_error!(
                :CODE_EXEC_QUERY_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :CODE_EXEC_QUERY_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def transaction!(fun_or_multi, opts \\ [])

      def transaction!(fun_or_multi, opts) do
        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :transaction, [fun_or_multi, opts])

              # SelfModule.all(query, opts)
            )
          )

        result =
          case result do
            {:ok, result} ->
              result

            {:error, reason} ->
              UniError.raise_error!(
                :CODE_TRANSACTION_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :CODE_TRANSACTION_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def get_by_query!(query, opts \\ [])

      def get_by_query!(query, opts) do
        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)
              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :all, [query, opts])

              # SelfModule.all(query, opts)
            )
          )

        result =
          case result do
            [] ->
              :CODE_NOTHING_FOUND

            {:error, reason} ->
              UniError.raise_error!(
                :CODE_GET_BY_QUERY_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            result ->
              result
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def preload!(struct_or_structs_or_nil, preloads, opts \\ [])

      def preload!(struct_or_structs_or_nil, preloads, opts) do
        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)
              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :preload, [struct_or_structs_or_nil, preloads, opts])

              # SelfModule.all(query, opts)
            )
          )

        result =
          case result do
            {:error, reason} ->
              UniError.raise_error!(
                :CODE_PRELOAD_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            result ->
              result
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      Insert
      """
      def insert_record!(obj) do
        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :insert, [obj])

              # SelfModule.insert(obj)
            )
          )

        result =
          case result do
            {:ok, item} ->
              item

            {:error, reason} ->
              UniError.raise_error!(
                :CODE_INSERT_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :CODE_INSERT_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Unexpected error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      Insert async
      """
      def insert_record_async(obj, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def insert_record_async(obj, rescue_func, rescue_func_args, module) do
        func = fn ->
          # :timer.sleep(20000)
          UniError.rescue_error!(SelfModule.insert_record!(obj), true, true, rescue_func, rescue_func_args, module)
        end

        pid = spawn(func)

        {:ok, pid}
      end

      ##############################################################################
      @doc """
      Update
      """
      def update_record!(obj) do
        result =
          UniError.rescue_error!(
            (
              {:ok, remote_node_name_prefixes} = Utils.get_app_env!(:postgresiar, :remote_node_name_prefixes)

              RPCUtils.call_local_or_rpc!(remote_node_name_prefixes, SelfModule, :update, [obj])

              # SelfModule.update(obj)
            )
          )

        result =
          case result do
            {:ok, item} ->
              item

            {:error, reason} ->
              UniError.raise_error!(
                :CODE_UPDATE_PERSISTENT_DB_ERROR,
                ["Error occurred while process operation persistent DB"],
                previous: reason
              )

            unexpected ->
              UniError.raise_error!(
                :CODE_UPDATE_PERSISTENT_DB_UNEXPECTED_ERROR,
                ["Unexpected error occurred while process operation persistent DB"],
                previous: unexpected
              )
          end

        {:ok, result}
      end

      ##############################################################################
      @doc """
      Insert async
      """
      def update_record_async(obj, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def update_record_async(obj, rescue_func, rescue_func_args, module) do
        func = fn ->
          UniError.rescue_error!(SelfModule.update_record!(obj), true, true, rescue_func, rescue_func_args, module)
        end

        pid = spawn(func)

        {:ok, pid}
      end

      defoverridable PostgresiarRepo
    end
  end

  ##############################################################################
  ##############################################################################
end
