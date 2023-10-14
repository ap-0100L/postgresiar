defmodule Postgresiar.Repo do
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

      use Ecto.Repo,
        otp_app: @otp_app,
        adapter: Ecto.Adapters.Postgres,
        read_only: @read_only

      use Utils

      alias Utils, as: Utils
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

        {:ok, config}
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

        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :query, [query, params, opts])
            query(query, params, opts)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :query, [query, params, opts])

                # __MODULE__.all(query, opts)
              )
            )
          end

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
        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :transaction, [fun_or_multi, opts])
            transaction(fun_or_multi, opts)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :transaction, [fun_or_multi, opts])

                # __MODULE__.all(query, opts)
              )
            )
          end

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
        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :all, [query, opts])
            all(query, opts)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)
                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :all, [query, opts])

                # __MODULE__.all(query, opts)
              )
            )
          end

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
        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :preload, [struct_or_structs_or_nil, preloads, opts])
            preload(struct_or_structs_or_nil, preloads, opts)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)
                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :preload, [struct_or_structs_or_nil, preloads, opts])

                # __MODULE__.all(query, opts)
              )
            )
          end

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

      ####################################################################################################################
      @doc """
      Insert
      """
      def insert_record!(obj) do
        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :insert, [obj])
            insert(obj)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :insert, [obj])

                # __MODULE__.insert(obj)
              )
            )
          end

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
      def insert_record_async(obj, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def insert_record_async(obj, rescue_func, rescue_func_args, module) do
        func = fn ->
          # :timer.sleep(20000)
          {reraise, log_error} =
            if is_nil(rescue_func) do
              {true, true}
            else
              {false, false}
            end

          UniError.rescue_error!(__MODULE__.insert_record!(obj), reraise, log_error, rescue_func, rescue_func_args, module)
        end

        pid = spawn(func)

        {:ok, pid}
      end

      ####################################################################################################################
      @doc """
      Update
      """
      def update_record!(obj) do
        {:ok, disable_rpc} = Utils.get_app_env(:postgresiar, :disable_rpc)

        result =
          if disable_rpc do
            #apply(__MODULE__, :update, [obj])
            update(obj)
          else
            UniError.rescue_error!(
              (
                {:ok, remote_node_name_prefixes} = Utils.get_app_env(:postgresiar, :remote_node_name_prefixes)

                RPCUtils.call_local_or_rpc(remote_node_name_prefixes, __MODULE__, :update, [obj])

                # __MODULE__.update(obj)
              )
            )
          end

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

  ####################################################################################################################
  ####################################################################################################################
end
