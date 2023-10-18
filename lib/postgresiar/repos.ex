repos = Application.get_env(:postgresiar, :repos)

for {
      {repo_rw, rw_opts},
      {repo_ro, ro_opts}
    } <- repos do
  defmodule repo_rw do
    ####################################################################################################################
    ####################################################################################################################
    @moduledoc """
    Read-write persistent repository
    """
    use Postgresiar.Repo, [otp_app: :postgresiar] ++ [read_only: false] ++ Keyword.delete(rw_opts[:module_opts] || [], :read_only)
  end

  defmodule repo_ro do
    ####################################################################################################################
    ####################################################################################################################
    @moduledoc """
    Read-only persistent repository
    """

    use Postgresiar.Repo, [otp_app: :postgresiar] ++ [read_only: true] ++ Keyword.delete(ro_opts[:module_opts] || [], :read_only)
  end
end

defmodule Postgresiar.Repos do
  ####################################################################################################################
  ####################################################################################################################
  @moduledoc """
  Module
  """
  app_with_dbo = :api_core
  dbo_modules_prefix = "Elixir.ApiCore.Db.Persistent.Dao"

  {:ok, modules_list} = :application.get_key(app_with_dbo, :modules)

end


#
#dbo_modules =
#  Enum.reduce(
#    modules_list,
#    [],
#    fn module, accum ->
#      if String.starts_with?("#{module}", dbo_modules_prefix) do
#        accum ++ [module]
#      else
#        accum
#      end
#    end
#  )

#result =
#  Enum.reduce(
#    dbo_modules,
#    [],
#    fn module, accum ->
#      accum ++ [apply(module, :create_table, [])]
#    end
#  )
