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
