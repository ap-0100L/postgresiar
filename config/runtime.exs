import Config

import ConfigUtils, only: [get_env!: 3, get_env!: 2, get_env_name!: 1]

if config_env() in [:prod, :dev] do
  config :postgresiar,
    remote_node_name_prefixes: get_env!(get_env_name!("POSTGRESIAR_REMOTE_NODE_NAME_PREFIXES"), :list_of_regex),
    log_config: get_env!(get_env_name!("POSTGRESIAR_LOG_CONFIG"), :boolean, false)
end
