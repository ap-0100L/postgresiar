import Config
#
#
#
#
config :postgresiar,
  remote_node_name_prefixes: [~r/persistent_db@/iu],
  log_config: true

config :logger,
  handle_otp_reports: true,
  backends: [
    :console,
    {LoggerFileBackend, :info_log},
    {LoggerFileBackend, :error_log}
  ]

config :logger,
       :console,
       level: :debug,
       format: "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n",
       metadata: :all

config :logger,
       :info_log,
       path: "log/info.log",
       level: :info,
       format: "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n",
       metadata: :all

config :logger,
       :error_log,
       path: "log/error.log",
       level: :error,
       format: "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n",
       metadata: :all

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
