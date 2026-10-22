# --- start-directory.sh: regional server {{SERVER}} (fragment appended to start-common.sh) ----
# Holds the databases `directory` and `source` (provisioner stage 2 creates them). No standby:
# the directory is not on the WAL-fenced path, so no synchronous_standby_names and no
# replication role here.
install_runtime_files
prepare_init ': # nothing to create before stage 2'
exec docker-entrypoint.sh postgres "${SERVER_OPTS[@]}"
