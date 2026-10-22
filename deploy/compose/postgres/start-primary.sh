# --- start-primary.sh: cell primary {{SERVER}} (fragment appended to start-common.sh) ---------
# Synchronous from the first start: synchronous_standby_names names the cell standby
# ({{STANDBY_NAME}}, its application_name), so every commit waits for it once it streams. The
# name is double-quoted there: a hyphen is not valid in an unquoted standby name.
# The replication role must exist before the standby's first base backup, which runs before
# provisioner stage 2, so it is created here at initialisation. That bootstrap server is
# socket-only (no standby can reach it), hence synchronous_commit=local for that one statement.
# The password reaches psql as a variable read from the tmpfs copy, never as an argument.
install_runtime_files
if ! cluster_exists; then
  install -o postgres -g postgres -m 0400 "$SRC/replicator.pass" "$TLS/replicator.pass"
fi
prepare_init "$(
  cat <<'INIT'
docker_process_sql --dbname postgres <<'SQL'
SET synchronous_commit = local;
\set replicator_password `cat /run/pgtls/replicator.pass`
CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD :'replicator_password';
SQL
rm -f /run/pgtls/replicator.pass
INIT
)"
exec docker-entrypoint.sh postgres "${SERVER_OPTS[@]}" \
  -c "synchronous_standby_names=ANY 1 (\"{{STANDBY_NAME}}\")"
