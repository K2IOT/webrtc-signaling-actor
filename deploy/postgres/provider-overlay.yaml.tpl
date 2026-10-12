# INPUT TEMPLATE ONLY: render from an approved NativeDatabaseProviderContract.
# Missing substitutions must reject rendering. This file is not an applyable manifest.
# Apply the cluster patch to both cell and directory authorities, with distinct archive identities.
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: ${CLUSTER_IDENTITY}
spec:
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: ${ARCHIVE_OBJECT_NAME}
        serverName: ${ARCHIVE_SERVER_NAME}
  postgresql:
    parameters:
      archive_timeout: '${WAL_ARCHIVE_TIMEOUT_SECONDS}s'
      autovacuum_max_workers: '${AUTOVACUUM_WORKERS}'
      maintenance_work_mem: '${MAINTENANCE_WORK_MEM_MIB}MB'
      autovacuum_vacuum_cost_limit: '${VACUUM_COST_LIMIT}'
      autovacuum_vacuum_cost_delay: '${VACUUM_COST_DELAY_MILLIS}ms'
      autovacuum_vacuum_scale_factor: '${VACUUM_SCALE_FACTOR}'
      autovacuum_analyze_scale_factor: '${ANALYZE_SCALE_FACTOR}'
      autovacuum_freeze_max_age: '${FREEZE_MAX_AGE}'
      autovacuum_multixact_freeze_max_age: '${MULTIXACT_FREEZE_MAX_AGE}'
      autovacuum_naptime: '${AUTOVACUUM_NAPTIME_SECONDS}s'
---
# S3 variant. Azure/GCS require their provider-specific credentials from the typed contract.
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: ${ARCHIVE_OBJECT_NAME}
spec:
  configuration:
    destinationPath: ${ARCHIVE_DESTINATION}
    endpointURL: ${ARCHIVE_HTTPS_ENDPOINT}
    endpointCA:
      name: ${ARCHIVE_CA_SECRET}
      key: ${ARCHIVE_CA_SECRET_KEY}
    s3Credentials:
      accessKeyId:
        name: ${ARCHIVE_ACCESS_KEY_SECRET}
        key: ${ARCHIVE_ACCESS_KEY_SECRET_KEY}
      secretAccessKey:
        name: ${ARCHIVE_SECRET_KEY_SECRET}
        key: ${ARCHIVE_SECRET_KEY_SECRET_KEY}
  retentionPolicy: '${ARCHIVE_RECOVERY_WINDOW_DAYS}d'
