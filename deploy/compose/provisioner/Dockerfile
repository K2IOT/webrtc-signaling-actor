# Provisioner image: PKI/keys (stage 1), contract rendering and database seeding (stage 2).
# LOCAL_TEST_ONLY / NOT_QUALIFIED. Holds no secret: every key, password and token is generated
# at run time into the `run` volume.
#
# Build from the repository root (the dedicated allow-list ignore file next to this Dockerfile,
# Dockerfile.dockerignore, admits only the inputs listed there; the root .dockerignore is not used):
#   docker build -t signaling-provisioner:local -f deploy/compose/provisioner/Dockerfile .
#
# Base: the same pinned JRE as the repository root Dockerfile (linux/amd64 manifest of 21.0.8+9).
# The JRE image ships keytool, which stage 1 needs for the PKCS12 trust stores.
FROM eclipse-temurin:21.0.8_9-jre-jammy@sha256:cddd554e8d69b48b46e8b0c9d1ce72ae5fe8d84819dcdb7131328531e9cc100b

# Flyway CLI from Maven Central. Central publishes only .md5/.sha1 next to this artifact (no
# .sha256), so the SHA-256 below was computed once from the downloaded file and pinned here; the
# build also cross-checks the SHA-1 that Central publishes.
ARG FLYWAY_VERSION=11.7.2
ARG FLYWAY_SHA256=aefbe603eacb6e32342188a5a35ce7eff78d9251f6b33e774aa828ddab63af49
# PostgreSQL Global Development Group archive signing key (https://www.postgresql.org/download/linux/ubuntu/)
ARG PGDG_SIGNER_FPR=B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8

LABEL org.opencontainers.image.title="webrtc-signaling-provisioner" \
      io.webrtc.signaling.release.status="NOT_QUALIFIED" \
      io.webrtc.signaling.qualification="LOCAL_TEST_ONLY"

# psql 17 comes from the PGDG repository (jammy's own client is 14). The key is fetched at build
# time and refused unless its fingerprint equals the pinned one.
RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg openssl jq; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /tmp/pgdg.asc; \
    actual="$(gpg --show-keys --with-colons /tmp/pgdg.asc | awk -F: '/^fpr:/ {print $10; exit}')"; \
    [ "$actual" = "$PGDG_SIGNER_FPR" ] || { echo "PGDG key fingerprint mismatch" >&2; exit 1; }; \
    gpg --dearmor </tmp/pgdg.asc >/usr/share/keyrings/pgdg.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" \
      >/etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends postgresql-client-17; \
    rm -f /tmp/pgdg.asc; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    base="https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/${FLYWAY_VERSION}"; \
    file="flyway-commandline-${FLYWAY_VERSION}-linux-x64.tar.gz"; \
    curl -fsSL "$base/$file" -o "/tmp/$file"; \
    echo "${FLYWAY_SHA256}  /tmp/$file" | sha256sum -c -; \
    echo "$(curl -fsSL "$base/$file.sha1")  /tmp/$file" | sha1sum -c -; \
    mkdir -p /opt/flyway; \
    tar -xzf "/tmp/$file" -C /opt/flyway --strip-components=1; \
    ln -s /opt/flyway/flyway /usr/local/bin/flyway; \
    rm -f "/tmp/$file"

# Provisioner scripts, topology and the inputs the stages read. Layout inside the image mirrors
# deploy/compose/ so provision.sh finds ../lib/topology.sh and ../topology.env the same way as in
# the repository.
COPY deploy/compose /opt/provisioner/compose
COPY signaling-identity/src/main/resources/keys/public.pem /opt/provisioner/identity-public.pem
COPY signaling-storage/src/main/resources/db/migration/ /opt/provisioner/migrations/storage/
COPY signaling-control-api/src/main/resources/db/directory/migration/ /opt/provisioner/migrations/directory/
COPY config/production-defaults.yaml /opt/provisioner/production-defaults.yaml

RUN chmod +x /opt/provisioner/compose/provisioner/provision.sh \
    && ln -s /opt/provisioner/compose/provisioner/provision.sh /usr/local/bin/provision.sh

ENV IDENTITY_PUBLIC_KEY=/opt/provisioner/identity-public.pem \
    RUN_ROOT=/run/signaling-src
WORKDIR /opt/provisioner
# Runs as root on purpose: stage 1 sets root:10001 ownership on the volume it writes.
CMD ["provision.sh", "all"]
