# Fake Kubernetes API (Pekko kubernetes-api discovery), one container per cell.
# LOCAL_TEST_ONLY / NOT_QUALIFIED. Holds no secret: certificate, key and bearer token are mounted
# read-only at run time (/run/signaling) and selected through KUBE_* environment variables.
#
# Build from the repository root (the dedicated allow-list ignore file next to this Dockerfile,
# Dockerfile.dockerignore, admits only server.mjs and this Dockerfile):
#   docker build -t signaling-kube-api:local -f deploy/compose/kube-api/Dockerfile .
#
# Base: node:22-bookworm-slim (Node 22 LTS), the multi-arch index digest resolved with
#   docker buildx imagetools inspect node:22-bookworm-slim
# on 2026-10-22. Zero dependencies: no npm install.
FROM node:22-bookworm-slim@sha256:de170b39439eb0e5a3444ad7d60b015614a381e10fecae2e30ec9e735122f38d

LABEL org.opencontainers.image.title="webrtc-signaling-kube-api" \
      io.webrtc.signaling.release.status="NOT_QUALIFIED" \
      io.webrtc.signaling.qualification="LOCAL_TEST_ONLY"

COPY --chown=10001:10001 deploy/compose/kube-api/server.mjs /opt/kube-api/server.mjs

USER 10001:10001
ENV NODE_ENV=production
ENTRYPOINT ["node", "/opt/kube-api/server.mjs"]
