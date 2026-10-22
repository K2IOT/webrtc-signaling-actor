# deploy/compose/Makefile -- front end of the two-cell compose environment.
# LOCAL_TEST_ONLY / NOT_QUALIFIED: no claim of production readiness, HA or capacity.
# Supported lifecycle: `make build` once, then `make up` -> use -> `make down` (always down -v).
# Run from anywhere: `make -C deploy/compose <target>`.

SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

COMPOSE_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
REPO := $(abspath $(COMPOSE_DIR)/../..)

# Project name comes from compose.yaml (`name:`), so volumes are <project>_run and so on.
PROJECT := $(shell sed -n 's/^name: *//p' $(COMPOSE_DIR)/compose.yaml)
RUN_VOLUME := $(PROJECT)_run

# Image tags: .env if present, else .env.example (the compose file reads the same keys).
ENV_SOURCE := $(firstword $(wildcard $(COMPOSE_DIR)/.env) $(COMPOSE_DIR)/.env.example)
env_val = $(shell sed -n 's/^$(1)=//p' $(ENV_SOURCE) | tail -n 1)
APP_IMAGE := $(call env_val,APP_IMAGE)
IDENTITY_IMAGE := $(call env_val,IDENTITY_IMAGE)
PLATFORM_IMAGE := $(call env_val,PLATFORM_IMAGE)
KUBE_API_IMAGE := $(call env_val,KUBE_API_IMAGE)
PROVISIONER_IMAGE := $(call env_val,PROVISIONER_IMAGE)

SOURCE_COMMIT := $(or $(shell git -C $(REPO) rev-parse HEAD 2>/dev/null),unknown)
MVN_OFFLINE ?= 1
MVN := ./mvnw$(if $(filter 1,$(MVN_OFFLINE)), -o)

COMPOSE := docker compose --project-directory $(COMPOSE_DIR) --env-file $(COMPOSE_DIR)/topology.env --env-file $(COMPOSE_DIR)/.env -f $(COMPOSE_DIR)/compose.yaml
SMOKE_DIR := $(COMPOSE_DIR)/.smoke

.PHONY: help build hosts preflight up smoke-ca smoke down reset lint test ensure-env

help: ## list the targets
	@echo "deploy/compose (LOCAL_TEST_ONLY / NOT_QUALIFIED). Targets:"
	@grep -E '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{ printf "  %-10s %s\n", $$1, $$2 }'

ensure-env: # creates .env from .env.example on first use; never overwrites
	@if [ ! -e $(COMPOSE_DIR)/.env ]; then cp $(COMPOSE_DIR)/.env.example $(COMPOSE_DIR)/.env; echo "created .env from .env.example"; fi

build: ## package the jars (MVN_OFFLINE=0 for online) and build the five own images
	cd $(REPO) && $(MVN) -pl signaling-app,signaling-identity,signaling-local-platform -am package -DskipTests -Dspotless.check.skip=true
	cd $(REPO) && docker build --build-arg SOURCE_COMMIT=$(SOURCE_COMMIT) -t $(APP_IMAGE) -f Dockerfile .
	cd $(REPO) && docker build --build-arg SOURCE_COMMIT=$(SOURCE_COMMIT) -t $(IDENTITY_IMAGE) -f Dockerfile.identity .
	cd $(REPO) && docker build --build-arg SOURCE_COMMIT=$(SOURCE_COMMIT) -t $(PLATFORM_IMAGE) -f Dockerfile.local-platform .
	cd $(REPO) && docker build --build-arg SOURCE_COMMIT=$(SOURCE_COMMIT) -t $(KUBE_API_IMAGE) -f deploy/compose/kube-api/Dockerfile .
	cd $(REPO) && docker build --build-arg SOURCE_COMMIT=$(SOURCE_COMMIT) -t $(PROVISIONER_IMAGE) -f deploy/compose/provisioner/Dockerfile .

hosts: ## print the /etc/hosts lines to add (never writes a file)
	@bash $(COMPOSE_DIR)/hosts.sh

preflight: ensure-env ## check compose version, free RAM, load, subnets and images
	@COMPOSE_PROJECT=$(PROJECT) bash $(COMPOSE_DIR)/preflight.sh

up: ensure-env ## preflight, lifecycle guard, then docker compose up -d --wait
	COMPOSE_PROJECT=$(PROJECT) bash $(COMPOSE_DIR)/preflight.sh
	COMPOSE_PROJECT=$(PROJECT) PROVISIONER_IMAGE=$(PROVISIONER_IMAGE) bash $(COMPOSE_DIR)/guard.sh
	$(COMPOSE) up -d --wait --wait-timeout 600 || { rc=$$?; echo "make up: a service did not become healthy"; bash $(COMPOSE_DIR)/status.sh failures; exit $$rc; }
	bash $(COMPOSE_DIR)/status.sh table

smoke-ca: ## export the CA and admin client material for the smoke client to .smoke/
	@docker volume inspect $(RUN_VOLUME) >/dev/null 2>&1 || { echo "smoke-ca: volume $(RUN_VOLUME) does not exist; run make up first"; exit 1; }
	umask 077; mkdir -p $(SMOKE_DIR); chmod 0700 $(SMOKE_DIR)
	umask 077; rm -f $(SMOKE_DIR)/ca-workload.crt $(SMOKE_DIR)/admin-client.crt $(SMOKE_DIR)/admin-client.key; \
	for f in ca-workload.crt admin/admin-client.crt admin/admin-client.key; do \
	  b=$$(basename $$f); t=$(SMOKE_DIR)/.tmp.$$$$; \
	  if docker run --rm -v $(RUN_VOLUME):/r:ro $(PROVISIONER_IMAGE) cat /r/_shared/$$f > $$t && chmod 0600 $$t && mv -f $$t $(SMOKE_DIR)/$$b; then \
	    echo "exported .smoke/$$b"; \
	  else \
	    rm -f $$t $(SMOKE_DIR)/$$b; echo "smoke-ca: export of $$b failed" >&2; exit 1; \
	  fi; \
	done

smoke: smoke-ca ## run the cross-cell smoke test against the running stack
	@[ -d $(COMPOSE_DIR)/smoke/node_modules/ws ] || (cd $(COMPOSE_DIR)/smoke && npm ci --ignore-scripts --no-audit --no-fund)
	@resolve=""; \
	hosts_ok="$$(getent hosts c001.signaling.local c002.signaling.local bootstrap.signaling.local identity.signaling.local | grep -c '^127\.0\.0\.1' || true)"; \
	if [ "$$hosts_ok" -lt 4 ]; then \
	  resolve="$$(bash $(COMPOSE_DIR)/hosts.sh 2>/dev/null | awk '{ printf "%s%s=%s", sep, $$2, $$1; sep="," }')"; \
	  echo "smoke: /etc/hosts entries absent; resolving $$resolve internally"; \
	fi; \
	CA_FILE=$(SMOKE_DIR)/ca-workload.crt ADMIN_CERT_FILE=$(SMOKE_DIR)/admin-client.crt ADMIN_KEY_FILE=$(SMOKE_DIR)/admin-client.key \
	  SMOKE_RESOLVE="$$resolve" node $(COMPOSE_DIR)/smoke/smoke.mjs

down: ensure-env ## docker compose down -v --remove-orphans (volumes always go: see README)
	$(COMPOSE) down -v --remove-orphans

reset: down ## down, then remove the exported .smoke/ material
	rm -rf $(SMOKE_DIR)

lint: ## lint the resolved compose model
	python3 $(COMPOSE_DIR)/lint/compose_lint.py

test: ## shell, node and python tests (RUN_DOCKER_TESTS=1 adds the Docker-gated ones)
	bash $(COMPOSE_DIR)/tests/run-tests.sh
