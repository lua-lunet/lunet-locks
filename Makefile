# Single entrypoint for every workflow. See CONVENTIONS.md.

LUAJIT_DIR ?= $(shell brew --prefix luajit 2>/dev/null || echo /usr/local)
LUAROCKS   ?= luarocks --lua-version=5.1 --lua-dir=$(LUAJIT_DIR) --tree=.rocks
CYAN       ?= .rocks/bin/cyan
TESTED     ?= .rocks/bin/tested
CERU       ?= .rocks/bin/ceru
LUNET_VERSION := v0.7.2
LUNET_ROOT := .lunet/$(LUNET_VERSION)
LUNET_RUN := $(LUNET_ROOT)/lunet-run
LUNET_OS := $(shell uname -s)
LUNET_ARCH := $(shell uname -m)

ifeq ($(LUNET_OS),Darwin)
LUNET_ARCHIVE := lunet-macos.tar.gz
LUNET_SHA256 := 125d9e30eb5576a0691e5d9b8b17464cb1ba94febbc24d241b1e6c18c282f907
else ifeq ($(LUNET_OS)-$(LUNET_ARCH),Linux-x86_64)
LUNET_ARCHIVE := lunet-linux-amd64.tar.gz
LUNET_SHA256 := 5bba15cf4da965e3aca70d0b92f4bd5183477c3fdce6acb90ea6a26c86e47149
else ifeq ($(LUNET_OS)-$(LUNET_ARCH),Linux-aarch64)
LUNET_ARCHIVE := lunet-linux-arm64.tar.gz
LUNET_SHA256 := d17548abe23805002fc81f8b9d7f2a6c6f7ac6194a812e089bedfa3b455edf33
else
$(error Unsupported Lunet runtime platform $(LUNET_OS)/$(LUNET_ARCH); v0.7.2 ships macOS, Linux amd64, and Linux arm64 archives)
endif

LUNET_URL := https://github.com/lua-lunet/lunet/releases/download/$(LUNET_VERSION)/$(LUNET_ARCHIVE)
LUNET_ARCHIVE_PATH := $(LUNET_ROOT)/$(LUNET_ARCHIVE)
# Teal outside source_dir, which `cyan build` does not reach.
CHECK_SOURCES = tests/teal_learning_test.tl \
                tests/advisory_lock_ffi_test.tl \
                tests/advisory_lock_pure_test.tl

.PHONY: init deps build check test smoke simulation simulation-test lunet-runtime docs clean ext ext-check ext-test fmt lint hooks docker-build docker-simulation

init:
	@command -v mise >/dev/null 2>&1 || { echo "ERROR: mise is not on PATH. Install it from https://mise.jdx.dev and try again."; exit 1; }
	mise install
	$(MAKE) deps

deps:
	$(LUAROCKS) install cyan
	$(LUAROCKS) install tested
	$(LUAROCKS) install cerulean

fmt:
	$(CERU) src tests

lint:
	$(CERU) --check src tests

hooks:
	git config core.hooksPath .githooks

build: ext
	$(CYAN) build --prune

check: build lint
	$(CYAN) check $(CHECK_SOURCES)

test: check
	LUA_PATH="$(abspath build)/?.lua;;" $(TESTED) tests

# Official, project-local Lunet runtime. Do not substitute a host installation:
# all service/smoke work must use this exact release and its adjacent `types/` docs.
lunet-runtime: $(LUNET_RUN)

$(LUNET_RUN):
	@mkdir -p $(LUNET_ROOT)
	curl --fail --location --retry 3 --output $(LUNET_ARCHIVE_PATH) $(LUNET_URL)
	@actual=$$(shasum -a 256 $(LUNET_ARCHIVE_PATH) | awk '{print $$1}'); \
		test "$$actual" = "$(LUNET_SHA256)" || { \
			echo "ERROR: $(LUNET_ARCHIVE) SHA-256 mismatch: $$actual" >&2; \
			rm -f $(LUNET_ARCHIVE_PATH); exit 1; \
		}
	tar -xzf $(LUNET_ARCHIVE_PATH) -C $(LUNET_ROOT)
	@test -x $(LUNET_RUN)

smoke: lunet-runtime
	LUNET_RUN=$(abspath $(LUNET_RUN)) CYAN=$(abspath $(CYAN)) tests/lunet_smoke.sh

# A real TCP-NDJSON three-replica failover demonstration. It uses only the
# pinned project-local runtime, never a host `lunet-run` on PATH.
SIM_DURATION ?= 30
SIM_BIN := .tmp/lease-failover-sim

$(SIM_BIN): tools/lease_failover_sim.rs
	@mkdir -p .tmp
	rustc --edition=2021 -O -o $(SIM_BIN) tools/lease_failover_sim.rs

simulation-test: tools/lease_failover_sim.rs
	rustc --edition=2021 --test -o .tmp/lease-failover-sim-test tools/lease_failover_sim.rs
	.tmp/lease-failover-sim-test

simulation: lunet-runtime build $(SIM_BIN)
	SIM_ROOT=$(CURDIR) LUNET_RUN=$(abspath $(LUNET_RUN)) $(SIM_BIN) --duration $(SIM_DURATION)

# Follow vrr-core's conventional plain multi-stage `docker build` model. A
# disposable vendored context avoids BuildKit SSH mounts while retaining the
# exact private dependency revision.
DOCKER_IMAGE ?= lunet-advisory-lock
DOCKER_PLATFORM ?= linux/arm64
docker-build: build lunet-runtime
	@context=$$(mktemp -d "$(CURDIR)/.tmp/docker-context.XXXXXX"); \
	tools/docker_prepare_context.sh "$$context"; \
	server=$$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}'); \
	[ "$$server" = "$(DOCKER_PLATFORM)" ] || { \
		echo "ERROR: docker daemon is $$server; native $(DOCKER_PLATFORM) is required (no emulation)" >&2; exit 1; \
	}; \
	docker build --platform $(DOCKER_PLATFORM) -f "$$context/docker/Dockerfile" -t $(DOCKER_IMAGE) "$$context"; \
	image=$$(docker image inspect --format '{{.Os}}/{{.Architecture}}' $(DOCKER_IMAGE)); \
	[ "$$image" = "$(DOCKER_PLATFORM)" ] || { \
		echo "ERROR: built image is $$image, expected native $(DOCKER_PLATFORM)" >&2; exit 1; \
	}

docker-simulation: docker-build $(SIM_BIN)
	SIM_BIN=$(abspath $(SIM_BIN)) DOCKER_IMAGE=$(DOCKER_IMAGE) DOCKER_PLATFORM=$(DOCKER_PLATFORM) SIM_DURATION=$(SIM_DURATION) tests/docker_simulation.sh

# Native extensions: one Rust crate per directory under ext/.
ext: ext-test
	cargo build --release --manifest-path ext/advisory_lock/Cargo.toml

ext-check:
	cargo fmt --manifest-path ext/advisory_lock/Cargo.toml -- --check
	cargo clippy --manifest-path ext/advisory_lock/Cargo.toml --all-targets -- -D warnings

ext-test: ext-check
	cargo test --manifest-path ext/advisory_lock/Cargo.toml

docs:
	docs/docs

clean:
	rm -rf build
