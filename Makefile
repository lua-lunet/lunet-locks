# Single entrypoint for every workflow. See CONVENTIONS.md.

LUAJIT_DIR ?= $(shell brew --prefix luajit 2>/dev/null || echo /usr/local)
LUAROCKS   ?= luarocks --lua-version=5.1 --lua-dir=$(LUAJIT_DIR) --tree=.rocks
CYAN       ?= .rocks/bin/cyan
TESTED     ?= .rocks/bin/tested
CERU       ?= .rocks/bin/ceru
LUNET_VERSION := v0.10.0
LUNET_ROOT := .lunet/$(LUNET_VERSION)
LUNET_RUN := $(LUNET_ROOT)/lunet-run
LUNET_OS := $(shell uname -s)
LUNET_ARCH := $(shell uname -m)

ifeq ($(LUNET_OS),Darwin)
LUNET_ARCHIVE := lunet-macos.tar.gz
LUNET_SHA256 := 8880dd6d0caf600760379e1476d1dc6343a5214938f37118c5588d533fef50a9
else ifeq ($(LUNET_OS)-$(LUNET_ARCH),Linux-x86_64)
LUNET_ARCHIVE := lunet-linux-amd64.tar.gz
LUNET_SHA256 := 979acc8669d644fd2276d328016f7a54916b444b652452abd6cd16fcbbe8ba94
else ifeq ($(LUNET_OS)-$(LUNET_ARCH),Linux-aarch64)
LUNET_ARCHIVE := lunet-linux-arm64.tar.gz
LUNET_SHA256 := e651a9d7b75a53be631a74f5f7fffd332f8a2f3e388170385686369fe0c51d3f
else
$(error Unsupported Lunet runtime platform $(LUNET_OS)/$(LUNET_ARCH); v0.10.0 ships macOS, Linux amd64, and Linux arm64 archives)
endif

LUNET_URL := https://github.com/lua-lunet/lunet/releases/download/$(LUNET_VERSION)/$(LUNET_ARCHIVE)
LUNET_ARCHIVE_PATH := $(LUNET_ROOT)/$(LUNET_ARCHIVE)
# Every tracked .tl in the tree is gate source: src, tests, tools, and
# console — one cyan check gate covers them all (`cyan build` alone does
# not reach outside its source_dir, and no .tl may sit outside a gate).
# tests/fixtures is test DATA, not source: two fixtures are deliberately
# type-broken, and their failing check output is what
# tests/teal_learning_test.tl asserts — the directory stays out of the gate.
TEAL_SOURCES = $(filter-out tests/fixtures/%,$(shell git ls-files '*.tl'))
# POSIX bin helpers carry no logic and still get the discipline: shellcheck
# plus the client-signal behavioural smoke.
SIGNAL_BIN := examples/lease-sequencer/bin
# The example crate's bench feature shape (the flight-recorder rig
# lane). The lint gate runs the example crate in BOTH shapes.
BENCH_DIR := examples/lease-sequencer
BENCH_FEATURES := flight-recorder
# The advisory-lock crate's Compliance ABI shape (docs/src/compliance-abi.md).
# The lint and test gates run the crate in BOTH shapes: the default one
# proves the nine unsafe_* exports are absent from the production build,
# this one proves they are all there.
COMPAT_FEATURES := compatibility_suite
# The corpus suite runs the same two shapes, from the outside: the
# production library must still expose no `unsafe_` symbol, and the
# gated library must expose all nine and replay the upstream corpus over
# the transport. The gated library builds into a target directory of its
# own so the production cdylib under `target/release` is never replaced
# by it — the door stays shut for anything that loads that path.
COMPAT_TARGET := $(CURDIR)/ext/advisory_lock/target/compliance
COMPAT_SOEXT := $(if $(filter Darwin,$(LUNET_OS)),dylib,so)
COMPAT_LIB := $(COMPAT_TARGET)/debug/liblunet_advisory_lock.$(COMPAT_SOEXT)
COMPAT_RUN_DIR := $(CURDIR)/.tmp/compliance/gate

.PHONY: init deps build check test smoke simulation simulation-test lunet-runtime docs clean ext ext-check ext-test example-check fmt lint hooks sh-check sh-smoke docker-build docker-simulation sanity release-images build-proof package package-verify bench compliance-lib compliance-production-shape compliance-suite-shape compliance-suite

init:
	@command -v mise >/dev/null 2>&1 || { echo "ERROR: mise is not on PATH. Install it from https://mise.jdx.dev and try again."; exit 1; }
	mise install
	$(MAKE) deps

deps:
	# The versions are load-bearing pins: tested 0.4.0 changed its API to
	# instance methods (the suite calls module-level `tested.test`), and
	# cerulean 1.9.1 changed the formatting rules the tree is formatted to.
	$(LUAROCKS) install cyan 0.4.1-1
	$(LUAROCKS) install tested 0.3.0-1
	$(LUAROCKS) install cerulean 1.9.0-1
	# tl (Teal) for the LuaJIT 5.1 ABI: the tooling loader and the
	# `tl check` gate over tools/lib. A 5.5-tree tl is invisible under
	# LuaJIT, so the Lua version is pinned here like everywhere else.
	@$(LUAROCKS) list tl 2>/dev/null | grep -q "^tl$$" || $(LUAROCKS) install tl
	# luasocket: the tooling's TCP client. The Debian luarocks package
	# happens to pull lua-socket system-wide; brew's does not, so the
	# project tree owns the rock and every runner resolves it locally.
	$(LUAROCKS) install luasocket

fmt:
	$(CERU) src tests tools console

lint:
	$(CERU) --check src tests tools console

hooks:
	git config core.hooksPath .githooks

build: ext
	$(CYAN) build --prune

sh-check:
	shellcheck $(SIGNAL_BIN)/*.sh

sh-smoke:
	$(SIGNAL_BIN)/client-signal-smoke.sh

# The gated library, built apart from the production one so the two
# shapes can never be confused for one another on disk.
compliance-lib:
	COMPATIBILITY_SUITE_ALLOW_DIRTY=1 CARGO_TARGET_DIR=$(COMPAT_TARGET) \
		cargo build --lib --manifest-path ext/advisory_lock/Cargo.toml \
		--features "$(COMPAT_FEATURES)"

# Shape one: the production library's symbol table. Zero `unsafe_`
# symbols, read from the built cdylib rather than asserted in a comment.
compliance-production-shape: build
	tools/compliance_gate.lua --shape production

# Shape two: the gated library's symbol table plus the upstream corpus
# replayed over the transport against the Lua compliance host.
# `contract.hurl` runs first (it is first in the gate's file list): it
# covers the endpoints that replay no case, so a transport fault names
# itself before any case is reached.
compliance-suite-shape: build lunet-runtime compliance-lib
	tools/compliance_gate.lua --shape compatibility_suite \
		--run-dir $(COMPAT_RUN_DIR)

compliance-suite: compliance-production-shape compliance-suite-shape

check: build lint example-check sh-check sh-smoke compliance-suite
	$(CYAN) check $(TEAL_SOURCES)

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
	LUNET_RUN=$(abspath $(LUNET_RUN)) CYAN=$(abspath $(CYAN)) tools/smoke.lua

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

# The on-the-bench harness (docs/src/bench-harness.md): the
# flight-recorder rig, three forked nodes on real UDP, the force-fed
# store, and the CAS-chain oracle. The flight build wants a clean tree;
# the bench's tapes are development artefacts, not release evidence, so
# the target carries the override (the tapes stamp dirty and every
# reader annotates it). Release evidence stays the clean-commit
# softball run.
bench:
	cd $(BENCH_DIR) && FLIGHT_RECORDER_ALLOW_DIRTY=1 cargo build \
		--features "$(BENCH_FEATURES)" \
		--bin lease-sequencer --bin skaffold_bench_driver
	$(BENCH_DIR)/target/debug/skaffold_bench_driver \
		--node-bin $(abspath $(BENCH_DIR)/target/debug/lease-sequencer) \
		--run-dir $(CURDIR)/.tmp/bench-run

# Plain multi-stage `docker build`. The prepared context carries the vendored
# dependency sources and the ext/uvrr-core submodule source (the manifest's
# [patch] section resolves vrr-core to the submodule), so nothing is fetched
# over the network inside Docker and no BuildKit mounts are needed.
DOCKER_IMAGE ?= lunet-advisory-lock
DOCKER_PLATFORM ?= native
AOF_IMAGE ?= ghcr.io/lua-lunet/lunet-locks/tbio-core:v0.17.9-lunet.5-arm64
docker-build: build lunet-runtime
	@context=$$(mktemp -d "$(CURDIR)/.tmp/docker-context.XXXXXX"); \
	tools/docker_prepare_context.sh "$$context" || exit 1; \
	server=$$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}'); \
	[ "$(DOCKER_PLATFORM)" = native ] || [ "$$server" = "$(DOCKER_PLATFORM)" ] || { \
		echo "ERROR: docker daemon is $$server; cross-platform builds are not supported" >&2; exit 1; \
	}; \
	docker build --platform "$$server" \
		--build-arg LUNET_LOCKS_HEAD=$$(git rev-parse HEAD) \
		--build-arg AOF_IMAGE=$(AOF_IMAGE) \
		-f "$$context/docker/Dockerfile" -t $(DOCKER_IMAGE) "$$context"; \
	image=$$(docker image inspect --format '{{.Os}}/{{.Architecture}}' $(DOCKER_IMAGE)); \
	[ "$$image" = "$$server" ] || { \
		echo "ERROR: built image is $$image, expected native $$server" >&2; exit 1; \
	}

docker-simulation: docker-build $(SIM_BIN)
	SIM_BIN=$(abspath $(SIM_BIN)) DOCKER_IMAGE=$(DOCKER_IMAGE) DOCKER_PLATFORM=$(DOCKER_PLATFORM) SIM_DURATION=$(SIM_DURATION) tests/docker_simulation.sh

# The build-confirmation gate: MANDATORY before every cloud test run
# (see docs/src/testing-on-cloud.md and docs/src/build-and-tests.md).
# (1) The tree is clean and the work is a commit at HEAD — no run ever
# ships from a dirty tree. (2) The fastbuild sanity payload runs inside
# colima: `cargo check` for BOTH linux triples (aarch64 native + x86
# cross-built natively by rustc — there is no emulated RUN step and no
# binfmt registration anywhere), every crate, the prod and
# flight-recorder shapes, against the classic manifests-first deps
# layer cache (no BuildKit; there are no volume mounts — artifacts leave
# via `docker create` + `docker cp` when a stage produces them).
# This is a build confirmation, not a deployment and not testing:
# nothing is deployed and nothing from the image is run — the build IS
# the proof that the commit does not rely on the laptop. The verdict and
# the commit hash print last; tee them into the run dir.

sanity:
	@test -z "$$(git status --porcelain)" || { \
		echo "ERROR: the tree is dirty. Commit the work first (the gate ships HEAD, never the working tree):" >&2; \
		git status --short >&2; exit 1; \
	}; \
	docker version >/dev/null 2>&1 || { \
		echo "ERROR: docker daemon not reachable. On macOS start colima first (colima start; docker context use colima)." >&2; exit 1; \
	}; \
	env DOCKER_BUILDKIT=0 docker build \
		--build-arg LUNET_LOCKS_HEAD=$$(git rev-parse HEAD) \
		--target check -t lunet-locks:sanity \
		-f docker/Dockerfile.fastbuild .; \
	echo "SANITY: cargo check green on colima for aarch64-unknown-linux-gnu + x86_64-unknown-linux-gnu (cdylib + rig crates, prod and flight-recorder shapes; paxe-core prod and kat shapes) at commit $$(git rev-parse --short=12 HEAD). Nothing deployed, nothing run from the image — the build is the proof."

# The RELEASE dual-arch image gate (docs/src/build-and-release.md):
# both linux architecture images built from the committed tree, each
# carrying BOTH binaries (prod and flight-recorder), with the native
# cross mechanics inside the fastbuild stages: the x86 binaries are
# built natively by rustc in the aarch64 container, and the amd64 IMAGE
# is assembled COPY-only on the amd64 base image — no amd64 code ever
# runs at build time, no qemu, no emulation, no BuildKit. This replaces
# the old emulated x86 image build as the release proof; `make sanity`
# (above) remains the pre-cloud gate.
build-proof:
	tools/release_images.sh local

# The full release flow (docs/src/build-and-release.md): a clean tree at
# the tag, the gate + flight builds, the dual-arch images, and the
# ghcr.io push via gh (RELEASE_PUSH=--push or no credentials, the flow
# does everything short of the push).
TAG ?=
release-images:
	@test -n "$(TAG)" || { echo "ERROR: usage: make release-images TAG=vX.Y.Z [RELEASE_PUSH=--push]" >&2; exit 64; }; \
	tools/release_images.sh $(TAG) $(RELEASE_PUSH)

# Native extensions: one Rust crate per directory under ext/.
ext: ext-test
	cargo build --release --manifest-path ext/advisory_lock/Cargo.toml
	cargo build --release --manifest-path ext/lunet-locks-aof/Cargo.toml
	cargo build --release --manifest-path ext/paxe-core/Cargo.toml

ext-check:
	cargo fmt --manifest-path ext/advisory_lock/Cargo.toml -- --check
	cargo clippy --manifest-path ext/advisory_lock/Cargo.toml --all-targets -- -D warnings
	COMPATIBILITY_SUITE_ALLOW_DIRTY=1 cargo clippy --manifest-path ext/advisory_lock/Cargo.toml --all-targets --features "$(COMPAT_FEATURES)" -- -D warnings
	cargo fmt --manifest-path ext/lunet-locks-aof/Cargo.toml -- --check
	cargo clippy --manifest-path ext/lunet-locks-aof/Cargo.toml --all-targets -- -D warnings
	mise exec -- zig fmt --check ext/lunet-locks-aof/zig/src
	cargo fmt --manifest-path ext/paxe-core/Cargo.toml -- --check
	cargo clippy --manifest-path ext/paxe-core/Cargo.toml --all-targets -- -D warnings

# The example crate is gated like the ext crates, in BOTH its feature
# shapes: the default build and the flight-recorder bench build. The
# bench shape's flight-recorder feature trips the AOF crate's clean-commit
# guard on a development tree, so the line carries the bench target's
# override (a clean CI checkout never trips the guard).
example-check:
	cargo fmt --manifest-path $(BENCH_DIR)/Cargo.toml -- --check
	cargo clippy --manifest-path $(BENCH_DIR)/Cargo.toml --all-targets -- -D warnings
	FLIGHT_RECORDER_ALLOW_DIRTY=1 cargo clippy --manifest-path $(BENCH_DIR)/Cargo.toml --all-targets --features "$(BENCH_FEATURES)" -- -D warnings

ext-test: ext-check
	cargo test --manifest-path ext/advisory_lock/Cargo.toml
	COMPATIBILITY_SUITE_ALLOW_DIRTY=1 cargo test --manifest-path ext/advisory_lock/Cargo.toml --features "$(COMPAT_FEATURES)"
	cargo test --manifest-path ext/lunet-locks-aof/Cargo.toml
	cd ext/lunet-locks-aof/zig && mise exec -- zig build test $(ZIG_TEST_FLAGS)
	cargo test --manifest-path ext/paxe-core/Cargo.toml

# The vendored checksum asserts AES hardware at comptime (vsr/checksum.zig);
# Linux arm64 CI resolves a generic CPU baseline that lacks the feature, so
# the arm64 build carries the flag explicitly — every deployment target
# carries ARMv8 AES (the runners are Ampere Altra, the cloud VMs Graviton).
ifeq ($(shell uname -m),aarch64)
ZIG_TEST_FLAGS := -Dcpu=baseline+aes
else
ZIG_TEST_FLAGS :=
endif

# Release packaging (tagged CI builds). Target keys match the CI matrix;
# the archive layout is documented in tests/package_release.sh.
PACKAGE_TARGET := $(shell if [ "$(LUNET_OS)" = Darwin ]; then echo macos; \
	elif [ "$(LUNET_OS)-$(LUNET_ARCH)" = Linux-x86_64 ]; then echo linux-amd64; \
	elif [ "$(LUNET_OS)-$(LUNET_ARCH)" = Linux-aarch64 ]; then echo linux-arm64; \
	else echo unknown; fi)
PACKAGE_ARCHIVE ?= lunet-locks-$(PACKAGE_TARGET).tar.gz

package: build
	tests/package_release.sh $(PACKAGE_TARGET) $(PACKAGE_ARCHIVE)

package-verify: lunet-runtime
	LUNET_RUN=$(abspath $(LUNET_RUN)) tests/package_verify.sh $(PACKAGE_ARCHIVE)

docs:
	docs/docs

clean:
	rm -rf build
