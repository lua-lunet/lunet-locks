# Single entrypoint for every workflow. See CONVENTIONS.md.
# Override LUAJIT_DIR / LUNET_URL for non-Homebrew or non-macOS hosts.

LUAJIT_DIR ?= $(shell brew --prefix luajit 2>/dev/null || echo /usr/local)
LUAROCKS   ?= luarocks --lua-version=5.1 --lua-dir=$(LUAJIT_DIR) --tree=.rocks
CYAN       ?= .rocks/bin/cyan
TESTED     ?= .rocks/bin/tested
LUNET_BIN  ?= bin/lunet-run
LUNET_URL  ?= https://github.com/lua-lunet/lunet/releases/download/v0.7.0/lunet-macos.tar.gz

# Teal outside source_dir, which `cyan build` does not reach.
CHECK_SOURCES = scripts/lib/proc.tl \
                scripts/lib/harness.tl \
                scripts/lib/scaffold_harness.tl \
                scripts/lib/nginx_counter.tl \
                tests/hello_world_test.tl \
                tests/teal_learning_test.tl \
                tests/vrr_ffi_test.tl

.PHONY: init deps fetch build check test harness load-test docs clean ext ext-check ext-test

init:
	@command -v mise >/dev/null 2>&1 || { echo "ERROR: mise is not on PATH. Install it from https://mise.jdx.dev and try again."; exit 1; }
	mise install
	$(MAKE) deps
	$(MAKE) fetch

deps:
	$(LUAROCKS) install cyan
	$(LUAROCKS) install tested

fetch:
	mkdir -p bin .tmp
	curl -fsSL "$(LUNET_URL)" -o .tmp/lunet.tar.gz
	tar -xzf .tmp/lunet.tar.gz -C bin
	chmod +x $(LUNET_BIN)

build: ext
	$(CYAN) build --prune

check: build
	$(CYAN) check $(CHECK_SOURCES)

test: check
	$(TESTED) tests

# Native extensions: one Rust crate per directory under ext/.
ext: ext-test
	cargo build --release --manifest-path ext/vrr/Cargo.toml

ext-check:
	cargo fmt --manifest-path ext/vrr/Cargo.toml -- --check
	cargo clippy --manifest-path ext/vrr/Cargo.toml --all-targets -- -D warnings

ext-test: ext-check
	cargo test --manifest-path ext/vrr/Cargo.toml

# Script/harness proof only; not part of the default forward pass.
harness: check
	./scripts/nginx-counter-demo

load-test: check
	./scripts/scaffold-load-test

docs:
	docs/docs

clean:
	rm -rf build
