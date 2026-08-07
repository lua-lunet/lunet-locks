# Single entrypoint for every workflow. See CONVENTIONS.md.

LUAJIT_DIR ?= $(shell brew --prefix luajit 2>/dev/null || echo /usr/local)
LUAROCKS   ?= luarocks --lua-version=5.1 --lua-dir=$(LUAJIT_DIR) --tree=.rocks
CYAN       ?= .rocks/bin/cyan
TESTED     ?= .rocks/bin/tested
CERU       ?= .rocks/bin/ceru
# Teal outside source_dir, which `cyan build` does not reach.
CHECK_SOURCES = tests/teal_learning_test.tl \
                tests/vrr_ffi_test.tl

.PHONY: init deps build check test docs clean ext ext-check ext-test fmt lint hooks

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
	$(TESTED) tests

# Native extensions: one Rust crate per directory under ext/.
ext: ext-test
	cargo build --release --manifest-path ext/vrr/Cargo.toml

ext-check:
	cargo fmt --manifest-path ext/vrr/Cargo.toml -- --check
	cargo clippy --manifest-path ext/vrr/Cargo.toml --all-targets -- -D warnings

ext-test: ext-check
	cargo test --manifest-path ext/vrr/Cargo.toml

docs:
	docs/docs

clean:
	rm -rf build
