# Lock Admin console — no-framework SPA behind nginx with a bun mock backend.
# Lifecycle logic lives in tooling/lib/consolectl.tl (typed, testable); the
# bin/*.sh wrappers are thin POSIX sh and carry no logic.

LUAROCKS ?= luarocks --lua-version=5.1 --tree=../.rocks
PORT     ?= 8480
MOCK_PORT ?= 8481
# The .mjs type gate's compiler, pinned: the SPA has no build step and no
# node_modules, so the runner installs TypeScript globally in its setup step
# (`npm install --global typescript@6.0.3`) and the gate below runs offline.
TSC ?= tsc

.PHONY: init up down status logs vendor vendor-lock mjs-check check clean

init:
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "Installing Teal toolchain into ../.rocks ..."; \
		$(MAKE) -C .. deps; }
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "ERROR: tl (Teal) not available in ../.rocks" >&2; exit 1; }
	@echo "console toolchain: OK"

up: init
	@PORT=$(PORT) MOCK_PORT=$(MOCK_PORT) bin/up.sh

down:
	@bin/down.sh

status:
	@bin/status.sh

logs:
	@tail -n 40 -f tmp/mock.log tmp/nginx-error.log 2>/dev/null

# Vendor Apache ECharts locally (pinned) and, with vendor-lock, mint the
# sha256-named immutable copy plus the nginx fragment that redirects the
# plain asset path to it.
ECHARTS_VERSION ?= 5.6.0

vendor:
	@ECHARTS_VERSION=$(ECHARTS_VERSION) bin/vendor.sh

vendor-lock:
	@ECHARTS_VERSION=$(ECHARTS_VERSION) bin/vendor.sh lock

# Type-check the SPA from its JSDoc (tsconfig.json). No package.json, no
# node_modules, no build step: tsc only reads (noEmit) and the .mjs files
# ship as-is. Fast enough to run on its own as an inner loop.
mjs-check:
	$(TSC) -p tsconfig.json

check: init mjs-check
	shellcheck bin/*.sh
	@../.rocks/bin/cyan check tooling/lib/consolectl.tl

clean: down
	rm -rf tmp
