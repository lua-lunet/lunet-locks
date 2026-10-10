# Lock Admin console — no-framework SPA behind nginx with a bun mock backend.
# Lifecycle logic lives in tooling/lib/consolectl.tl (typed, testable); the
# bin/*.sh wrappers are thin POSIX sh and carry no logic.

LUAROCKS ?= luarocks --lua-version=5.1 --tree=../.rocks
PORT     ?= 8480
MOCK_PORT ?= 8481
# The .mjs type gate's compiler, pinned: the SPA has no build step and no
# node_modules, so the runner installs TypeScript globally in its setup step
# (`npm install --global typescript@6.0.3`) and the gate below runs offline.
# The version assert under mjs-check makes a WRONG compiler refuse instead
# of gate differently — the same version by construction, locally and in CI.
TSC        ?= tsc
TSC_VERSION ?= 6.0.3

.PHONY: init up down status logs vendor vendor-lock mjs-check mjs-check-negative check clean

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
# ship as-is. Fast enough to run on its own as an inner loop. The version
# assert first: strict over types.mjs is exactly where tsc versions
# disagree, so a different compiler is a different gate and refuses.
mjs-check:
	@actual=$$($(TSC) --version | awk '{print $$NF}'); \
		test "$$actual" = "$(TSC_VERSION)" || { \
			echo "ERROR: the .mjs gate pins typescript@$(TSC_VERSION); this machine's tsc is $$actual" >&2; \
			echo "       install the pinned compiler (npm install --global typescript@$(TSC_VERSION))" >&2; \
			exit 1; }; \
		echo "tsc $$actual: pinned OK"
	$(TSC) -p tsconfig.json

# The gate's negative proof: the fixture carries one deliberate type error
# and tsc must refuse it. If the fixture ever type-checks clean — the
# tsconfig mis-scoped, checkJs off — the gate is checking nothing, and this
# target is the one that says so.
mjs-check-negative:
	@test -f tooling/mjs-gate-fixture/broken.mjs || { \
		echo "ERROR: the negative fixture is gone; the gate's ability to fail is unproven" >&2; \
		exit 1; }
	@if $(TSC) -p tsconfig-negative.json >/dev/null 2>&1; then \
		echo "ERROR: the negative fixture type-checked CLEAN — the gate cannot fail and is not a gate" >&2; \
		exit 1; \
	else \
		echo "tsc negative fixture: refused as required — the gate can fail"; \
	fi

check: init mjs-check mjs-check-negative
	shellcheck bin/*.sh
	@../.rocks/bin/cyan check tooling/lib/consolectl.tl

clean: down
	rm -rf tmp
