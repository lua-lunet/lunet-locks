# Contributing

This repository accepts patches through pull requests. A patch must carry
a test that reproduces the bug it fixes or exercises the feature it adds;
a report whose reproduction test fails on the unchanged code is a
confirmed finding, and the same test is the acceptance criterion for the
fix. Reporters are asked to post a patch with a test to reproduce any bug.

## Red and green

Preserve the failing result of the reproduction test before touching
production code; that red run is the evidence the patch is judged against.
Make the minimum production change that turns that same test green, then
re-run the affected suite and the full gates below. If the reproduction
passes without a production-code change, the finding is closed as
unreproducible and no change is made.

## Gates

A patch is reviewed only when the repository gates pass:

```console
make check    # Cyan type checks and Cerulean formatting verification
make test     # the gate above plus the tested suite
```

Teal sources are formatted with Cerulean (`make fmt` formats in place,
`make check` rejects unformatted code); `make hooks` enables the
pre-commit formatting guard once after clone. Changes to the native
adapter or its vendored submodule must additionally pass the Rust gates
(`make ext-test`: `cargo fmt`, `clippy` with warnings denied, and
`cargo test`), which `make build` runs.

## Documentation

Every document on `main` states what the system is and does as fact, as at
the release cut. Write the documentation first, in that factual voice, and
let it drive the implementation. Plans, status banners, and
contemporaneous commentary do not belong in the docs tree.

## Upstream engagement

The replication core is the vendored `ext/uvrr-core` submodule. A serious
correctness, safety, or replication bug found in that tree is a
stop-and-report issue: file the issue against
[lua-lunet/uvrr-core](https://github.com/lua-lunet/uvrr-core) and surface
it to the coordinator before changing the submodule; bugs in vendored or
derived code are reported to this project first, then upstreamed to both
communities (see [`ATTRIBUTIONS.md`](../ATTRIBUTIONS.md)).

## Attribution

Any change that adds, removes, or re-pins a dependency or third-party
asset updates `ATTRIBUTIONS.md` in the same patch, with the entry's
version and verified licence.
