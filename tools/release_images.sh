#!/bin/sh
# The release image flow: from the committed tagged tree to the
# dual-arch release images (and, with --push, onto ghcr.io).
#
# Usage: tools/release_images.sh TAG [--push]
#   TAG     the release tag (a v* tag whose commit is HEAD), e.g. v0.1.0;
#           the literal word "local" is the build-proof mechanism's
#           local run (images named lunet-locks:local-<sha8>-{arm64,amd64}
#           — the release proof, never pushed)
#   --push  authenticate through `gh` and push both arch images plus the
#           multi-arch aggregate manifest to ghcr.io (without
#           credentials the local images are still built and kept, and
#           the push steps are printed instead of executed)
#
# The flow:
#   1. assert the tree is clean, the tag exists, and the tag IS HEAD;
#   2. `make sanity` — the gate (clean commit + cross-check both
#      triples inside colima) — then the flight-recorder build;
#   3. `make build` — the Cyan service tree the image runs;
#   4. build the fastbuild artifacts + rootfs images and extract both
#      via `docker create` + `docker cp` — no volume mounts, no BuildKit;
#      the classic manifests-first deps layer is the cache;
#   5. assemble the dual-arch images, each carrying BOTH binaries (prod
#      and flight-recorder), tagged lunet-locks:<tag>-amd64 / -arm64;
#      the aarch64 image builds natively in the aarch64 VM; the amd64
#      image is assembled COPY-only on an amd64 base — no amd64 code
#      runs at build time, no emulation anywhere;
#   6. with --push: authenticate via `gh`, push both arch images and
#      the aggregate manifest to ghcr.io.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

tag_request=${1:-}
shift || true
push=0
for arg in "$@"; do
    case "$arg" in
        --push) push=1 ;;
        *) echo "release images: unknown argument: $arg" >&2; exit 64 ;;
    esac
done

# 1. The tree is clean; a v-tag call also asserts the tag IS HEAD.
if [ -n "$(git status --porcelain)" ]; then
    echo "release images: the tree is dirty. Cut the release from a clean tree:" >&2
    git status --short >&2
    exit 1
fi
head=$(git rev-parse HEAD)
case "$tag_request" in
    local)
        # The release-image build proof: no tag push, images named to
        # the commit they carry.
        tag=local-$(git rev-parse --short=8 HEAD)
        ;;
    v[0-9]*)
        tag=$tag_request
        tag_commit=$(git rev-parse "$tag^{commit}" 2>/dev/null) || {
            echo "release images: tag does not exist: $tag" >&2
            exit 1
        }
        if [ "$tag_commit" != "$head" ]; then
            echo "release images: tag $tag is not HEAD ($tag_commit != $head)" >&2
            exit 1
        fi
        ;;
    "") echo "release images: usage: tools/release_images.sh TAG [--push]" >&2; exit 64 ;;
    *) echo "release images: TAG must be a v-tag (e.g. v0.1.0) or 'local', got: $tag_request" >&2; exit 64 ;;
esac
echo "release images: $tag = $(git rev-parse --short=12 HEAD)"

# 2. The gate (clean-commit assert + cross-check both triples) and the
#    flight-recorder build on the host toolchain.
make sanity
if ! cargo build --locked --features flight-recorder \
        --manifest-path examples/lease-sequencer/Cargo.toml \
        >.tmp/release-images-flight.log 2>&1; then
    echo "release images: flight-recorder build failed (see .tmp/release-images-flight.log)" >&2
    exit 1
fi
echo "release images: flight-recorder build green on the host"

# 3. The Cyan service tree the image carries.
make build

# 4. The fastbuild artifacts + rootfs, extracted to the staging tree.
staging=.tmp/release-images-staging
rm -rf "$staging"
mkdir -p "$staging"

env DOCKER_BUILDKIT=0 docker build \
    --build-arg LUNET_LOCKS_HEAD="$head" \
    --target artifacts -t lunet-locks:artifacts \
    -f docker/Dockerfile.fastbuild .
for arch in aarch64 amd64; do
    cid=$(docker create lunet-locks:artifacts)
    docker cp "$cid:/out/$arch" "$staging"
    docker rm "$cid" >/dev/null
done

env DOCKER_BUILDKIT=0 docker build \
    --build-arg LUNET_LOCKS_HEAD="$head" \
    --target rootfs -t lunet-locks:rootfs \
    -f docker/Dockerfile.fastbuild .
cid=$(docker create lunet-locks:rootfs)
docker cp "$cid:/out/amd64-rootfs" "$staging"
docker rm "$cid" >/dev/null

# 5. Assemble the dual-arch release images.
#    arm64: native build (context: lib/, demo/bin/, build/, docker/).
stage_aarch64="$staging/.aarch64"
mkdir -p "$stage_aarch64/lib" "$stage_aarch64/demo/bin" "$stage_aarch64/docker"
cp "$staging/aarch64"/liblunet_advisory_lock*.so "$stage_aarch64/lib/"
cp "$staging/aarch64"/lease-sequencer "$stage_aarch64/demo/bin/lease-sequencer"
cp "$staging/aarch64"/lease-sequencer-flight "$stage_aarch64/demo/bin/lease-sequencer-flight"
cp -R build "$stage_aarch64/build"
cp docker/entrypoint.sh docker/cluster.jsonl "$stage_aarch64/docker/"
env DOCKER_BUILDKIT=0 docker build \
    -f docker/Dockerfile.release -t "lunet-locks:$tag-arm64" "$stage_aarch64"

#    amd64: COPY-only on the amd64 base. The base image's layers are
#    pulled for amd64 (a data pull), the runtime-rootfs overlay (the
#    amd64 packages unpacked in the fastbuild stage) and the app tree
#    are copied into the created-but-never-run container, and the image
#    is committed with the entrypoint config. No RUN step, so nothing
#    amd64 executes to build the image.
stage_amd64="$staging/.amd64"
mkdir -p "$stage_amd64/app/lib" "$stage_amd64/app/demo/bin" "$stage_amd64/app/docker"
cp "$staging/amd64"/liblunet_advisory_lock*.so "$stage_amd64/app/lib/"
cp "$staging/amd64"/lease-sequencer "$stage_amd64/app/demo/bin/lease-sequencer"
cp "$staging/amd64"/lease-sequencer-flight "$stage_amd64/app/demo/bin/lease-sequencer-flight"
cp -R build "$stage_amd64/app/build"
cp docker/entrypoint.sh docker/cluster.jsonl "$stage_amd64/app/docker/"
docker pull --platform linux/amd64 debian:bookworm-slim >/dev/null
cid=$(docker create --platform linux/amd64 debian:bookworm-slim)
docker cp "$staging/amd64-rootfs/." "$cid:/"
cp docker/entrypoint.sh docker/cluster.jsonl "$stage_amd64/app/docker/"
docker cp "$stage_amd64/app" "$cid:/app/"
docker commit \
    --change 'ENTRYPOINT ["/app/docker/entrypoint.sh"]' \
    --change 'ENV LUNET_ADVISORY_LOCK_LIB=/app/lib/liblunet_advisory_lock.so' \
    "$cid" "lunet-locks:$tag-amd64"
docker rm "$cid" >/dev/null

arch_arm=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "lunet-locks:$tag-arm64")
arch_amd=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "lunet-locks:$tag-amd64")
if [ "$arch_arm" != "linux/arm64" ]; then
    echo "release images: the arm image is $arch_arm, expected linux/arm64" >&2
    exit 1
fi
if [ "$arch_amd" != "linux/amd64" ]; then
    echo "release images: the amd64 image is $arch_amd, expected linux/amd64" >&2
    exit 1
fi
echo "RELEASE IMAGES: lunet-locks:$tag-arm64 linux/arm64 + lunet-locks:$tag-amd64 linux/amd64 (prod and flight-recorder binaries in both), from commit $(git rev-parse --short=12 HEAD)."

if [ "$push" = 0 ]; then
    echo "release images: not pushing (no --push). Images kept locally."
    exit 0
fi

# 6. Push via gh auth: ghcr.io/<owner>/lunet-locks.
token=$(gh auth token 2>/dev/null || true)
if [ -z "$token" ]; then
    echo "release images: no gh credentials. Implement the push by hand or in CI:" 2>&1
    echo "  gh auth token | docker login ghcr.io -u <login> --password-stdin" >&2
    echo "  docker tag lunet-locks:$tag-{arm64,amd64} ghcr.io/<owner>/lunet-locks:..." >&2
    echo "  docker push ghcr.io/<owner>/lunet-locks:$tag-{arm64,amd64}" >&2
    echo "  docker manifest create ghcr.io/<owner>/lunet-locks:$tag ghcr.io/<owner>/lunet-locks:$tag-{arm64,amd64}" >&2
    echo "  docker manifest push --purge ghcr.io/<owner>/lunet-locks:$tag" >&2
    exit 0
fi
owner=$(gh api user --jq .login)
echo "release images: pushing to ghcr.io/$owner/lunet-locks"
printf '%s' "$token" | docker login ghcr.io --username "$owner" --password-stdin
for archtag in arm64 amd64; do
    docker tag "lunet-locks:$tag-$archtag" "ghcr.io/$owner/lunet-locks:$tag-$archtag"
    docker push "ghcr.io/$owner/lunet-locks:$tag-$archtag"
done
docker manifest create "ghcr.io/$owner/lunet-locks:$tag" \
    "ghcr.io/$owner/lunet-locks:$tag-arm64" \
    "ghcr.io/$owner/lunet-locks:$tag-amd64" >/dev/null
docker manifest push --purge "ghcr.io/$owner/lunet-locks:$tag"
echo "RELEASE: dual-arch images pushed to ghcr.io/$owner/lunet-locks:$tag."
