#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
builder_image="rust:1.89-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff"
cargo_deb_version="3.7.0"

mkdir -p "$repo_dir/.cargo" "$repo_dir/target" "$repo_dir/dist"

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e CARGO_HOME=/cargo \
  -e CARGO_TARGET_DIR=/workspace/target \
  -v "$repo_dir/.cargo:/cargo" \
  -v "$repo_dir:/workspace" \
  -w /workspace \
  "$builder_image" \
  bash -c "
    set -euo pipefail
    cargo install cargo-deb --locked --version '$cargo_deb_version'
    find dist -maxdepth 1 -type f -name 'swarm-mcp_*.deb' -delete
    cargo deb --locked --output dist
  "

shopt -s nullglob
packages=("$repo_dir"/dist/swarm-mcp_*.deb)
if [ "${#packages[@]}" -ne 1 ]; then
  echo "Expected exactly one swarm-mcp package, found ${#packages[@]}" >&2
  exit 1
fi
sha256sum "${packages[0]}"
