#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
prefix="${PREFIX:-${HOME}/.local}"
bin_dir="${prefix}/bin"
applications_dir="${prefix}/share/applications"
pixmaps_dir="${prefix}/share/pixmaps"

printf 'Building Cloudreve Sync...\n'
cargo build --release --locked --manifest-path "${project_dir}/Cargo.toml"

install -Dm755 "${project_dir}/target/release/cloudreve-sync" "${bin_dir}/cloudreve-sync"
install -Dm644 "${project_dir}/logo-sync.png" "${pixmaps_dir}/cloudreve-sync.png"

desktop_file="${applications_dir}/cloudreve-sync.desktop"
mkdir -p "${applications_dir}"
while IFS= read -r line; do
  case "${line}" in
    Exec=*) printf 'Exec=%s\n' "${bin_dir}/cloudreve-sync" ;;
    *) printf '%s\n' "${line}" ;;
  esac
done < "${project_dir}/assets/cloudreve-sync.desktop" > "${desktop_file}"
chmod 644 "${desktop_file}"

if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "${applications_dir}" >/dev/null 2>&1 || true
fi

printf '\nCloudreve Sync installed successfully.\n'
printf 'Binary:  %s\n' "${bin_dir}/cloudreve-sync"
printf 'Launcher: %s\n' "${desktop_file}"
printf '\nRun it with: %s\n' "${bin_dir}/cloudreve-sync"
