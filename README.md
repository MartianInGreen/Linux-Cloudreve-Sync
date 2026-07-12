# Cloudreve Sync for Linux

A small native desktop companion for two-way folder synchronization with a
Cloudreve WebDAV remote. The backend and GUI are both written in Rust.

## Features

- Map any number of local folders to Cloudreve folders.
- Poll for local and remote changes and transfer them automatically.
- Detect simultaneous local/remote edits and ask which copy to keep.
- Show the current file operation, transfer counts, transferred size, and recent activity.
- Persist configuration and sync snapshots between launches.
- Use Cloudreve's version-independent WebDAV interface.

This is an early release. Keep backups of important files. System tray
integration, autostart, ignore patterns, and encrypted credential storage are
not implemented yet.

## Build and run

Install the Rust toolchain and common Linux windowing development packages, then:

```sh
cargo run --release
```

On Debian/Ubuntu, eframe normally needs:

```sh
sudo apt install build-essential pkg-config libx11-dev libxi-dev libgl1-mesa-dev libxkbcommon-dev
```

## Setup

1. Enable WebDAV in Cloudreve and copy its WebDAV endpoint URL.
2. Enter the endpoint, Cloudreve username, and WebDAV password in the app.
3. Add a local folder and enter its remote path, for example `Documents/work`.
4. Save, then select **Sync now**.

Configuration is stored in `~/.config/cloudreve-sync/config.json`. The password
is currently stored in that file, so ensure your home directory is private.

## Sync behavior

The app caches BLAKE3 hashes and file metadata in the local Turso/libSQL database
`~/.config/cloudreve-sync/local-index.db`, and compares remote ETags to the last
successful snapshot. Unchanged files do not need to be read and hashed again.
A change or deletion on only one side is applied
to the other side automatically. If both copies changed since the snapshot, the
app pauses that file and displays a conflict with **Keep local** and **Keep
remote** actions.

If Cloudreve rejects a path during upload, the app encodes unsafe folder names
with `.dissalowed-folder` and encodes the filename with `.dissalowed-type`. The
encoding and suffixes are removed when files are downloaded, restoring the exact
original local path.
