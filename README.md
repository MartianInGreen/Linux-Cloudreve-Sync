# Cloudreve Sync for Linux

A small native desktop companion for two-way folder synchronization with a
Cloudreve WebDAV remote. The backend and GUI are both written in Rust.

## Features

- Map any number of local folders to Cloudreve folders.
- Poll for local and remote changes and transfer them automatically.
- Detect simultaneous local/remote edits and ask which copy to keep.
- Show the current file operation, transfer counts, transferred size, and recent activity.
- Continue syncing from a Linux system tray with show, sync-now, and quit actions.
- Optionally start automatically through the XDG desktop autostart mechanism.
- Apply gitignore-style patterns independently to each folder mapping.
- Select multiple immediate subfolders from a top directory in one step.
- Persist configuration and sync snapshots between launches.
- Use Cloudreve's version-independent WebDAV interface.
- Pause active synchronization for system suspend and reconnect after resume.
- Refuse unsafe bulk deletions when a mapped folder or storage backend disappears.

This is an early release. Keep backups of important files. System tray
integration, autostart, ignore patterns, and encrypted credential storage are
not implemented yet.

## Build and run

Install the Rust toolchain and common Linux windowing development packages, then:

```sh
cargo run --release
```

To build and install it for the current user under `~/.local`:

```sh
./install.sh
```

Set `PREFIX` to use another location, for example
`PREFIX=/opt/cloudreve-sync sudo -E ./install.sh`.

Arch Linux users can build the package with `makepkg -si`. Nix users can run or
install it with `nix run .` or `nix profile install .`.

On Debian/Ubuntu, eframe normally needs:

```sh
sudo apt install build-essential pkg-config libx11-dev libxi-dev libgl1-mesa-dev libxkbcommon-dev zenity
```

## Setup

1. Enable WebDAV in Cloudreve and copy its WebDAV endpoint URL.
2. Enter the endpoint, Cloudreve username, and WebDAV password in the app.
3. Add a local folder and enter its remote path, for example `Documents/work`.
4. Save, then select **Sync now**.

Use **Add some folders...** to select a top-level directory, choose its immediate
subfolders from a checkbox list, and map all selected folders below one remote
parent. Closing the main window hides it; use the tray menu to show it again or
quit the process.

Each mapping accepts one gitignore-style ignore pattern per line. Examples:

```gitignore
*.tmp
.cache/
**/target/
```

Ignored paths are excluded from both upload and download decisions. Enabling
**Start automatically when I sign in** writes
`~/.config/autostart/cloudreve-sync.desktop` when settings are saved.

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

Mappings fail closed when the local folder is unavailable, the remote folder
returns `404`, the server omits file ETags, or at least ten and at least 25% of
the known files appear to have disappeared from one side. This prevents a
temporarily unavailable mount or Cloudreve storage backend from being treated
as an intentional bulk deletion. Configuration and state writes are atomic,
and a malformed state file is reported instead of silently resetting sync
history.

If Cloudreve rejects a path during upload, the app encodes unsafe folder names
with `.dissalowed-folder` and encodes the filename with `.dissalowed-type`. The
encoding and suffixes are removed when files are downloaded, restoring the exact
original local path.
