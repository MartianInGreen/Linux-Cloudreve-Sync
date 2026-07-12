use crate::{
    local_index::{build_ignores, LocalIndex},
    model::{
        AppConfig, BackendCommand, BackendEvent, Conflict, ConflictChoice, EntryState, SyncMapping,
        SyncState, Transfer, TransferDirection,
    },
    storage,
    webdav::{RemoteEntry, WebDavClient},
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc;

pub async fn run(
    mut commands: mpsc::UnboundedReceiver<BackendCommand>,
    events: mpsc::UnboundedSender<BackendEvent>,
) {
    let mut config = storage::load_config();
    let mut state = storage::load_state();
    if state.hash_algorithm != "blake3" {
        state.mappings.clear();
        state.hash_algorithm = "blake3".into();
        if let Err(error) = storage::save_state(&state) {
            let _ = events.send(BackendEvent::Error(format!(
                "Could not migrate sync state to BLAKE3: {error}"
            )));
            return;
        }
    }
    let index = match storage::data_path("local-index.db") {
        Ok(path) => match LocalIndex::open(path).await {
            Ok(index) => index,
            Err(error) => {
                let _ = events.send(BackendEvent::Error(format!(
                    "Could not open local file index: {error}"
                )));
                return;
            }
        },
        Err(error) => {
            let _ = events.send(BackendEvent::Error(error.to_string()));
            return;
        }
    };
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_secs(config.poll_seconds.max(2)));
    loop {
        tokio::select! {
            _ = ticker.tick() => sync_all(&config, &mut state, &index, &events).await,
            command = commands.recv() => match command {
                Some(BackendCommand::UpdateConfig(next)) => {
                    let changed: Vec<_> = next
                        .mappings
                        .iter()
                        .filter(|mapping| {
                            config
                                .mappings
                                .iter()
                                .find(|old| old.id == mapping.id)
                                .is_some_and(|old| mapping_paths_changed(old, mapping))
                        })
                        .map(|mapping| mapping.id)
                        .collect();
                    for id in changed {
                        state.mappings.remove(&id);
                    }
                    state
                        .mappings
                        .retain(|id, _| next.mappings.iter().any(|mapping| mapping.id == *id));
                    if let Err(error) = storage::save_state(&state) {
                        let _ = events.send(BackendEvent::Error(format!(
                            "Could not reset changed folder state: {error}"
                        )));
                    }
                    config = next;
                    ticker = tokio::time::interval(std::time::Duration::from_secs(config.poll_seconds.max(2)));
                }
                Some(BackendCommand::SyncNow) => sync_all(&config, &mut state, &index, &events).await,
                Some(BackendCommand::Resolve(conflict, choice)) => {
                    if let Err(error) = resolve(&config, &mut state, &conflict, choice, &events).await {
                        let _ = events.send(BackendEvent::Error(error.to_string()));
                    }
                }
                Some(BackendCommand::Shutdown) | None => break,
            }
        }
    }
}

fn client(config: &AppConfig) -> Result<WebDavClient> {
    WebDavClient::new(&config.server_url, &config.username, &config.password)
}

async fn sync_all(
    config: &AppConfig,
    state: &mut SyncState,
    index: &LocalIndex,
    events: &mpsc::UnboundedSender<BackendEvent>,
) {
    if config.server_url.is_empty() {
        return;
    }
    let _ = events.send(BackendEvent::SyncStarted);
    let _ = events.send(BackendEvent::Status("Connecting to Cloudreve...".into()));
    let dav = match client(config) {
        Ok(client) => client,
        Err(error) => {
            let _ = events.send(BackendEvent::Error(error.to_string()));
            let _ = events.send(BackendEvent::SyncFinished(false));
            return;
        }
    };
    if let Err(error) = dav.test().await {
        let _ = events.send(BackendEvent::Error(format!("Connection failed: {error}")));
        let _ = events.send(BackendEvent::SyncFinished(false));
        return;
    }
    let mut succeeded = true;
    for mapping in config.mappings.iter().filter(|m| m.enabled) {
        let _ = events.send(BackendEvent::Status(format!(
            "Checking {}...",
            mapping.local_path.display()
        )));
        if let Err(error) = sync_mapping(&dav, mapping, state, index, events).await {
            succeeded = false;
            let _ = events.send(BackendEvent::Error(format!(
                "{}: {error}",
                mapping.local_path.display()
            )));
        }
    }
    if let Err(error) = storage::save_state(state) {
        succeeded = false;
        let _ = events.send(BackendEvent::Error(format!(
            "Could not save sync state: {error}"
        )));
    }
    let status = if succeeded {
        "Up to date"
    } else {
        "Sync completed with errors"
    };
    let _ = events.send(BackendEvent::Status(status.into()));
    let _ = events.send(BackendEvent::SyncFinished(succeeded));
}

async fn sync_mapping(
    dav: &WebDavClient,
    mapping: &SyncMapping,
    state: &mut SyncState,
    index: &LocalIndex,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    std::fs::create_dir_all(&mapping.local_path)?;
    let _ = events.send(BackendEvent::Status(format!(
        "Scanning local files in {}...",
        mapping.local_path.display()
    )));
    let ignores = build_ignores(&mapping.local_path, &mapping.ignore_patterns)?;
    let local = index
        .scan(mapping.id, &mapping.local_path, &ignores)
        .await?;
    let _ = events.send(BackendEvent::Status(format!(
        "Reading Cloudreve folder /{}...",
        mapping.remote_path.trim_matches('/')
    )));
    let mut remote = canonical_remote_entries(dav.list_recursive(&mapping.remote_path).await?);
    remote.retain(|path, entry| {
        !ignores
            .matched_path_or_any_parents(path, entry.is_dir)
            .is_ignore()
    });
    let previous = state.mappings.entry(mapping.id).or_default();
    let mut conflicted = BTreeSet::new();
    let paths: BTreeSet<_> = local.keys().chain(remote.keys()).cloned().collect();
    for path in paths {
        let local_hash = local.get(&path).cloned();
        let remote_entry = remote.get(&path);
        if remote_entry.is_some_and(|e| e.is_dir) {
            continue;
        }
        let remote_tag = remote_entry.map(|e| e.tag.clone());
        let old = previous.get(&path);
        let local_changed = old.map_or(local_hash.is_some(), |s| s.local_hash != local_hash);
        let remote_changed = old.map_or(remote_tag.is_some(), |s| s.remote_tag != remote_tag);

        match (
            local_hash.as_ref(),
            remote_entry,
            old,
            local_changed,
            remote_changed,
        ) {
            (Some(_), None, None, _, _) => upload(dav, mapping, &path, events).await?,
            (None, Some(entry), None, _, _) => {
                let local_hash = download(dav, mapping, &path, entry, events).await?;
                previous.insert(
                    path,
                    EntryState {
                        local_hash: Some(local_hash),
                        remote_tag,
                    },
                );
            }
            (None, Some(entry), Some(_), true, false) => {
                delete_remote(dav, mapping, &entry.relative_path, events).await?
            }
            (Some(_), None, Some(_), false, true) => delete_local(mapping, &path, events).await?,
            (Some(_), Some(_), _, true, false) => upload(dav, mapping, &path, events).await?,
            (Some(_), Some(entry), _, false, true) => {
                let local_hash = download(dav, mapping, &path, entry, events).await?;
                previous.insert(
                    path,
                    EntryState {
                        local_hash: Some(local_hash),
                        remote_tag,
                    },
                );
            }
            (local, remote, _, true, true) => {
                if let (Some(local_hash), Some(remote_entry)) = (local, remote) {
                    let _ = events.send(BackendEvent::Status(format!("Comparing {}...", path)));
                    let remote_data = dav
                        .download(&remote_file(mapping, &remote_entry.relative_path))
                        .await?;
                    if hash_bytes(&remote_data) == *local_hash {
                        previous.insert(
                            path,
                            EntryState {
                                local_hash: Some(local_hash.clone()),
                                remote_tag,
                            },
                        );
                        continue;
                    }
                }
                let _ = events.send(BackendEvent::Conflict(Conflict {
                    mapping_id: mapping.id,
                    relative_path: path.clone(),
                    local_exists: local.is_some(),
                    remote_exists: remote.is_some(),
                    remote_path: remote.map(|entry| entry.relative_path.clone()),
                }));
                conflicted.insert(path.clone());
                continue;
            }
            _ => {}
        }
    }
    let refreshed_local = index
        .scan(mapping.id, &mapping.local_path, &ignores)
        .await?;
    let mut refreshed_remote =
        canonical_remote_entries(dav.list_recursive(&mapping.remote_path).await?);
    refreshed_remote.retain(|path, entry| {
        !ignores
            .matched_path_or_any_parents(path, entry.is_dir)
            .is_ignore()
    });
    let refreshed_paths: BTreeSet<_> = refreshed_local
        .keys()
        .chain(refreshed_remote.keys())
        .cloned()
        .collect();
    previous.retain(|path, _| conflicted.contains(path));
    for path in refreshed_paths {
        if conflicted.contains(&path)
            || refreshed_remote
                .get(&path)
                .is_some_and(|entry| entry.is_dir)
        {
            continue;
        }
        previous.insert(
            path.clone(),
            EntryState {
                local_hash: refreshed_local.get(&path).cloned(),
                remote_tag: refreshed_remote.get(&path).map(|entry| entry.tag.clone()),
            },
        );
    }
    Ok(())
}

async fn upload(
    dav: &WebDavClient,
    mapping: &SyncMapping,
    relative: &str,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let data = tokio::fs::read(mapping.local_path.join(relative)).await?;
    let transfer = Transfer {
        direction: TransferDirection::Upload,
        relative_path: relative.into(),
        bytes: Some(data.len() as u64),
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let has_invalid_folder = relative.rsplit_once('/').is_some_and(|(parent, _)| {
        parent
            .split('/')
            .any(|segment| !valid_remote_segment(segment))
    });
    let initial_result = if has_invalid_folder {
        let fallback = safe_remote_path(relative);
        let _ = events.send(BackendEvent::Status(format!(
            "Uploading {relative} using storage-safe folder names"
        )));
        dav.upload(&remote_file(mapping, &fallback), data.clone())
            .await
    } else {
        dav.upload(&remote_file(mapping, relative), data.clone())
            .await
    };
    let result = match initial_result {
        Ok(()) => Ok(()),
        Err(error) if has_invalid_folder => Err(anyhow!(
            "safe-path upload of {relative} as {} failed: {error}",
            safe_remote_path(relative)
        )),
        Err(original_error) => {
            let fallback = safe_remote_path(relative);
            let _ = events.send(BackendEvent::Status(format!(
                "Cloudreve rejected {relative}; retrying as {fallback}"
            )));
            match dav.upload(&remote_file(mapping, &fallback), data).await {
                Ok(()) => Ok(()),
                Err(fallback_error) => Err(anyhow!(
                    "upload of {relative} failed: {original_error}; safe-name retry as {fallback} failed: {fallback_error}"
                )),
            }
        }
    };
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
}

async fn download(
    dav: &WebDavClient,
    mapping: &SyncMapping,
    relative: &str,
    entry: &RemoteEntry,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<String> {
    let mut transfer = Transfer {
        direction: TransferDirection::Download,
        relative_path: relative.into(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let target = mapping.local_path.join(relative);
    let result = async {
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let data = dav
            .download(&remote_file(mapping, &entry.relative_path))
            .await?;
        let temp = target.with_extension("cloudreve-download");
        let bytes = data.len() as u64;
        let hash = hash_bytes(&data);
        tokio::fs::write(&temp, data).await?;
        tokio::fs::rename(temp, target).await?;
        Ok::<(u64, String), anyhow::Error>((bytes, hash))
    }
    .await;
    if let Ok((bytes, _)) = &result {
        transfer.bytes = Some(*bytes);
    }
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result.map(|(_, hash)| hash)
}

async fn delete_remote(
    dav: &WebDavClient,
    mapping: &SyncMapping,
    relative: &str,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let transfer = Transfer {
        direction: TransferDirection::DeleteRemote,
        relative_path: relative.into(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let result = dav.delete(&remote_file(mapping, relative)).await;
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
}

async fn delete_local(
    mapping: &SyncMapping,
    relative: &str,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let transfer = Transfer {
        direction: TransferDirection::DeleteLocal,
        relative_path: relative.into(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let result = tokio::fs::remove_file(mapping.local_path.join(relative)).await;
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result.map_err(Into::into)
}

async fn resolve(
    config: &AppConfig,
    state: &mut SyncState,
    conflict: &Conflict,
    choice: ConflictChoice,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let mapping = config
        .mappings
        .iter()
        .find(|m| m.id == conflict.mapping_id)
        .context("sync mapping no longer exists")?;
    let dav = client(config)?;
    match choice {
        ConflictChoice::KeepLocal if conflict.local_exists => {
            upload(&dav, mapping, &conflict.relative_path, events).await?
        }
        ConflictChoice::KeepLocal => {
            delete_remote(
                &dav,
                mapping,
                conflict
                    .remote_path
                    .as_deref()
                    .unwrap_or(&conflict.relative_path),
                events,
            )
            .await?
        }
        ConflictChoice::KeepRemote if conflict.remote_exists => {
            let entry = RemoteEntry {
                tag: String::new(),
                is_dir: false,
                relative_path: conflict
                    .remote_path
                    .clone()
                    .unwrap_or_else(|| conflict.relative_path.clone()),
            };
            download(&dav, mapping, &conflict.relative_path, &entry, events).await?;
        }
        ConflictChoice::KeepRemote => {
            delete_local(mapping, &conflict.relative_path, events).await?;
        }
    }
    state
        .mappings
        .entry(mapping.id)
        .or_default()
        .remove(&conflict.relative_path);
    storage::save_state(state)
}

fn remote_file(mapping: &SyncMapping, relative: &str) -> String {
    format!(
        "{}/{}",
        mapping.remote_path.trim_matches('/'),
        relative.trim_start_matches('/')
    )
}

fn canonical_remote_entries(
    entries: BTreeMap<String, RemoteEntry>,
) -> BTreeMap<String, RemoteEntry> {
    entries
        .into_iter()
        .map(|(path, entry)| {
            let canonical = canonical_remote_path(&path);
            (canonical, entry)
        })
        .collect()
}

fn safe_remote_path(path: &str) -> String {
    let mut segments: Vec<_> = path.split('/').collect();
    let name = segments.pop().unwrap_or_default();
    let mut safe_segments: Vec<String> = segments
        .into_iter()
        .map(|segment| {
            if valid_remote_segment(segment) {
                segment.to_string()
            } else {
                encode_remote_segment(segment, ".dissalowed-folder")
            }
        })
        .collect();
    safe_segments.push(encode_remote_segment(name, ".dissalowed-type"));
    safe_segments.join("/")
}

fn canonical_remote_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            decode_remote_segment(segment, ".dissalowed-folder")
                .or_else(|| decode_remote_segment(segment, ".dissalowed-type"))
                .unwrap_or_else(|| {
                    segment
                        .strip_suffix(".dissalowed-type")
                        .unwrap_or(segment)
                        .to_string()
                })
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn valid_remote_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && !segment.ends_with([' ', '.'])
        && !segment
            .chars()
            .any(|character| character.is_control() || r#"<>:"\|?*"#.contains(character))
}

fn encode_remote_segment(segment: &str, suffix: &str) -> String {
    format!(
        "cloudreve-sync-{}{suffix}",
        URL_SAFE_NO_PAD.encode(segment.as_bytes())
    )
}

fn decode_remote_segment(segment: &str, suffix: &str) -> Option<String> {
    segment
        .strip_suffix(suffix)?
        .strip_prefix("cloudreve-sync-")
        .and_then(|encoded| URL_SAFE_NO_PAD.decode(encoded).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

fn hash_bytes(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

fn mapping_paths_changed(old: &SyncMapping, new: &SyncMapping) -> bool {
    old.local_path != new.local_path
        || old.remote_path.trim_matches('/') != new.remote_path.trim_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn joins_remote_paths() {
        let mapping = SyncMapping {
            id: Uuid::new_v4(),
            local_path: "/tmp".into(),
            remote_path: "/documents/".into(),
            enabled: true,
            ignore_patterns: String::new(),
        };
        assert_eq!(
            remote_file(&mapping, "work/file.txt"),
            "documents/work/file.txt"
        );
    }

    #[test]
    fn detects_mapping_path_changes_but_ignores_slashes() {
        let id = Uuid::new_v4();
        let old = SyncMapping {
            id,
            local_path: "/tmp/local".into(),
            remote_path: "documents".into(),
            enabled: true,
            ignore_patterns: String::new(),
        };
        let mut changed = old.clone();
        changed.remote_path = "/documents/".into();
        assert!(!mapping_paths_changed(&old, &changed));
        changed.remote_path = "other".into();
        assert!(mapping_paths_changed(&old, &changed));
    }

    #[test]
    fn hashes_remote_content() {
        assert_eq!(
            hash_bytes(b"cloudreve"),
            "2c7be30df802842e73a242c04d3c1e1e3df1297b6f7a85ee08c7611295e634dc"
        );
    }

    #[test]
    fn disallowed_remote_suffix_maps_to_original_local_name() {
        let entry = RemoteEntry {
            tag: "etag".into(),
            is_dir: false,
            relative_path: "course/page.html.dissalowed-type".into(),
        };
        let remote =
            canonical_remote_entries(BTreeMap::from([(entry.relative_path.clone(), entry)]));

        assert!(remote.contains_key("course/page.html"));
        assert_eq!(
            remote["course/page.html"].relative_path,
            "course/page.html.dissalowed-type"
        );
    }

    #[test]
    fn rejected_filename_round_trips_through_safe_remote_name() {
        let original = "course/NEU: Link zu Selbstlerneinheiten für MKL.html";
        let safe = safe_remote_path(original);

        assert!(!safe.contains(':'));
        assert!(!safe.contains(' '));
        assert!(safe.ends_with(".dissalowed-type"));
        assert_eq!(canonical_remote_path(&safe), original);
    }

    #[test]
    fn invalid_folder_round_trips_through_safe_remote_name() {
        let original = "Übungsunterlagen/Übung: Fluidtechnik - Kopie/Fluidtechnik.pdf";
        let safe = safe_remote_path(original);

        assert!(safe.starts_with("Übungsunterlagen/cloudreve-sync-"));
        assert!(safe.contains(".dissalowed-folder/"));
        assert!(!safe.contains("Übung:"));
        assert_eq!(canonical_remote_path(&safe), original);
    }

    #[test]
    fn detects_reserved_remote_path_characters() {
        assert!(valid_remote_segment("Übungsunterlagen"));
        assert!(!valid_remote_segment("Übung: Fluidtechnik"));
        assert!(!valid_remote_segment("trailing."));
        assert!(!valid_remote_segment("question?"));
    }
}
