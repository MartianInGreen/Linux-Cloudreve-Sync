use crate::{
    local_index::{build_ignores, LocalIndex},
    model::{
        AppConfig, BackendCommand, BackendEvent, Conflict, ConflictChoice, EntryState, SyncMapping,
        SyncState, Transfer, TransferDirection,
    },
    storage,
    webdav::{validate_remote_path, CollectionListing, RemoteEntry, WebDavClient},
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use reqwest::StatusCode;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::Write,
    path::{Component, Path, PathBuf},
};
use tokio::sync::mpsc;

pub async fn run(
    mut commands: mpsc::UnboundedReceiver<BackendCommand>,
    events: mpsc::UnboundedSender<BackendEvent>,
) {
    let mut config = match storage::load_config() {
        Ok(config) => config,
        Err(error) => {
            let _ = events.send(BackendEvent::Error(error.to_string()));
            return;
        }
    };
    let mut state = match storage::load_state() {
        Ok(state) => state,
        Err(error) => {
            let _ = events.send(BackendEvent::Error(error.to_string()));
            return;
        }
    };
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
    let mut ticker = make_ticker(config.poll_seconds);
    let (power_tx, mut power_rx) = mpsc::unbounded_channel();
    let power_events = events.clone();
    let power_monitor = tokio::spawn(async move {
        if let Err(error) = crate::power::monitor(power_tx).await {
            let _ = power_events.send(BackendEvent::Error(format!(
                "Suspend monitoring unavailable: {error}"
            )));
        }
    });
    let mut suspended = false;
    let mut power_active = true;
    let mut wait_for_network = false;
    let mut pending_commands = VecDeque::new();
    loop {
        let action = if suspended {
            tokio::select! {
                biased;
                command = commands.recv() => RunAction::Command(command),
                power = power_rx.recv(), if power_active => RunAction::Power(power),
            }
        } else if let Some(command) = pending_commands.pop_front() {
            RunAction::Command(Some(command))
        } else {
            tokio::select! {
                biased;
                command = commands.recv() => RunAction::Command(command),
                power = power_rx.recv(), if power_active => RunAction::Power(power),
                _ = ticker.tick() => RunAction::Sync,
            }
        };

        match action {
            RunAction::Command(Some(BackendCommand::UpdateConfig(next))) => {
                apply_config(&mut config, next, &mut state, &events);
                ticker = make_ticker(config.poll_seconds);
            }
            RunAction::Command(Some(BackendCommand::SyncNow)) | RunAction::Sync if !suspended => {
                if wait_for_network {
                    wait_for_network = false;
                    if let Some(interruption) = interruptible_network_wait(
                        &config,
                        &events,
                        &mut commands,
                        &mut power_rx,
                        &mut power_active,
                    )
                    .await
                    {
                        handle_interruption(
                            interruption,
                            &mut suspended,
                            &mut wait_for_network,
                            &mut pending_commands,
                            &state,
                            &events,
                        );
                        continue;
                    }
                }
                ticker.reset();
                let interruption = {
                    let operation = sync_all(&config, &mut state, &index, &events);
                    tokio::pin!(operation);
                    loop {
                        tokio::select! {
                            biased;
                            command = commands.recv() => match command {
                                Some(BackendCommand::SyncNow) => continue,
                                command => break Some(Interruption::Command(command)),
                            },
                            power = power_rx.recv(), if power_active => match power {
                                Some(power) => break Some(Interruption::Power(Some(power))),
                                None => {
                                    power_active = false;
                                    continue;
                                }
                            },
                            _ = &mut operation => break None,
                        }
                    }
                };
                if let Some(interruption) = interruption {
                    handle_interruption(
                        interruption,
                        &mut suspended,
                        &mut wait_for_network,
                        &mut pending_commands,
                        &state,
                        &events,
                    );
                }
            }
            RunAction::Command(Some(BackendCommand::Resolve(conflict, choice))) if !suspended => {
                let outcome = {
                    let operation = resolve(&config, &mut state, &conflict, choice, &events);
                    tokio::pin!(operation);
                    loop {
                        tokio::select! {
                            biased;
                            command = commands.recv() => break ResolveOutcome::Interrupted(Interruption::Command(command)),
                            power = power_rx.recv(), if power_active => match power {
                                Some(power) => break ResolveOutcome::Interrupted(Interruption::Power(Some(power))),
                                None => {
                                    power_active = false;
                                    continue;
                                }
                            },
                            result = &mut operation => break ResolveOutcome::Finished(result),
                        }
                    }
                };
                match outcome {
                    ResolveOutcome::Finished(Ok(())) => {}
                    ResolveOutcome::Finished(Err(error)) => {
                        let _ = events.send(BackendEvent::Error(error.to_string()));
                    }
                    ResolveOutcome::Interrupted(interruption) => handle_interruption(
                        interruption,
                        &mut suspended,
                        &mut wait_for_network,
                        &mut pending_commands,
                        &state,
                        &events,
                    ),
                }
            }
            RunAction::Command(Some(BackendCommand::Shutdown)) | RunAction::Command(None) => break,
            RunAction::Power(Some(crate::power::PowerEvent::Suspending(ack))) => {
                suspended = true;
                let _ = storage::save_state(&state);
                let _ = ack.send(());
            }
            RunAction::Power(Some(crate::power::PowerEvent::Resumed)) => {
                suspended = false;
                wait_for_network = true;
                enqueue_command(&mut pending_commands, BackendCommand::SyncNow);
                ticker.reset();
            }
            RunAction::Power(None) => {
                power_active = false;
                if suspended {
                    suspended = false;
                    wait_for_network = true;
                    enqueue_command(&mut pending_commands, BackendCommand::SyncNow);
                    let _ = events.send(BackendEvent::Error(
                        "Suspend monitoring stopped; resuming synchronization conservatively"
                            .into(),
                    ));
                }
            }
            RunAction::Command(Some(command)) if suspended => {
                enqueue_command(&mut pending_commands, command);
            }
            RunAction::Sync | RunAction::Command(Some(_)) => {}
        }
    }
    power_monitor.abort();
    if let Err(error) = storage::save_state(&state) {
        let _ = events.send(BackendEvent::Error(format!(
            "Could not save sync state: {error}"
        )));
    }
}

enum RunAction {
    Command(Option<BackendCommand>),
    Power(Option<crate::power::PowerEvent>),
    Sync,
}

enum Interruption {
    Command(Option<BackendCommand>),
    Power(Option<crate::power::PowerEvent>),
}

enum ResolveOutcome {
    Finished(Result<()>),
    Interrupted(Interruption),
}

fn make_ticker(seconds: u64) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(seconds.max(2)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

fn apply_config(
    config: &mut AppConfig,
    next: AppConfig,
    state: &mut SyncState,
    events: &mpsc::UnboundedSender<BackendEvent>,
) {
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
    if let Err(error) = storage::save_state(state) {
        let _ = events.send(BackendEvent::Error(format!(
            "Could not reset changed folder state: {error}"
        )));
    }
    *config = next;
}

fn handle_interruption(
    interruption: Interruption,
    suspended: &mut bool,
    wait_for_network: &mut bool,
    pending_commands: &mut VecDeque<BackendCommand>,
    state: &SyncState,
    events: &mpsc::UnboundedSender<BackendEvent>,
) {
    let _ = events.send(BackendEvent::SyncFinished(false));
    match interruption {
        Interruption::Command(Some(command)) => enqueue_command(pending_commands, command),
        Interruption::Command(None) => {}
        Interruption::Power(Some(crate::power::PowerEvent::Suspending(ack))) => {
            *suspended = true;
            if let Err(error) = storage::save_state(state) {
                let _ = events.send(BackendEvent::Error(format!(
                    "Could not save state before suspend: {error}"
                )));
            }
            let _ = ack.send(());
        }
        Interruption::Power(Some(crate::power::PowerEvent::Resumed)) => {
            *suspended = false;
            *wait_for_network = true;
            enqueue_command(pending_commands, BackendCommand::SyncNow);
        }
        Interruption::Power(None) => {}
    }
}

async fn interruptible_network_wait(
    config: &AppConfig,
    events: &mpsc::UnboundedSender<BackendEvent>,
    commands: &mut mpsc::UnboundedReceiver<BackendCommand>,
    power: &mut mpsc::UnboundedReceiver<crate::power::PowerEvent>,
    power_active: &mut bool,
) -> Option<Interruption> {
    let wait = async {
        let delays = [1, 2, 4, 8, 15];
        for delay in delays {
            match client(config) {
                Ok(client) if client.is_reachable().await.unwrap_or(false) => return,
                _ => {
                    let _ = events.send(BackendEvent::Status("Waiting for network...".into()));
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                }
            }
        }
    };
    tokio::pin!(wait);
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => break Some(Interruption::Command(command)),
            event = power.recv(), if *power_active => match event {
                Some(event) => break Some(Interruption::Power(Some(event))),
                None => {
                    *power_active = false;
                    continue;
                }
            },
            _ = &mut wait => break None,
        }
    }
}

fn enqueue_command(queue: &mut VecDeque<BackendCommand>, command: BackendCommand) {
    if matches!(command, BackendCommand::SyncNow)
        && queue
            .iter()
            .any(|item| matches!(item, BackendCommand::SyncNow))
    {
        return;
    }
    queue.push_back(command);
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
    if let Err(error) = validate_mappings(config) {
        let _ = events.send(BackendEvent::Error(error.to_string()));
        let _ = events.send(BackendEvent::SyncFinished(false));
        return;
    }
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
    let local_root = validate_local_root(&mapping.local_path)?;
    let remote_root = mapping.remote_path.trim_matches('/');
    if !remote_root.is_empty() {
        validate_remote_path(remote_root).context("invalid mapped remote folder")?;
    }
    let _ = events.send(BackendEvent::Status(format!(
        "Scanning local files in {}...",
        mapping.local_path.display()
    )));
    let ignores = build_ignores(&mapping.local_path, &mapping.ignore_patterns)?;
    let local = index.scan(mapping.id, &local_root, &ignores).await?;
    let _ = events.send(BackendEvent::Status(format!(
        "Reading Cloudreve folder /{}...",
        mapping.remote_path.trim_matches('/')
    )));
    let listed = dav.list_recursive(remote_root).await?;
    let mut remote = match listed {
        CollectionListing::Found(entries) => canonical_remote_entries(entries)?,
        CollectionListing::Missing => {
            anyhow::bail!(
                "mapped remote folder /{remote_root} is unavailable; refusing to treat it as empty"
            )
        }
    };
    remote.retain(|path, entry| {
        !ignores
            .matched_path_or_any_parents(path, entry.is_dir)
            .is_ignore()
    });
    let previous = state.mappings.entry(mapping.id).or_default();
    guard_bulk_deletions(previous, &local, &remote)?;
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
            (Some(_), None, None, _, _) => {
                upload(dav, mapping, &local_root, &path, None, events).await?
            }
            (None, Some(entry), None, _, _) => {
                let local_hash =
                    download(dav, mapping, &local_root, &path, entry, None, events).await?;
                previous.insert(
                    path,
                    EntryState {
                        local_hash: Some(local_hash),
                        remote_tag,
                    },
                );
            }
            (None, Some(entry), Some(_), true, false) => {
                delete_remote(dav, mapping, entry, events).await?;
                previous.remove(&path);
            }
            (Some(local_hash), None, Some(_), false, true) => {
                delete_local(&local_root, &path, local_hash, events).await?;
                previous.remove(&path);
            }
            (Some(_), Some(entry), _, true, false) => {
                upload(dav, mapping, &local_root, &path, Some(entry), events).await?
            }
            (Some(local), Some(entry), _, false, true) => {
                let local_hash =
                    download(dav, mapping, &local_root, &path, entry, Some(local), events).await?;
                previous.insert(
                    path,
                    EntryState {
                        local_hash: Some(local_hash),
                        remote_tag,
                    },
                );
            }
            (None, None, Some(_), true, true) => {
                previous.remove(&path);
            }
            (local, remote, _, true, true) => {
                if let (Some(local_hash), Some(remote_entry)) = (local, remote) {
                    let _ = events.send(BackendEvent::Status(format!("Comparing {}...", path)));
                    let remote_data = dav
                        .download(
                            &remote_file(mapping, &remote_entry.relative_path),
                            Some(&remote_entry.tag),
                        )
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
                    local_hash: local.cloned(),
                    remote_tag: remote.map(|entry| entry.tag.clone()),
                }));
                conflicted.insert(path.clone());
                continue;
            }
            _ => {}
        }
    }
    let refreshed_local = index.scan(mapping.id, &local_root, &ignores).await?;
    let mut refreshed_remote = match dav.list_recursive(remote_root).await? {
        CollectionListing::Found(entries) => canonical_remote_entries(entries)?,
        CollectionListing::Missing => {
            anyhow::bail!("mapped remote folder /{remote_root} disappeared while synchronizing")
        }
    };
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
    local_root: &Path,
    relative: &str,
    existing: Option<&RemoteEntry>,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let local_path = safe_local_path(local_root, relative)?;
    let data = tokio::fs::read(&local_path).await?;
    let current_hash = hash_bytes(&data);
    let latest_data = tokio::fs::read(&local_path).await?;
    if hash_bytes(&latest_data) != current_hash {
        anyhow::bail!("{relative} changed while it was being prepared for upload");
    }
    let data = latest_data;
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
    let expected_tag = existing.map(|entry| entry.tag.as_str());
    let initial_remote = existing
        .map(|entry| entry.relative_path.as_str())
        .unwrap_or(relative);
    let initial_result = if has_invalid_folder && existing.is_none() {
        let fallback = safe_remote_path(relative);
        let _ = events.send(BackendEvent::Status(format!(
            "Uploading {relative} using storage-safe folder names"
        )));
        dav.upload(&remote_file(mapping, &fallback), data.clone(), expected_tag)
            .await
    } else {
        dav.upload(
            &remote_file(mapping, initial_remote),
            data.clone(),
            expected_tag,
        )
        .await
    };
    let result = match initial_result {
        Ok(status) if status.is_success() => Ok(()),
        Ok(StatusCode::PRECONDITION_FAILED) => Err(anyhow!(
            "remote file changed during upload; retry on the next sync"
        )),
        Ok(status) if existing.is_some() => Err(anyhow!("upload returned {status}")),
        Ok(status) if has_invalid_folder => {
            Err(anyhow!("safe-path upload of {relative} returned {status}"))
        }
        Ok(status)
            if !matches!(
                status,
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
            ) =>
        {
            Err(anyhow!("upload returned {status}"))
        }
        Err(error) if has_invalid_folder => Err(anyhow!(
            "safe-path upload of {relative} as {} failed: {error}",
            safe_remote_path(relative)
        )),
        Err(original_error) => {
            return finish_transfer(
                events,
                transfer,
                Err(anyhow!("upload of {relative} failed: {original_error}")),
            );
        }
        Ok(original_status) => {
            let fallback = safe_remote_path(relative);
            let _ = events.send(BackendEvent::Status(format!(
                "Cloudreve rejected {relative}; retrying as {fallback}"
            )));
            match dav.upload(&remote_file(mapping, &fallback), data, None).await {
                Ok(status) if status.is_success() => Ok(()),
                Ok(status) => Err(anyhow!(
                    "upload of {relative} returned {original_status}; safe-name retry as {fallback} returned {status}"
                )),
                Err(fallback_error) => Err(anyhow!(
                    "upload of {relative} returned {original_status}; safe-name retry as {fallback} failed: {fallback_error}"
                )),
            }
        }
    };
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
}

fn finish_transfer<T>(
    events: &mpsc::UnboundedSender<BackendEvent>,
    transfer: Transfer,
    result: Result<T>,
) -> Result<T> {
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
}

async fn download(
    dav: &WebDavClient,
    mapping: &SyncMapping,
    local_root: &Path,
    relative: &str,
    entry: &RemoteEntry,
    expected_local_hash: Option<&str>,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<String> {
    let mut transfer = Transfer {
        direction: TransferDirection::Download,
        relative_path: relative.into(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let target = safe_local_path(local_root, relative)?;
    let result = async {
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let data = dav
            .download(
                &remote_file(mapping, &entry.relative_path),
                Some(&entry.tag),
            )
            .await?;
        let bytes = data.len() as u64;
        let hash = hash_bytes(&data);
        match expected_local_hash {
            Some(expected) => {
                let current = tokio::fs::read(&target).await?;
                if hash_bytes(&current) != expected {
                    anyhow::bail!(
                        "{relative} changed while the remote copy was downloading; refusing to replace it"
                    );
                }
            }
            None if tokio::fs::try_exists(&target).await? => {
                anyhow::bail!(
                    "{relative} appeared while the remote copy was downloading; refusing to replace it"
                );
            }
            None => {}
        }
        atomic_replace(target, data).await?;
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
    entry: &RemoteEntry,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let transfer = Transfer {
        direction: TransferDirection::DeleteRemote,
        relative_path: entry.relative_path.clone(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let result = dav
        .delete(&remote_file(mapping, &entry.relative_path), &entry.tag)
        .await;
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
}

async fn delete_local(
    local_root: &Path,
    relative: &str,
    expected_hash: &str,
    events: &mpsc::UnboundedSender<BackendEvent>,
) -> Result<()> {
    let transfer = Transfer {
        direction: TransferDirection::DeleteLocal,
        relative_path: relative.into(),
        bytes: None,
    };
    let _ = events.send(BackendEvent::TransferStarted(transfer.clone()));
    let target = safe_local_path(local_root, relative)?;
    let result = async {
        let data = tokio::fs::read(&target).await?;
        if hash_bytes(&data) != expected_hash {
            anyhow::bail!(
                "{relative} changed while synchronization was running; refusing to delete it"
            );
        }
        tokio::fs::remove_file(target).await?;
        Ok(())
    }
    .await;
    let _ = events.send(BackendEvent::TransferFinished(transfer, result.is_ok()));
    result
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
    let local_root = validate_local_root(&mapping.local_path)?;
    let dav = client(config)?;
    match choice {
        ConflictChoice::KeepLocal if conflict.local_exists => {
            let existing = conflict.remote_exists.then(|| RemoteEntry {
                tag: conflict.remote_tag.clone().unwrap_or_default(),
                is_dir: false,
                relative_path: conflict
                    .remote_path
                    .clone()
                    .unwrap_or_else(|| conflict.relative_path.clone()),
            });
            upload(
                &dav,
                mapping,
                &local_root,
                &conflict.relative_path,
                existing.as_ref(),
                events,
            )
            .await?
        }
        ConflictChoice::KeepLocal => {
            let entry = RemoteEntry {
                tag: conflict.remote_tag.clone().unwrap_or_default(),
                is_dir: false,
                relative_path: conflict
                    .remote_path
                    .clone()
                    .unwrap_or_else(|| conflict.relative_path.clone()),
            };
            delete_remote(&dav, mapping, &entry, events).await?
        }
        ConflictChoice::KeepRemote if conflict.remote_exists => {
            let entry = RemoteEntry {
                tag: conflict.remote_tag.clone().unwrap_or_default(),
                is_dir: false,
                relative_path: conflict
                    .remote_path
                    .clone()
                    .unwrap_or_else(|| conflict.relative_path.clone()),
            };
            download(
                &dav,
                mapping,
                &local_root,
                &conflict.relative_path,
                &entry,
                conflict.local_hash.as_deref(),
                events,
            )
            .await?;
        }
        ConflictChoice::KeepRemote => {
            delete_local(
                &local_root,
                &conflict.relative_path,
                conflict
                    .local_hash
                    .as_deref()
                    .context("conflict is missing its local file hash")?,
                events,
            )
            .await?;
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
    let root = mapping.remote_path.trim_matches('/');
    let relative = relative.trim_start_matches('/');
    if root.is_empty() {
        relative.to_string()
    } else {
        format!("{root}/{relative}")
    }
}

fn canonical_remote_entries(
    entries: BTreeMap<String, RemoteEntry>,
) -> Result<BTreeMap<String, RemoteEntry>> {
    let mut canonical_entries = BTreeMap::new();
    for (path, entry) in entries {
        if !entry.is_dir && entry.tag.is_empty() {
            anyhow::bail!(
                "WebDAV server omitted the ETag for /{path}; safe two-way sync is unavailable"
            );
        }
        let canonical = canonical_remote_path(&path);
        validate_relative_path(&canonical)?;
        if canonical_entries.insert(canonical.clone(), entry).is_some() {
            anyhow::bail!("multiple remote files map to the same local path: {canonical}");
        }
    }
    Ok(canonical_entries)
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

fn validate_local_root(path: &Path) -> Result<PathBuf> {
    let root = path
        .canonicalize()
        .with_context(|| format!("mapped local folder {} is unavailable", path.display()))?;
    if !root.is_dir() || root.parent().is_none() {
        anyhow::bail!(
            "mapped local path {} must be an existing non-root directory",
            path.display()
        );
    }
    Ok(root)
}

fn validate_relative_path(relative: &str) -> Result<()> {
    if relative.is_empty()
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains('\\'))
        || Path::new(relative)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        anyhow::bail!("unsafe relative path: {relative}");
    }
    Ok(())
}

fn safe_local_path(root: &Path, relative: &str) -> Result<PathBuf> {
    validate_relative_path(relative)?;
    let mut current = root.to_path_buf();
    for component in Path::new(relative).components() {
        let Component::Normal(part) = component else {
            unreachable!();
        };
        current.push(part);
        if let Ok(metadata) = std::fs::symlink_metadata(&current) {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("refusing to follow symlink {}", current.display());
            }
        }
    }
    Ok(current)
}

async fn atomic_replace(target: PathBuf, data: Vec<u8>) -> Result<()> {
    let parent = target.parent().context("download target has no parent")?;
    std::fs::create_dir_all(parent)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".cloudreve-download-")
        .tempfile_in(parent)?;
    temp.write_all(&data)?;
    temp.as_file().sync_all()?;
    temp.persist(&target).map_err(|error| error.error)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn guard_bulk_deletions(
    previous: &BTreeMap<String, EntryState>,
    local: &BTreeMap<String, String>,
    remote: &BTreeMap<String, RemoteEntry>,
) -> Result<()> {
    if previous.len() < 10 {
        return Ok(());
    }
    let local_missing = previous
        .keys()
        .filter(|path| !local.contains_key(*path) && remote.contains_key(*path))
        .count();
    let remote_missing = previous
        .keys()
        .filter(|path| !remote.contains_key(*path) && local.contains_key(*path))
        .count();
    let threshold = (previous.len() / 4).max(10);
    if local_missing >= threshold {
        anyhow::bail!(
            "{local_missing} local files disappeared at once; refusing bulk remote deletion"
        );
    }
    if remote_missing >= threshold {
        anyhow::bail!(
            "{remote_missing} remote files disappeared at once; refusing bulk local deletion"
        );
    }
    Ok(())
}

fn mapping_paths_changed(old: &SyncMapping, new: &SyncMapping) -> bool {
    old.local_path != new.local_path
        || old.remote_path.trim_matches('/') != new.remote_path.trim_matches('/')
}

fn validate_mappings(config: &AppConfig) -> Result<()> {
    let enabled: Vec<_> = config
        .mappings
        .iter()
        .filter(|mapping| mapping.enabled)
        .collect();
    for (index, mapping) in enabled.iter().enumerate() {
        let local = validate_local_root(&mapping.local_path)?;
        let remote = mapping.remote_path.trim_matches('/');
        if !remote.is_empty() {
            validate_remote_path(remote)?;
        }
        for other in enabled.iter().skip(index + 1) {
            let other_local = validate_local_root(&other.local_path)?;
            if local.starts_with(&other_local) || other_local.starts_with(&local) {
                anyhow::bail!(
                    "local mappings overlap: {} and {}",
                    mapping.local_path.display(),
                    other.local_path.display()
                );
            }
            let other_remote = other.remote_path.trim_matches('/');
            if !other_remote.is_empty() {
                validate_remote_path(other_remote)?;
            }
            if remote.is_empty()
                || other_remote.is_empty()
                || remote == other_remote
                || remote.starts_with(&format!("{other_remote}/"))
                || other_remote.starts_with(&format!("{remote}/"))
            {
                anyhow::bail!("remote mappings overlap: /{remote} and /{other_remote}");
            }
        }
    }
    Ok(())
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
        let root_mapping = SyncMapping {
            remote_path: "/".into(),
            ..mapping
        };
        assert_eq!(remote_file(&root_mapping, "work/file.txt"), "work/file.txt");
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
            canonical_remote_entries(BTreeMap::from([(entry.relative_path.clone(), entry)]))
                .unwrap();

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

    #[test]
    fn rejects_paths_outside_the_mapping() {
        assert!(validate_relative_path("../outside").is_err());
        assert!(validate_relative_path("folder/./file").is_err());
        assert!(validate_relative_path("folder/file").is_ok());
    }

    #[test]
    fn rejects_suspicious_bulk_disappearance() {
        let previous: BTreeMap<_, _> = (0..10)
            .map(|index| {
                (
                    format!("file-{index}"),
                    EntryState {
                        local_hash: Some("hash".into()),
                        remote_tag: Some("tag".into()),
                    },
                )
            })
            .collect();
        let local = BTreeMap::new();
        let remote: BTreeMap<String, RemoteEntry> = previous
            .keys()
            .map(|path| {
                (
                    path.clone(),
                    RemoteEntry {
                        tag: "tag".into(),
                        is_dir: false,
                        relative_path: path.clone(),
                    },
                )
            })
            .collect();

        assert!(guard_bulk_deletions(&previous, &local, &remote).is_err());
    }

    #[tokio::test]
    async fn atomic_download_does_not_use_the_old_fixed_temp_name() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("note.txt");
        let old_temp = directory.path().join("note.cloudreve-download");
        std::fs::write(&old_temp, b"keep").unwrap();

        atomic_replace(target.clone(), b"new".to_vec())
            .await
            .unwrap();

        assert_eq!(std::fs::read(target).unwrap(), b"new");
        assert_eq!(std::fs::read(old_temp).unwrap(), b"keep");
    }
}
