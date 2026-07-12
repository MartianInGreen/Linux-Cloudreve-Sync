use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub server_url: String,
    pub username: String,
    pub password: String,
    pub poll_seconds: u64,
    pub mappings: Vec<SyncMapping>,
    #[serde(default)]
    pub autostart: bool,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server_url: String::new(),
            username: String::new(),
            password: String::new(),
            poll_seconds: 10,
            mappings: Vec::new(),
            autostart: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncMapping {
    pub id: Uuid,
    pub local_path: PathBuf,
    pub remote_path: String,
    pub enabled: bool,
    #[serde(default)]
    pub ignore_patterns: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SyncState {
    pub mappings: BTreeMap<Uuid, BTreeMap<String, EntryState>>,
    #[serde(default)]
    pub hash_algorithm: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryState {
    pub local_hash: Option<String>,
    pub remote_tag: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Conflict {
    pub mapping_id: Uuid,
    pub relative_path: String,
    pub local_exists: bool,
    pub remote_exists: bool,
    pub remote_path: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum ConflictChoice {
    KeepLocal,
    KeepRemote,
}

#[derive(Clone, Debug)]
pub enum BackendCommand {
    UpdateConfig(AppConfig),
    SyncNow,
    Resolve(Conflict, ConflictChoice),
    Shutdown,
}

#[derive(Clone, Debug)]
pub enum BackendEvent {
    Status(String),
    Error(String),
    Conflict(Conflict),
    SyncStarted,
    SyncFinished(bool),
    TransferStarted(Transfer),
    TransferFinished(Transfer, bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferDirection {
    Upload,
    Download,
    DeleteLocal,
    DeleteRemote,
}

#[derive(Clone, Debug)]
pub struct Transfer {
    pub direction: TransferDirection,
    pub relative_path: String,
    pub bytes: Option<u64>,
}
