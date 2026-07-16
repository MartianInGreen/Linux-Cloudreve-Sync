use crate::model::{AppConfig, SyncState};
use anyhow::{Context, Result};
use fs2::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write},
    path::PathBuf,
};

pub struct InstanceLock {
    _file: File,
}

fn app_dir() -> Result<PathBuf> {
    let dir = dirs::config_dir()
        .context("could not find the user configuration directory")?
        .join("cloudreve-sync");
    fs::create_dir_all(&dir).context("could not create configuration directory")?;
    Ok(dir)
}

pub fn data_path(name: &str) -> Result<PathBuf> {
    Ok(app_dir()?.join(name))
}

pub fn load_config() -> Result<AppConfig> {
    load_or_default("config.json")
}

pub fn save_config(config: &AppConfig) -> Result<()> {
    save("config.json", config)
}

pub fn load_state() -> Result<SyncState> {
    load_or_default("state.json")
}

pub fn save_state(state: &SyncState) -> Result<()> {
    save("state.json", state)
}

pub fn acquire_instance_lock() -> Result<InstanceLock> {
    let path = app_dir()?.join("instance.lock");
    acquire_instance_lock_at(path)
}

fn acquire_instance_lock_at(path: PathBuf) -> Result<InstanceLock> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("could not open {}", path.display()))?;
    file.try_lock_exclusive()
        .context("Cloudreve Sync is already running")?;
    Ok(InstanceLock { _file: file })
}

fn load_or_default<T: serde::de::DeserializeOwned + Default>(name: &str) -> Result<T> {
    let path = app_dir()?.join(name);
    load_or_default_at(path)
}

fn load_or_default_at<T: serde::de::DeserializeOwned + Default>(path: PathBuf) -> Result<T> {
    match fs::read(&path) {
        Ok(data) => serde_json::from_slice(&data)
            .with_context(|| format!("could not parse {}", path.display())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error).with_context(|| format!("could not read {}", path.display())),
    }
}

fn save<T: serde::Serialize>(name: &str, value: &T) -> Result<()> {
    let path = app_dir()?.join(name);
    let parent = path.parent().context("data file has no parent directory")?;
    let mut temp = tempfile::Builder::new()
        .prefix(".cloudreve-sync-")
        .tempfile_in(parent)?;
    temp.write_all(&serde_json::to_vec_pretty(value)?)?;
    temp.as_file().sync_all()?;
    temp.persist(&path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_lock_is_exclusive() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("instance.lock");
        let first = acquire_instance_lock_at(path.clone()).unwrap();
        assert!(acquire_instance_lock_at(path.clone()).is_err());
        drop(first);
        assert!(acquire_instance_lock_at(path).is_ok());
    }

    #[test]
    fn malformed_data_is_not_silently_defaulted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        fs::write(&path, b"not json").unwrap();

        let result: Result<SyncState> = load_or_default_at(path.clone());

        assert!(result.is_err());
        assert_eq!(fs::read(path).unwrap(), b"not json");
    }

    #[test]
    fn missing_data_uses_the_default() {
        let directory = tempfile::tempdir().unwrap();
        let state: SyncState = load_or_default_at(directory.path().join("missing.json")).unwrap();
        assert!(state.mappings.is_empty());
    }
}
