use crate::model::{AppConfig, SyncState};
use anyhow::{Context, Result};
use std::{fs, path::PathBuf};

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

pub fn load_config() -> AppConfig {
    load("config.json").unwrap_or_default()
}

pub fn save_config(config: &AppConfig) -> Result<()> {
    save("config.json", config)
}

pub fn load_state() -> SyncState {
    load("state.json").unwrap_or_default()
}

pub fn save_state(state: &SyncState) -> Result<()> {
    save("state.json", state)
}

fn load<T: serde::de::DeserializeOwned>(name: &str) -> Result<T> {
    let data = fs::read(app_dir()?.join(name))?;
    Ok(serde_json::from_slice(&data)?)
}

fn save<T: serde::Serialize>(name: &str, value: &T) -> Result<()> {
    let path = app_dir()?.join(name);
    let temp = path.with_extension("tmp");
    fs::write(&temp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temp, path)?;
    Ok(())
}
