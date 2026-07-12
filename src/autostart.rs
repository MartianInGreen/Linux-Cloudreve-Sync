use anyhow::{Context, Result};
use std::{fs, path::PathBuf};

fn desktop_file() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("could not find the user configuration directory")?
        .join("autostart/cloudreve-sync.desktop"))
}

pub fn set_enabled(enabled: bool) -> Result<()> {
    let path = desktop_file()?;
    if !enabled {
        if path.exists() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }
    let executable = std::env::current_exe().context("could not locate the application binary")?;
    let parent = path.parent().context("invalid autostart path")?;
    fs::create_dir_all(parent)?;
    let escaped = executable
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    fs::write(
        path,
        format!(
            "[Desktop Entry]\nType=Application\nName=Cloudreve Sync\nComment=Synchronize Cloudreve folders\nExec=\"{escaped}\"\nIcon=cloudreve-sync\nTerminal=false\nX-GNOME-Autostart-enabled=true\n"
        ),
    )?;
    Ok(())
}
