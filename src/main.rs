mod autostart;
mod local_index;
mod model;
mod storage;
mod sync;
mod webdav;

use eframe::egui;
use model::{
    AppConfig, BackendCommand, BackendEvent, Conflict, ConflictChoice, SyncMapping, Transfer,
    TransferDirection,
};
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

fn main() -> anyhow::Result<()> {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sync::run(commands_rx, events_tx));
    });
    let (tray, tray_events) = create_tray()?;
    let tray_events = Arc::new(Mutex::new(tray_events));
    let mut state = Some(SyncState::new(commands_tx.clone(), events_rx));
    loop {
        let (state_tx, state_rx) = std_mpsc::channel();
        let app_state = state.take().expect("application state must be available");
        let options = native_options();
        let app_tray_events = tray_events.clone();
        eframe::run_native(
            "Cloudreve Sync",
            options,
            Box::new(move |cc| {
                cc.egui_ctx.set_visuals(egui::Visuals::dark());
                Ok(Box::new(SyncApp {
                    state: Some(app_state),
                    state_tx,
                    tray_events: app_tray_events,
                    quitting: false,
                }))
            }),
        )
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (returned_state, quitting) = state_rx
            .recv()
            .expect("window must return application state");
        state = Some(returned_state);
        if quitting {
            let _ = commands_tx.send(BackendCommand::Shutdown);
            drop(tray);
            return Ok(());
        }
        loop {
            match tray_events.lock().unwrap().recv() {
                Ok(TrayEvent::Show) => break,
                Ok(TrayEvent::Sync) => {
                    let _ = commands_tx.send(BackendCommand::SyncNow);
                }
                Ok(TrayEvent::Quit) | Err(_) => {
                    let _ = commands_tx.send(BackendCommand::Shutdown);
                    drop(tray);
                    return Ok(());
                }
            }
        }
    }
}

fn native_options() -> eframe::NativeOptions {
    let (rgba, width, height) = app_icon();
    eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Cloudreve Sync")
            .with_app_id("cloudreve-sync")
            .with_inner_size([720.0, 560.0])
            .with_min_inner_size([560.0, 420.0])
            .with_icon(Arc::new(egui::IconData {
                rgba,
                width,
                height,
            })),
        ..Default::default()
    }
}

struct SyncState {
    config: AppConfig,
    commands: mpsc::UnboundedSender<BackendCommand>,
    events: mpsc::UnboundedReceiver<BackendEvent>,
    status: String,
    errors: Vec<String>,
    conflicts: BTreeMap<(Uuid, String), Conflict>,
    active_transfer: Option<Transfer>,
    activity: VecDeque<String>,
    syncing: bool,
    uploads: usize,
    downloads: usize,
    transferred_bytes: u64,
    folder_selection: Option<FolderSelection>,
}

struct SyncApp {
    state: Option<SyncState>,
    state_tx: std_mpsc::Sender<(SyncState, bool)>,
    tray_events: Arc<Mutex<std_mpsc::Receiver<TrayEvent>>>,
    quitting: bool,
}

impl Deref for SyncApp {
    type Target = SyncState;

    fn deref(&self) -> &Self::Target {
        self.state.as_ref().unwrap()
    }
}

impl DerefMut for SyncApp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_mut().unwrap()
    }
}

struct FolderSelection {
    root: PathBuf,
    remote_parent: String,
    folders: Vec<(PathBuf, bool)>,
}

impl SyncState {
    fn new(
        commands: mpsc::UnboundedSender<BackendCommand>,
        events: mpsc::UnboundedReceiver<BackendEvent>,
    ) -> Self {
        Self {
            config: storage::load_config(),
            commands,
            events,
            status: "Ready".into(),
            errors: Vec::new(),
            conflicts: BTreeMap::new(),
            active_transfer: None,
            activity: VecDeque::new(),
            syncing: false,
            uploads: 0,
            downloads: 0,
            transferred_bytes: 0,
            folder_selection: None,
        }
    }

    fn save_settings(&mut self) {
        if let Err(error) = autostart::set_enabled(self.config.autostart) {
            self.errors
                .push(format!("Could not update autostart: {error}"));
            return;
        }
        match storage::save_config(&self.config) {
            Ok(()) => {
                let _ = self
                    .commands
                    .send(BackendCommand::UpdateConfig(self.config.clone()));
                self.status = "Settings saved".into();
            }
            Err(error) => self.errors.push(error.to_string()),
        }
    }

    fn choose_folder_group(&mut self) {
        let Some(root) = rfd::FileDialog::new().pick_folder() else {
            return;
        };
        match std::fs::read_dir(&root) {
            Ok(entries) => {
                let mut folders: Vec<_> = entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                    .map(|entry| (entry.path(), false))
                    .collect();
                folders.sort_by(|left, right| left.0.file_name().cmp(&right.0.file_name()));
                self.folder_selection = Some(FolderSelection {
                    root,
                    remote_parent: String::new(),
                    folders,
                });
            }
            Err(error) => self.errors.push(format!("Could not read folder: {error}")),
        }
    }

    fn add_selected_folders(&mut self, selection: FolderSelection) {
        for (path, selected) in selection.folders {
            if !selected
                || self
                    .config
                    .mappings
                    .iter()
                    .any(|mapping| mapping.local_path == path)
            {
                continue;
            }
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default();
            let remote_path = if selection.remote_parent.trim_matches('/').is_empty() {
                name.into_owned()
            } else {
                format!("{}/{}", selection.remote_parent.trim_matches('/'), name)
            };
            self.config.mappings.push(SyncMapping {
                id: Uuid::new_v4(),
                local_path: path,
                remote_path,
                enabled: true,
                ignore_patterns: String::new(),
            });
        }
    }

    fn record_activity(&mut self, message: String) {
        self.activity.push_front(message);
        self.activity.truncate(12);
    }
}

#[derive(Clone, Copy)]
enum TrayEvent {
    Show,
    Sync,
    Quit,
}

struct CloudreveTray {
    events: std_mpsc::Sender<TrayEvent>,
    icon: Vec<ksni::Icon>,
}

impl ksni::Tray for CloudreveTray {
    fn id(&self) -> String {
        "cloudreve-sync".into()
    }

    fn title(&self) -> String {
        "Cloudreve Sync".into()
    }

    fn icon_name(&self) -> String {
        String::new()
    }

    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        self.icon.clone()
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.events.send(TrayEvent::Show);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        use ksni::menu::StandardItem;
        vec![
            StandardItem {
                label: "Show Cloudreve Sync".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.events.send(TrayEvent::Show);
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Sync now".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.events.send(TrayEvent::Sync);
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Quit".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.events.send(TrayEvent::Quit);
                }),
                ..Default::default()
            }
            .into(),
        ]
    }
}

fn create_tray() -> anyhow::Result<(
    ksni::blocking::Handle<CloudreveTray>,
    std_mpsc::Receiver<TrayEvent>,
)> {
    let image = image::load_from_memory(include_bytes!("../logo-sync.png"))?.into_rgba8();
    let image = image::imageops::resize(&image, 64, 64, image::imageops::FilterType::Lanczos3);
    let (width, height) = image.dimensions();
    let rgba = image.into_raw();
    let argb = rgba
        .chunks_exact(4)
        .flat_map(|pixel| [pixel[3], pixel[0], pixel[1], pixel[2]])
        .collect();
    let (events, receiver) = std_mpsc::channel();
    let tray = ksni::blocking::TrayMethods::spawn(CloudreveTray {
        events,
        icon: vec![ksni::Icon {
            width: width as i32,
            height: height as i32,
            data: argb,
        }],
    })?;
    Ok((tray, receiver))
}

fn app_icon() -> (Vec<u8>, u32, u32) {
    let image = image::load_from_memory(include_bytes!("../logo-sync.png"))
        .expect("embedded application logo must be a valid PNG")
        .into_rgba8();
    let (width, height) = image.dimensions();
    (image.into_raw(), width, height)
}

fn direction_label(direction: TransferDirection) -> &'static str {
    match direction {
        TransferDirection::Upload => "Uploading",
        TransferDirection::Download => "Downloading",
        TransferDirection::DeleteLocal => "Deleting locally",
        TransferDirection::DeleteRemote => "Deleting remotely",
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

impl eframe::App for SyncApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        loop {
            let event = self.tray_events.lock().unwrap().try_recv();
            let Ok(event) = event else { break };
            match event {
                TrayEvent::Show => {
                    ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                }
                TrayEvent::Sync => {
                    self.save_settings();
                    let _ = self.commands.send(BackendCommand::SyncNow);
                }
                TrayEvent::Quit => {
                    self.quitting = true;
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                }
            }
        }
        if ctx.input(|input| input.viewport().close_requested()) && !self.quitting {
            self.record_activity("Closed window to the system tray".into());
        }
        while let Ok(event) = self.events.try_recv() {
            match event {
                BackendEvent::Status(status) => self.status = status,
                BackendEvent::Error(error) => self.errors.push(error),
                BackendEvent::Conflict(conflict) => {
                    self.conflicts.insert(
                        (conflict.mapping_id, conflict.relative_path.clone()),
                        conflict,
                    );
                }
                BackendEvent::SyncStarted => {
                    self.syncing = true;
                    self.conflicts.clear();
                    self.uploads = 0;
                    self.downloads = 0;
                    self.transferred_bytes = 0;
                    self.record_activity("Started checking for changes".into());
                }
                BackendEvent::SyncFinished(success) => {
                    self.syncing = false;
                    self.active_transfer = None;
                    self.record_activity(if success {
                        "Sync check completed".into()
                    } else {
                        "Sync check stopped with an error".into()
                    });
                }
                BackendEvent::TransferStarted(transfer) => {
                    self.status = format!(
                        "{} {}",
                        direction_label(transfer.direction),
                        transfer.relative_path
                    );
                    self.active_transfer = Some(transfer);
                }
                BackendEvent::TransferFinished(transfer, success) => {
                    self.active_transfer = None;
                    if success {
                        match transfer.direction {
                            TransferDirection::Upload => self.uploads += 1,
                            TransferDirection::Download => self.downloads += 1,
                            _ => {}
                        }
                        self.transferred_bytes += transfer.bytes.unwrap_or(0);
                    }
                    self.record_activity(format!(
                        "{} {}{}",
                        direction_label(transfer.direction),
                        transfer.relative_path,
                        if success { " - complete" } else { " - failed" }
                    ));
                }
            }
        }
        ctx.request_repaint_after(std::time::Duration::from_millis(500));

        egui::TopBottomPanel::top("header").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Cloudreve Sync");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(&self.status);
                });
            });
        });
        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.group(|ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        if self.syncing {
                            ui.spinner();
                        }
                        ui.strong(&self.status);
                    });
                    if let Some(transfer) = &self.active_transfer {
                        ui.add(egui::ProgressBar::new(0.5).animate(true).text(format!(
                            "{}: {}{}",
                            direction_label(transfer.direction),
                            transfer.relative_path,
                            transfer
                                .bytes
                                .map(|bytes| format!(" ({})", format_bytes(bytes)))
                                .unwrap_or_default()
                        )));
                    } else if self.syncing {
                        ui.add(
                            egui::ProgressBar::new(0.5)
                                .animate(true)
                                .text(self.status.clone()),
                        );
                    }
                    ui.horizontal_wrapped(|ui| {
                        ui.label(format!("Uploads: {}", self.uploads));
                        ui.separator();
                        ui.label(format!("Downloads: {}", self.downloads));
                        ui.separator();
                        ui.label(format!(
                            "Transferred: {}",
                            format_bytes(self.transferred_bytes)
                        ));
                        if !self.conflicts.is_empty() {
                            ui.separator();
                            ui.colored_label(
                                egui::Color32::YELLOW,
                                format!("Conflicts: {}", self.conflicts.len()),
                            );
                        }
                    });
                });
                ui.add_space(18.0);
                ui.heading("Connection");
                egui::Grid::new("connection")
                    .num_columns(2)
                    .spacing([12.0, 8.0])
                    .show(ui, |ui| {
                        ui.label("WebDAV URL");
                        ui.text_edit_singleline(&mut self.config.server_url);
                        ui.end_row();
                        ui.label("Username");
                        ui.text_edit_singleline(&mut self.config.username);
                        ui.end_row();
                        ui.label("Password");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.config.password).password(true),
                        );
                        ui.end_row();
                        ui.label("Check every (seconds)");
                        ui.add(egui::DragValue::new(&mut self.config.poll_seconds).range(2..=3600));
                        ui.end_row();
                        ui.label("Desktop");
                        ui.checkbox(
                            &mut self.config.autostart,
                            "Start automatically when I sign in",
                        );
                        ui.end_row();
                    });
                ui.add_space(18.0);
                ui.horizontal(|ui| {
                    ui.heading("Folders");
                    if ui.button("Add folder").clicked() {
                        if let Some(path) = rfd::FileDialog::new().pick_folder() {
                            self.config.mappings.push(SyncMapping {
                                id: Uuid::new_v4(),
                                local_path: path,
                                remote_path: String::new(),
                                enabled: true,
                                ignore_patterns: String::new(),
                            });
                        }
                    }
                    if ui.button("Add some folders...").clicked() {
                        self.choose_folder_group();
                    }
                });
                let mut remove = None;
                for (index, mapping) in self.config.mappings.iter_mut().enumerate() {
                    ui.group(|ui| {
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut mapping.enabled, "Enabled");
                            if ui.small_button("Remove").clicked() {
                                remove = Some(index);
                            }
                        });
                        ui.horizontal(|ui| {
                            ui.label("Local");
                            ui.monospace(mapping.local_path.display().to_string());
                        });
                        ui.horizontal(|ui| {
                            ui.label("Remote");
                            ui.text_edit_singleline(&mut mapping.remote_path);
                        });
                        ui.label("Ignore patterns (one gitignore-style pattern per line)");
                        ui.add(
                            egui::TextEdit::multiline(&mut mapping.ignore_patterns)
                                .desired_rows(2)
                                .hint_text("*.tmp\n.cache/\n**/target/"),
                        );
                    });
                }
                if let Some(index) = remove {
                    self.config.mappings.remove(index);
                }
                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    if ui.button("Save settings").clicked() {
                        self.save_settings();
                    }
                    if ui.button("Sync now").clicked() {
                        self.save_settings();
                        let _ = self.commands.send(BackendCommand::SyncNow);
                    }
                });

                if !self.conflicts.is_empty() {
                    ui.add_space(20.0);
                    ui.separator();
                    ui.heading("Conflicts");
                    let conflicts: Vec<_> = self.conflicts.values().cloned().collect();
                    for conflict in conflicts {
                        ui.horizontal_wrapped(|ui| {
                            ui.label(&conflict.relative_path);
                            if !conflict.local_exists {
                                ui.label("(deleted locally)");
                            }
                            if !conflict.remote_exists {
                                ui.label("(deleted remotely)");
                            }
                            if ui.button("Keep local").clicked() {
                                let _ = self.commands.send(BackendCommand::Resolve(
                                    conflict.clone(),
                                    ConflictChoice::KeepLocal,
                                ));
                                self.conflicts
                                    .remove(&(conflict.mapping_id, conflict.relative_path.clone()));
                            }
                            if ui.button("Keep remote").clicked() {
                                let _ = self.commands.send(BackendCommand::Resolve(
                                    conflict.clone(),
                                    ConflictChoice::KeepRemote,
                                ));
                                self.conflicts
                                    .remove(&(conflict.mapping_id, conflict.relative_path.clone()));
                            }
                        });
                    }
                }
                if !self.errors.is_empty() {
                    ui.add_space(20.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.colored_label(egui::Color32::LIGHT_RED, "Errors");
                        if ui.small_button("Clear").clicked() {
                            self.errors.clear();
                        }
                    });
                    for error in self.errors.iter().rev().take(5) {
                        ui.colored_label(egui::Color32::LIGHT_RED, error);
                    }
                }
                if !self.activity.is_empty() {
                    ui.add_space(20.0);
                    ui.separator();
                    ui.heading("Recent activity");
                    for item in &self.activity {
                        ui.label(item);
                    }
                }
            });
        });

        if let Some(mut selection) = self.folder_selection.take() {
            let mut open = true;
            let mut add = false;
            egui::Window::new("Add some folders")
                .open(&mut open)
                .collapsible(false)
                .resizable(true)
                .show(ctx, |ui| {
                    ui.label("Top folder");
                    ui.monospace(selection.root.display().to_string());
                    ui.add_space(8.0);
                    ui.label("Remote parent folder");
                    ui.text_edit_singleline(&mut selection.remote_parent);
                    ui.label("Each selected folder will be mapped below this remote folder.");
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.small_button("Select all").clicked() {
                            for (_, selected) in &mut selection.folders {
                                *selected = true;
                            }
                        }
                        if ui.small_button("Select none").clicked() {
                            for (_, selected) in &mut selection.folders {
                                *selected = false;
                            }
                        }
                    });
                    egui::ScrollArea::vertical()
                        .max_height(280.0)
                        .show(ui, |ui| {
                            for (path, selected) in &mut selection.folders {
                                let name = path
                                    .file_name()
                                    .map(|name| name.to_string_lossy())
                                    .unwrap_or_default();
                                ui.checkbox(selected, name);
                            }
                        });
                    ui.add_space(8.0);
                    if ui
                        .add_enabled(
                            selection.folders.iter().any(|(_, selected)| *selected),
                            egui::Button::new("Add selected folders"),
                        )
                        .clicked()
                    {
                        add = true;
                    }
                });
            if add {
                self.add_selected_folders(selection);
            } else if open {
                self.folder_selection = Some(selection);
            }
        }
    }

    fn on_exit(&mut self, _: Option<&eframe::glow::Context>) {
        let _ = self
            .state_tx
            .send((self.state.take().unwrap(), self.quitting));
    }
}
