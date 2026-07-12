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
use tokio::sync::mpsc;
use uuid::Uuid;

fn main() -> eframe::Result {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sync::run(commands_rx, events_tx));
    });
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([720.0, 560.0])
            .with_min_inner_size([560.0, 420.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Cloudreve Sync",
        options,
        Box::new(|cc| Ok(Box::new(SyncApp::new(cc, commands_tx, events_rx)))),
    )
}

struct SyncApp {
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
}

impl SyncApp {
    fn new(
        cc: &eframe::CreationContext<'_>,
        commands: mpsc::UnboundedSender<BackendCommand>,
        events: mpsc::UnboundedReceiver<BackendEvent>,
    ) -> Self {
        cc.egui_ctx.set_visuals(egui::Visuals::dark());
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
        }
    }

    fn save(&mut self) {
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

    fn record_activity(&mut self, message: String) {
        self.activity.push_front(message);
        self.activity.truncate(12);
    }
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
                            });
                        }
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
                    });
                }
                if let Some(index) = remove {
                    self.config.mappings.remove(index);
                }
                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    if ui.button("Save settings").clicked() {
                        self.save();
                    }
                    if ui.button("Sync now").clicked() {
                        self.save();
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
    }

    fn on_exit(&mut self, _: Option<&eframe::glow::Context>) {
        let _ = self.commands.send(BackendCommand::Shutdown);
    }
}
