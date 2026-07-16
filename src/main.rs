mod autostart;
mod local_index;
mod model;
mod storage;
mod sync;
mod webdav;

use egui_winit::winit;
use egui_winit::winit::raw_window_handle::HasWindowHandle;
use model::{
    AppConfig, BackendCommand, BackendEvent, Conflict, ConflictChoice, SyncMapping, Transfer,
    TransferDirection,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;
use winit::event::WindowEvent;
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop, EventLoopProxy};
use winit::window::{Icon, WindowAttributes, WindowId};

// ── Glutin GL window context (adapted from egui_glow pure_glow example) ──

struct GlutinWindowContext {
    window: winit::window::Window,
    gl_context: glutin::context::PossiblyCurrentContext,
    gl_display: glutin::display::Display,
    gl_surface: glutin::surface::Surface<glutin::surface::WindowSurface>,
}

impl GlutinWindowContext {
    #[allow(unsafe_code)]
    unsafe fn new(event_loop: &ActiveEventLoop) -> Self {
        use glutin::context::NotCurrentGlContext;
        use glutin::display::GetGlDisplay;
        use glutin::display::GlDisplay;
        use glutin::prelude::GlSurface;

        let (icon_rgba, icon_w, icon_h) = app_icon();
        let window_attrs = WindowAttributes::default()
            .with_title("Cloudreve Sync")
            .with_inner_size(winit::dpi::LogicalSize {
                width: 900.0,
                height: 680.0,
            })
            .with_min_inner_size(winit::dpi::LogicalSize {
                width: 600.0,
                height: 460.0,
            })
            .with_window_icon(Icon::from_rgba(icon_rgba, icon_w, icon_h).ok())
            .with_visible(false);

        let config_template = glutin::config::ConfigTemplateBuilder::new()
            .prefer_hardware_accelerated(None)
            .with_depth_size(0)
            .with_stencil_size(0)
            .with_transparency(false);

        let (mut window, gl_config) = glutin_winit::DisplayBuilder::new()
            .with_preference(glutin_winit::ApiPreference::FallbackEgl)
            .with_window_attributes(Some(window_attrs.clone()))
            .build(event_loop, config_template, |mut iter| {
                iter.next().expect("no matching GL config")
            })
            .expect("failed to create GL config");
        let gl_display = gl_config.display();

        let raw_handle = window.as_ref().map(|w| {
            w.window_handle()
                .expect("failed to get window handle")
                .as_raw()
        });
        let context_attrs = glutin::context::ContextAttributesBuilder::new().build(raw_handle);
        let fallback_attrs = glutin::context::ContextAttributesBuilder::new()
            .with_context_api(glutin::context::ContextApi::Gles(None))
            .build(raw_handle);

        let not_current = unsafe {
            gl_display
                .create_context(&gl_config, &context_attrs)
                .unwrap_or_else(|_| {
                    gl_display
                        .create_context(&gl_config, &fallback_attrs)
                        .expect("failed to create GL context")
                })
        };

        let window = window.take().unwrap_or_else(|| {
            glutin_winit::finalize_window(event_loop, window_attrs, &gl_config)
                .expect("failed to finalize window")
        });

        let (w, h): (u32, u32) = window.inner_size().into();
        let w = NonZeroU32::new(w).unwrap_or(NonZeroU32::MIN);
        let h = NonZeroU32::new(h).unwrap_or(NonZeroU32::MIN);
        let surface_attrs =
            glutin::surface::SurfaceAttributesBuilder::<glutin::surface::WindowSurface>::new()
                .build(
                    window
                        .window_handle()
                        .expect("failed to get window handle")
                        .as_raw(),
                    w,
                    h,
                );
        let gl_surface = unsafe {
            gl_display
                .create_window_surface(&gl_config, &surface_attrs)
                .unwrap()
        };
        let gl_context = not_current.make_current(&gl_surface).unwrap();
        gl_surface
            .set_swap_interval(
                &gl_context,
                glutin::surface::SwapInterval::Wait(NonZeroU32::MIN),
            )
            .unwrap();

        Self {
            window,
            gl_context,
            gl_display,
            gl_surface,
        }
    }

    fn resize(&self, size: winit::dpi::PhysicalSize<u32>) {
        use glutin::surface::GlSurface;
        self.gl_surface.resize(
            &self.gl_context,
            size.width.try_into().unwrap(),
            size.height.try_into().unwrap(),
        );
    }

    fn swap_buffers(&self) {
        use glutin::surface::GlSurface;
        let _ = self.gl_surface.swap_buffers(&self.gl_context);
    }

    fn get_proc_address(&self, addr: &std::ffi::CStr) -> *const std::ffi::c_void {
        use glutin::display::GlDisplay;
        self.gl_display.get_proc_address(addr)
    }
}

// ── User events ──

#[derive(Debug)]
enum AppUserEvent {
    Tray(TrayEvent),
    Redraw(std::time::Duration),
    PickedAddFolder(PathBuf),
    PickedFolderGroup(PathBuf),
}

#[derive(Clone, Copy, Debug)]
enum TrayEvent {
    Show,
    Sync,
    Quit,
}

// ── App ──

struct App {
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
    quitting: bool,
    folder_selection: Option<FolderSelection>,
    folder_filter: String,
    expanded_mapping: Option<Uuid>,
    pending_remove: Option<Uuid>,
    collapsed_folder_groups: BTreeSet<PathBuf>,
    proxy: EventLoopProxy<AppUserEvent>,
    _tray: ksni::blocking::Handle<CloudreveTray>,
    // Window state — None when hidden to tray
    gl_window: Option<GlutinWindowContext>,
    gl: Option<Arc<glow::Context>>,
    egui_glow: Option<egui_glow::EguiGlow>,
    repaint_delay: std::time::Duration,
}

struct FolderSelection {
    root: PathBuf,
    remote_parent: String,
    filter: String,
    folders: Vec<(PathBuf, bool)>,
}

enum FolderListRow {
    Group(PathBuf, usize),
    Mapping(usize),
}

impl App {
    fn new(
        commands: mpsc::UnboundedSender<BackendCommand>,
        events: mpsc::UnboundedReceiver<BackendEvent>,
        proxy: EventLoopProxy<AppUserEvent>,
        tray: ksni::blocking::Handle<CloudreveTray>,
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
            quitting: false,
            folder_selection: None,
            folder_filter: String::new(),
            expanded_mapping: None,
            pending_remove: None,
            collapsed_folder_groups: BTreeSet::new(),
            proxy,
            _tray: tray,
            gl_window: None,
            gl: None,
            egui_glow: None,
            repaint_delay: std::time::Duration::MAX,
        }
    }

    fn create_window(&mut self, event_loop: &ActiveEventLoop) {
        if self.gl_window.is_some() {
            return;
        }
        let gl_window = unsafe { GlutinWindowContext::new(event_loop) };
        let gl = unsafe {
            Arc::new(glow::Context::from_loader_function(|s| {
                let s = std::ffi::CString::new(s).expect("CString for gl proc");
                gl_window.get_proc_address(&s)
            }))
        };
        let egui_glow = egui_glow::EguiGlow::new(event_loop, gl.clone(), None, None, true);
        configure_ui(&egui_glow.egui_ctx);

        let proxy = egui::mutex::Mutex::new(self.proxy.clone());
        egui_glow
            .egui_ctx
            .set_request_repaint_callback(move |info| {
                let _ = proxy.lock().send_event(AppUserEvent::Redraw(info.delay));
            });

        gl_window.window.set_visible(true);
        gl_window.window.request_redraw();

        self.gl_window = Some(gl_window);
        self.gl = Some(gl);
        self.egui_glow = Some(egui_glow);
    }

    fn destroy_window(&mut self) {
        if let Some(mut eg) = self.egui_glow.take() {
            eg.destroy();
        }
        self.gl = None;
        self.gl_window = None;
        self.repaint_delay = std::time::Duration::MAX;
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
        let proxy = self.proxy.clone();
        std::thread::spawn(move || {
            if let Some(path) = pick_folder("Select a folder group") {
                let _ = proxy.send_event(AppUserEvent::PickedFolderGroup(path));
            }
        });
    }

    fn finish_choose_folder_group(&mut self, root: PathBuf) {
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
                    filter: String::new(),
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

    fn poll_backend_events(&mut self) {
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
    }

    fn render_messages(&mut self, ui: &mut egui::Ui) {
        if !self.conflicts.is_empty() {
            ui.add_space(16.0);
            ui.label(section_title("Needs attention"));
            let conflicts: Vec<_> = self.conflicts.values().cloned().collect();
            section_frame().show(ui, |ui| {
                for conflict in conflicts {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(egui::RichText::new(&conflict.relative_path).strong());
                        if !conflict.local_exists {
                            ui.label("Deleted locally");
                        }
                        if !conflict.remote_exists {
                            ui.label("Deleted remotely");
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
                    ui.separator();
                }
            });
        }

        if !self.errors.is_empty() {
            ui.add_space(16.0);
            ui.horizontal(|ui| {
                ui.label(section_title("Errors").color(egui::Color32::from_rgb(238, 132, 139)));
                if ui.small_button("Clear").clicked() {
                    self.errors.clear();
                }
            });
            egui::Frame::new()
                .fill(egui::Color32::from_rgb(55, 31, 36))
                .corner_radius(7.0)
                .inner_margin(egui::Margin::same(14))
                .show(ui, |ui| {
                    for error in self.errors.iter().rev().take(5) {
                        ui.colored_label(egui::Color32::from_rgb(245, 176, 181), error);
                    }
                });
        }

        if !self.activity.is_empty() {
            ui.add_space(16.0);
            egui::CollapsingHeader::new(format!("Recent activity ({})", self.activity.len()))
                .id_salt("recent_activity")
                .show(ui, |ui| {
                    section_frame().show(ui, |ui| {
                        for item in &self.activity {
                            ui.label(
                                egui::RichText::new(item)
                                    .color(egui::Color32::from_rgb(174, 185, 195)),
                            );
                        }
                    });
                });
        }
    }

    fn render_ui(&mut self, egui_ctx: &egui::Context) {
        egui::TopBottomPanel::top("header")
            .exact_height(62.0)
            .frame(
                egui::Frame::new()
                    .fill(egui::Color32::from_rgb(17, 24, 32))
                    .inner_margin(egui::Margin::symmetric(14, 6)),
            )
            .show(egui_ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new("CLOUDREVE")
                                .size(11.0)
                                .strong()
                                .color(egui::Color32::from_rgb(84, 193, 173)),
                        );
                        ui.label(egui::RichText::new("Folder sync").size(20.0).strong());
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let needs_attention = !self.errors.is_empty() || !self.conflicts.is_empty();
                        let state = if self.syncing {
                            "SYNCING"
                        } else if needs_attention {
                            "ATTENTION"
                        } else {
                            "READY"
                        };
                        ui.label(egui::RichText::new(state).size(11.0).strong().color(
                            if self.syncing {
                                egui::Color32::from_rgb(84, 193, 173)
                            } else if needs_attention {
                                egui::Color32::from_rgb(238, 132, 139)
                            } else {
                                egui::Color32::from_rgb(150, 163, 175)
                            },
                        ));
                        if self.syncing {
                            ui.spinner();
                        }
                    });
                });
            });

        egui::TopBottomPanel::bottom("actions")
            .exact_height(58.0)
            .frame(
                egui::Frame::new()
                    .fill(egui::Color32::from_rgb(17, 24, 32))
                    .inner_margin(egui::Margin::symmetric(18, 11)),
            )
            .show(egui_ctx, |ui| {
                ui.horizontal(|ui| {
                    if ui.button("Save settings").clicked() {
                        self.save_settings();
                    }
                    if ui
                        .add_enabled(
                            !self.syncing,
                            egui::Button::new("Sync now")
                                .fill(egui::Color32::from_rgb(41, 139, 126)),
                        )
                        .clicked()
                    {
                        self.save_settings();
                        let _ = self.commands.send(BackendCommand::SyncNow);
                    }
                });
            });

        egui::CentralPanel::default().show(egui_ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                section_frame().show(ui, |ui| {
                    ui.label(section_title("Sync status"));
                    ui.horizontal_wrapped(|ui| {
                        metric(ui, "Uploads", &self.uploads.to_string());
                        ui.separator();
                        metric(ui, "Downloads", &self.downloads.to_string());
                        ui.separator();
                        metric(ui, "Transferred", &format_bytes(self.transferred_bytes));
                    });
                    ui.add_space(4.0);
                    ui.add(
                        egui::Label::new(
                            egui::RichText::new(&self.status)
                                .size(15.0)
                                .color(egui::Color32::from_rgb(225, 230, 235)),
                        )
                        .truncate(),
                    )
                    .on_hover_text(&self.status);
                    if let Some(transfer) = &self.active_transfer {
                        ui.add_space(7.0);
                        ui.add(egui::ProgressBar::new(0.5).animate(true).text(format!(
                            "{}{}",
                            direction_label(transfer.direction),
                            transfer
                                .bytes
                                .map(|bytes| format!("  {}", format_bytes(bytes)))
                                .unwrap_or_default()
                        )));
                    } else if self.syncing {
                        ui.add_space(7.0);
                        ui.add(egui::ProgressBar::new(0.5).animate(true));
                    }
                });

                ui.add_space(12.0);
                egui::CollapsingHeader::new("Connection and schedule")
                    .id_salt("connection_settings")
                    .default_open(self.config.server_url.is_empty())
                    .show(ui, |ui| {
                        section_frame().show(ui, |ui| {
                            ui.label(field_label("WEBDAV URL"));
                            ui.add_sized(
                                [ui.available_width(), 28.0],
                                egui::TextEdit::singleline(&mut self.config.server_url)
                                    .hint_text("https://cloud.example.com/dav"),
                            );
                            ui.add_space(8.0);
                            if ui.available_width() >= 620.0 {
                                ui.columns(2, |columns| {
                                    columns[0].label(field_label("USERNAME"));
                                    columns[0].add_sized(
                                        [columns[0].available_width(), 28.0],
                                        egui::TextEdit::singleline(&mut self.config.username),
                                    );
                                    columns[1].label(field_label("PASSWORD"));
                                    columns[1].add_sized(
                                        [columns[1].available_width(), 28.0],
                                        egui::TextEdit::singleline(&mut self.config.password)
                                            .password(true),
                                    );
                                });
                            } else {
                                ui.label(field_label("USERNAME"));
                                ui.text_edit_singleline(&mut self.config.username);
                                ui.label(field_label("PASSWORD"));
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.config.password)
                                        .password(true),
                                );
                            }
                            ui.add_space(10.0);
                            ui.horizontal_wrapped(|ui| {
                                ui.label("Check for changes every");
                                ui.add(
                                    egui::DragValue::new(&mut self.config.poll_seconds)
                                        .range(2..=3600)
                                        .suffix(" seconds"),
                                );
                                ui.separator();
                                ui.checkbox(&mut self.config.autostart, "Start when I sign in");
                            });
                        });
                    });

                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(section_title("Folders"));
                        let enabled = self.config.mappings.iter().filter(|m| m.enabled).count();
                        ui.label(
                            egui::RichText::new(format!(
                                "{enabled} of {} folders active",
                                self.config.mappings.len()
                            ))
                            .color(egui::Color32::from_rgb(145, 157, 168)),
                        );
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Add folder group").clicked() {
                            self.choose_folder_group();
                        }
                        if ui.button("Add folder").clicked() {
                            let proxy = self.proxy.clone();
                            std::thread::spawn(move || {
                                if let Some(path) = pick_folder("Select a folder to sync") {
                                    let _ = proxy.send_event(AppUserEvent::PickedAddFolder(path));
                                }
                            });
                        }
                    });
                });
                ui.add_space(10.0);

                section_frame().show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [ui.available_width().max(280.0) - 230.0, 28.0],
                            egui::TextEdit::singleline(&mut self.folder_filter)
                                .hint_text("Filter by local or remote path"),
                        );
                        if ui.small_button("Enable all").clicked() {
                            for mapping in &mut self.config.mappings {
                                mapping.enabled = true;
                            }
                        }
                        if ui.small_button("Disable all").clicked() {
                            for mapping in &mut self.config.mappings {
                                mapping.enabled = false;
                            }
                        }
                    });
                    ui.add_space(8.0);

                    let query = self.folder_filter.trim().to_lowercase();
                    let visible: Vec<_> = self
                        .config
                        .mappings
                        .iter()
                        .enumerate()
                        .filter(|(_, mapping)| {
                            query.is_empty()
                                || mapping
                                    .local_path
                                    .to_string_lossy()
                                    .to_lowercase()
                                    .contains(&query)
                                || mapping.remote_path.to_lowercase().contains(&query)
                        })
                        .map(|(index, _)| index)
                        .collect();

                    let mut groups: BTreeMap<PathBuf, Vec<usize>> = BTreeMap::new();
                    for index in &visible {
                        let mapping = &self.config.mappings[*index];
                        let parent = mapping
                            .local_path
                            .parent()
                            .unwrap_or(&mapping.local_path)
                            .to_path_buf();
                        groups.entry(parent).or_default().push(*index);
                    }
                    let mut rows = Vec::new();
                    for (parent, mappings) in groups {
                        rows.push(FolderListRow::Group(parent.clone(), mappings.len()));
                        if !query.is_empty() || !self.collapsed_folder_groups.contains(&parent) {
                            rows.extend(mappings.into_iter().map(FolderListRow::Mapping));
                        }
                    }

                    if self.config.mappings.is_empty() {
                        empty_state(
                            ui,
                            "No folders configured",
                            "Add one folder or import the subfolders of a directory as a group.",
                        );
                    } else if visible.is_empty() {
                        empty_state(ui, "No matching folders", "Try a shorter path or clear the filter.");
                    } else {
                        egui::ScrollArea::vertical()
                            .id_salt("folder_list")
                            .max_height(340.0)
                            .auto_shrink([false, false])
                            .show_rows(ui, 46.0, rows.len(), |ui, visible_rows| {
                                for row in visible_rows {
                                    let FolderListRow::Mapping(index) = &rows[row] else {
                                        let FolderListRow::Group(parent, count) = &rows[row] else {
                                            unreachable!();
                                        };
                                        let collapsed =
                                            self.collapsed_folder_groups.contains(parent);
                                        let marker = if collapsed { "+" } else { "-" };
                                        let response = egui::Frame::new()
                                            .fill(egui::Color32::from_rgb(24, 34, 43))
                                            .corner_radius(5.0)
                                            .inner_margin(egui::Margin::symmetric(10, 6))
                                            .show(ui, |ui| {
                                                ui.horizontal(|ui| {
                                                    ui.label(
                                                        egui::RichText::new(marker)
                                                            .monospace()
                                                            .color(egui::Color32::from_rgb(
                                                                84, 193, 173,
                                                            )),
                                                    );
                                                    ui.label(
                                                        egui::RichText::new(
                                                            parent.display().to_string(),
                                                        )
                                                        .strong(),
                                                    );
                                                    ui.with_layout(
                                                        egui::Layout::right_to_left(
                                                            egui::Align::Center,
                                                        ),
                                                        |ui| {
                                                            ui.label(
                                                                egui::RichText::new(format!(
                                                                    "{count} folder{}",
                                                                    if *count == 1 { "" } else { "s" }
                                                                ))
                                                                .color(egui::Color32::from_rgb(
                                                                    145, 157, 168,
                                                                )),
                                                            );
                                                        },
                                                    );
                                                });
                                            });
                                        if response.response.interact(egui::Sense::click()).clicked()
                                        {
                                            if collapsed {
                                                self.collapsed_folder_groups.remove(parent);
                                            } else {
                                                self.collapsed_folder_groups.insert(parent.clone());
                                            }
                                        }
                                        continue;
                                    };
                                    let mapping = &mut self.config.mappings[*index];
                                    let selected = self.expanded_mapping == Some(mapping.id);
                                    let name = mapping
                                        .local_path
                                        .file_name()
                                        .map(|name| name.to_string_lossy().into_owned())
                                        .unwrap_or_else(|| mapping.local_path.display().to_string());
                                    egui::Frame::new()
                                        .fill(if selected {
                                            egui::Color32::from_rgb(31, 52, 56)
                                        } else {
                                            egui::Color32::TRANSPARENT
                                        })
                                        .corner_radius(5.0)
                                        .inner_margin(egui::Margin::symmetric(7, 5))
                                        .show(ui, |ui| {
                                            ui.horizontal(|ui| {
                                                ui.add_space(18.0);
                                                ui.checkbox(&mut mapping.enabled, "")
                                                    .on_hover_text("Include this folder in syncs");
                                                let label = ui.selectable_label(
                                                    selected,
                                                    egui::RichText::new(name).strong(),
                                                );
                                                if label.clicked() {
                                                    self.expanded_mapping = Some(mapping.id);
                                                }
                                                label.on_hover_text(
                                                    mapping.local_path.display().to_string(),
                                                );
                                                ui.with_layout(
                                                    egui::Layout::right_to_left(
                                                        egui::Align::Center,
                                                    ),
                                                    |ui| {
                                                        let remote = if mapping.remote_path.is_empty()
                                                        {
                                                            "Not set"
                                                        } else {
                                                            &mapping.remote_path
                                                        };
                                                        if ui
                                                            .selectable_label(selected, remote)
                                                            .clicked()
                                                        {
                                                            self.expanded_mapping = Some(mapping.id);
                                                        }
                                                    },
                                                );
                                            });
                                        });
                                }
                            });
                    }
                });

                if let Some(id) = self.expanded_mapping {
                    if let Some(mapping) = self.config.mappings.iter_mut().find(|m| m.id == id) {
                        ui.add_space(10.0);
                        section_frame().show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(section_title("Folder details"));
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui.small_button("Close").clicked() {
                                            self.expanded_mapping = None;
                                        }
                                    },
                                );
                            });
                            ui.label(field_label("LOCAL PATH"));
                            ui.add(
                                egui::Label::new(
                                    egui::RichText::new(mapping.local_path.display().to_string())
                                        .monospace()
                                        .color(egui::Color32::from_rgb(186, 199, 210)),
                                )
                                .wrap(),
                            );
                            ui.add_space(8.0);
                            ui.label(field_label("REMOTE PATH"));
                            ui.add_sized(
                                [ui.available_width(), 28.0],
                                egui::TextEdit::singleline(&mut mapping.remote_path)
                                    .hint_text("Documents/work"),
                            );
                            ui.add_space(8.0);
                            ui.label(field_label("IGNORE PATTERNS"));
                            ui.label(
                                egui::RichText::new(
                                    "One gitignore-style pattern per line. Applied in both directions.",
                                )
                                .small()
                                .color(egui::Color32::from_rgb(135, 148, 160)),
                            );
                            ui.add(
                                egui::TextEdit::multiline(&mut mapping.ignore_patterns)
                                    .desired_rows(3)
                                    .desired_width(f32::INFINITY)
                                    .hint_text("*.tmp\n.cache/\n**/target/"),
                            );
                            ui.add_space(8.0);
                            if ui
                                .add(
                                    egui::Button::new("Remove folder")
                                        .fill(egui::Color32::from_rgb(84, 38, 43)),
                                )
                                .clicked()
                            {
                                self.pending_remove = Some(mapping.id);
                            }
                        });
                    } else {
                        self.expanded_mapping = None;
                    }
                }

                self.render_messages(ui);
                ui.add_space(14.0);
            });
        });

        if let Some(mut selection) = self.folder_selection.take() {
            let mut open = true;
            let mut add = false;
            egui::Window::new("Add folder group")
                .open(&mut open)
                .collapsible(false)
                .resizable(true)
                .default_width(520.0)
                .show(egui_ctx, |ui| {
                    ui.label(field_label("SOURCE DIRECTORY"));
                    ui.monospace(selection.root.display().to_string());
                    ui.add_space(10.0);
                    ui.label(field_label("REMOTE PARENT"));
                    ui.add_sized(
                        [ui.available_width(), 28.0],
                        egui::TextEdit::singleline(&mut selection.remote_parent)
                            .hint_text("Optional, for example Backups"),
                    );
                    ui.label(
                        egui::RichText::new(
                            "Each selected subfolder keeps its name beneath this path.",
                        )
                        .small()
                        .color(egui::Color32::from_rgb(145, 157, 168)),
                    );
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [ui.available_width().max(250.0) - 190.0, 26.0],
                            egui::TextEdit::singleline(&mut selection.filter)
                                .hint_text("Filter subfolders"),
                        );
                        if ui.small_button("Select all").clicked() {
                            let query = selection.filter.to_lowercase();
                            for (path, selected) in &mut selection.folders {
                                if query.is_empty()
                                    || path.to_string_lossy().to_lowercase().contains(&query)
                                {
                                    *selected = true;
                                }
                            }
                        }
                        if ui.small_button("Clear").clicked() {
                            for (_, selected) in &mut selection.folders {
                                *selected = false;
                            }
                        }
                    });
                    let query = selection.filter.trim().to_lowercase();
                    egui::ScrollArea::vertical()
                        .max_height(320.0)
                        .show(ui, |ui| {
                            for (path, selected) in &mut selection.folders {
                                let name = path
                                    .file_name()
                                    .map(|name| name.to_string_lossy())
                                    .unwrap_or_default();
                                if query.is_empty() || name.to_lowercase().contains(&query) {
                                    ui.checkbox(selected, name);
                                }
                            }
                        });
                    ui.add_space(8.0);
                    let selected_count = selection
                        .folders
                        .iter()
                        .filter(|(_, selected)| *selected)
                        .count();
                    if ui
                        .add_enabled(
                            selected_count > 0,
                            egui::Button::new(format!(
                                "Add {selected_count} selected folder{}",
                                if selected_count == 1 { "" } else { "s" }
                            ))
                            .fill(egui::Color32::from_rgb(41, 139, 126)),
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

        if let Some(id) = self.pending_remove {
            let mut open = true;
            egui::Window::new("Remove folder?")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
                .show(egui_ctx, |ui| {
                    ui.label("This removes the sync mapping. Files on disk and Cloudreve are not deleted.");
                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui.button("Cancel").clicked() {
                            self.pending_remove = None;
                        }
                        if ui
                            .add(
                                egui::Button::new("Remove mapping")
                                    .fill(egui::Color32::from_rgb(126, 48, 56)),
                            )
                            .clicked()
                        {
                            self.config.mappings.retain(|mapping| mapping.id != id);
                            self.expanded_mapping = None;
                            self.pending_remove = None;
                        }
                    });
                });
            if !open {
                self.pending_remove = None;
            }
        }
    }

    fn redraw(&mut self) {
        let gl_window = match self.gl_window.take() {
            Some(w) => w,
            None => return,
        };
        let mut egui_glow = match self.egui_glow.take() {
            Some(e) => e,
            None => {
                self.gl_window = Some(gl_window);
                return;
            }
        };
        let gl = self.gl.clone();

        egui_glow.run(&gl_window.window, |ctx| {
            self.render_ui(ctx);
        });

        if let Some(gl) = &gl {
            unsafe {
                use glow::HasContext as _;
                gl.clear_color(0.1, 0.1, 0.1, 1.0);
                gl.clear(glow::COLOR_BUFFER_BIT);
            }
        }

        egui_glow.paint(&gl_window.window);
        gl_window.swap_buffers();

        self.gl_window = Some(gl_window);
        self.egui_glow = Some(egui_glow);
    }
}

// ── ApplicationHandler ──

impl winit::application::ApplicationHandler<AppUserEvent> for App {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        if self.gl_window.is_none() {
            self.create_window(event_loop);
        }
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: AppUserEvent) {
        match event {
            AppUserEvent::Tray(TrayEvent::Show) => {
                if let Some(win) = &self.gl_window {
                    win.window.set_visible(true);
                    win.window.focus_window();
                    win.window.request_redraw();
                } else {
                    self.create_window(event_loop);
                }
            }
            AppUserEvent::Tray(TrayEvent::Sync) => {
                self.save_settings();
                let _ = self.commands.send(BackendCommand::SyncNow);
            }
            AppUserEvent::Tray(TrayEvent::Quit) => {
                self.quitting = true;
                let _ = self.commands.send(BackendCommand::Shutdown);
                event_loop.exit();
            }
            AppUserEvent::Redraw(delay) => {
                self.repaint_delay = delay;
                if let Some(win) = &self.gl_window {
                    win.window.request_redraw();
                }
            }
            AppUserEvent::PickedAddFolder(path) => {
                if let Some(mapping) = self
                    .config
                    .mappings
                    .iter()
                    .find(|mapping| mapping.local_path == path)
                {
                    self.expanded_mapping = Some(mapping.id);
                } else {
                    let id = Uuid::new_v4();
                    self.config.mappings.push(SyncMapping {
                        id,
                        local_path: path,
                        remote_path: String::new(),
                        enabled: true,
                        ignore_patterns: String::new(),
                    });
                    self.expanded_mapping = Some(id);
                }
                if let Some(gl_window) = self.gl_window.as_ref() {
                    gl_window.window.request_redraw();
                }
            }
            AppUserEvent::PickedFolderGroup(root) => {
                self.finish_choose_folder_group(root);
                if let Some(gl_window) = self.gl_window.as_ref() {
                    gl_window.window.request_redraw();
                }
            }
        }
    }

    fn new_events(&mut self, event_loop: &ActiveEventLoop, _cause: winit::event::StartCause) {
        self.poll_backend_events();
        if self.gl_window.is_some() {
            let next = std::time::Instant::now()
                + std::cmp::min(self.repaint_delay, std::time::Duration::from_millis(500));
            event_loop.set_control_flow(ControlFlow::WaitUntil(next));
        } else {
            event_loop.set_control_flow(ControlFlow::Wait);
        }
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        _window_id: WindowId,
        event: WindowEvent,
    ) {
        if matches!(event, WindowEvent::CloseRequested | WindowEvent::Destroyed) {
            if self.quitting {
                event_loop.exit();
            } else {
                self.record_activity("Closed window to the system tray".into());
                self.destroy_window();
            }
            return;
        }

        if let WindowEvent::Resized(size) = &event {
            if let Some(gl_window) = self.gl_window.as_mut() {
                gl_window.resize(*size);
            }
        }

        if event == WindowEvent::RedrawRequested {
            self.redraw();
            return;
        }

        let Some(gl_window) = self.gl_window.as_mut() else {
            return;
        };
        let Some(egui_glow) = self.egui_glow.as_mut() else {
            return;
        };

        let response = egui_glow.on_window_event(&gl_window.window, &event);
        if response.repaint {
            gl_window.window.request_redraw();
        }
    }

    fn exiting(&mut self, _event_loop: &ActiveEventLoop) {
        if let Some(mut eg) = self.egui_glow.take() {
            eg.destroy();
        }
    }
}

// ── Tray ──

struct CloudreveTray {
    proxy: EventLoopProxy<AppUserEvent>,
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
        let _ = self.proxy.send_event(AppUserEvent::Tray(TrayEvent::Show));
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        use ksni::menu::StandardItem;
        vec![
            StandardItem {
                label: "Show Cloudreve Sync".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.proxy.send_event(AppUserEvent::Tray(TrayEvent::Show));
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Sync now".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.proxy.send_event(AppUserEvent::Tray(TrayEvent::Sync));
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Quit".into(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.proxy.send_event(AppUserEvent::Tray(TrayEvent::Quit));
                }),
                ..Default::default()
            }
            .into(),
        ]
    }
}

fn create_tray(
    proxy: EventLoopProxy<AppUserEvent>,
) -> anyhow::Result<ksni::blocking::Handle<CloudreveTray>> {
    let image = image::load_from_memory(include_bytes!("../logo-sync.png"))?.into_rgba8();
    let image = image::imageops::resize(&image, 64, 64, image::imageops::FilterType::Lanczos3);
    let (width, height) = image.dimensions();
    let rgba = image.into_raw();
    let argb = rgba
        .chunks_exact(4)
        .flat_map(|pixel| [pixel[3], pixel[0], pixel[1], pixel[2]])
        .collect();
    let tray = ksni::blocking::TrayMethods::spawn(CloudreveTray {
        proxy,
        icon: vec![ksni::Icon {
            width: width as i32,
            height: height as i32,
            data: argb,
        }],
    })?;
    Ok(tray)
}

// ── Helpers ──

fn configure_ui(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = egui::Color32::from_rgb(12, 18, 25);
    visuals.window_fill = egui::Color32::from_rgb(20, 28, 37);
    visuals.faint_bg_color = egui::Color32::from_rgb(25, 34, 44);
    visuals.extreme_bg_color = egui::Color32::from_rgb(9, 14, 20);
    visuals.selection.bg_fill = egui::Color32::from_rgb(38, 117, 108);
    visuals.selection.stroke.color = egui::Color32::from_rgb(192, 236, 228);
    visuals.widgets.inactive.bg_fill = egui::Color32::from_rgb(29, 39, 49);
    visuals.widgets.inactive.weak_bg_fill = egui::Color32::from_rgb(29, 39, 49);
    visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(39, 53, 64);
    visuals.widgets.active.bg_fill = egui::Color32::from_rgb(38, 117, 108);
    visuals.widgets.noninteractive.bg_stroke.color = egui::Color32::from_rgb(48, 60, 71);
    visuals.window_corner_radius = egui::CornerRadius::same(8);
    ctx.set_visuals(visuals);

    let mut style = (*ctx.style()).clone();
    style.spacing.item_spacing = egui::vec2(8.0, 7.0);
    style.spacing.button_padding = egui::vec2(11.0, 6.0);
    style.spacing.interact_size.y = 28.0;
    style.text_styles.insert(
        egui::TextStyle::Heading,
        egui::FontId::new(22.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Body,
        egui::FontId::new(14.0, egui::FontFamily::Proportional),
    );
    ctx.set_style(style);
}

fn section_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(20, 28, 37))
        .stroke(egui::Stroke::new(
            1.0_f32,
            egui::Color32::from_rgb(43, 55, 66),
        ))
        .corner_radius(8.0)
        .inner_margin(egui::Margin::same(14))
}

fn section_title(text: &str) -> egui::RichText {
    egui::RichText::new(text).size(17.0).strong()
}

fn field_label(text: &str) -> egui::RichText {
    egui::RichText::new(text)
        .size(10.0)
        .strong()
        .color(egui::Color32::from_rgb(130, 145, 157))
}

fn metric(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.label(egui::RichText::new(label).color(egui::Color32::from_rgb(145, 157, 168)));
    ui.label(egui::RichText::new(value).strong());
}

fn empty_state(ui: &mut egui::Ui, title: &str, description: &str) {
    ui.add_space(24.0);
    ui.vertical_centered(|ui| {
        ui.label(egui::RichText::new(title).size(16.0).strong());
        ui.label(egui::RichText::new(description).color(egui::Color32::from_rgb(145, 157, 168)));
    });
    ui.add_space(24.0);
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

fn pick_folder(title: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("zenity")
        .args(["--file-selection", "--directory", "--title", title])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

// ── Main ──

fn main() -> anyhow::Result<()> {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let runtime = tokio::runtime::Runtime::new()?;
    std::thread::spawn(move || {
        runtime.block_on(sync::run(commands_rx, events_tx));
    });

    let event_loop = EventLoop::<AppUserEvent>::with_user_event().build()?;
    let proxy = event_loop.create_proxy();

    let tray = create_tray(proxy.clone())?;
    let mut app = App::new(commands_tx, events_rx, proxy, tray);
    event_loop.run_app(&mut app)?;
    Ok(())
}
