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
use std::collections::{BTreeMap, VecDeque};
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
                width: 720.0,
                height: 560.0,
            })
            .with_min_inner_size(winit::dpi::LogicalSize {
                width: 560.0,
                height: 420.0,
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
    folders: Vec<(PathBuf, bool)>,
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
        egui_glow.egui_ctx.set_visuals(egui::Visuals::dark());

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

    fn render_ui(&mut self, egui_ctx: &egui::Context) {
        egui::TopBottomPanel::top("header").show(egui_ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Cloudreve Sync");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(&self.status);
                });
            });
        });
        egui::CentralPanel::default().show(egui_ctx, |ui| {
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
                        let proxy = self.proxy.clone();
                        std::thread::spawn(move || {
                            if let Some(path) = pick_folder("Select a folder to sync") {
                                let _ = proxy.send_event(AppUserEvent::PickedAddFolder(path));
                            }
                        });
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
                .show(egui_ctx, |ui| {
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
                if self.gl_window.is_some() {
                    let win = self.gl_window.as_ref().unwrap();
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
                if self.gl_window.is_some() {
                    self.gl_window.as_ref().unwrap().window.request_redraw();
                }
            }
            AppUserEvent::PickedAddFolder(path) => {
                self.config.mappings.push(SyncMapping {
                    id: Uuid::new_v4(),
                    local_path: path,
                    remote_path: String::new(),
                    enabled: true,
                    ignore_patterns: String::new(),
                });
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
