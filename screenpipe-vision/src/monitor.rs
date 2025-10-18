use anyhow::{Error, Result};
use image::DynamicImage;
use std::sync::Arc;
use tracing;
use xcap::Monitor;

#[derive(Clone)]
pub struct SafeMonitor {
    monitor_id: u32,
    monitor_data: Arc<MonitorData>,
}

#[derive(Clone)]
pub struct MonitorData {
    pub width: u32,
    pub height: u32,
    pub name: String,
    pub is_primary: bool,
}

impl SafeMonitor {
    pub fn new(monitor: Monitor) -> Self {
        let monitor_id = monitor.id().unwrap();
        let monitor_data = Arc::new(MonitorData {
            width: monitor.width().unwrap(),
            height: monitor.height().unwrap(),
            name: monitor.name().unwrap().to_string(),
            is_primary: monitor.is_primary().unwrap(),
        });

        Self {
            monitor_id,
            monitor_data,
        }
    }

    pub async fn capture_image(&self) -> Result<DynamicImage> {
        let monitor_id = self.monitor_id;

        let image = std::thread::spawn(move || -> Result<DynamicImage> {
            let monitor = Monitor::all()
                .map_err(Error::from)?
                .into_iter()
                .find(|m| m.id().unwrap() == monitor_id)
                .ok_or_else(|| anyhow::anyhow!("Monitor not found"))?;

            if monitor.width().unwrap() == 0 || monitor.height().unwrap() == 0 {
                return Err(anyhow::anyhow!("Invalid monitor dimensions"));
            }

            monitor
                .capture_image()
                .map_err(Error::from)
                .map(DynamicImage::ImageRgba8)
        })
        .join()
        .unwrap()?;

        Ok(image)
    }

    pub fn id(&self) -> u32 {
        self.monitor_id
    }

    pub fn dimensions(&self) -> (u32, u32) {
        (self.monitor_data.width, self.monitor_data.height)
    }

    pub fn name(&self) -> &str {
        &self.monitor_data.name
    }

    pub fn width(&self) -> u32 {
        self.monitor_data.width
    }

    pub fn height(&self) -> u32 {
        self.monitor_data.height
    }

    pub fn is_primary(&self) -> bool {
        self.monitor_data.is_primary
    }

    pub fn get_info(&self) -> MonitorData {
        (*self.monitor_data).clone()
    }
}

pub async fn list_monitors() -> Vec<SafeMonitor> {
    tokio::task::spawn_blocking(|| {
        Monitor::all()
            .unwrap()
            .into_iter()
            .map(SafeMonitor::new)
            .collect()
    })
    .await
    .unwrap()
}

pub async fn get_default_monitor() -> SafeMonitor {
    tokio::task::spawn_blocking(|| {
        SafeMonitor::new(Monitor::all().unwrap().first().unwrap().clone())
    })
    .await
    .unwrap()
}

pub async fn get_monitor_by_id(id: u32) -> Option<SafeMonitor> {
    tokio::task::spawn_blocking(move || match Monitor::all() {
        Ok(monitors) => {
            let monitor_count = monitors.len();
            let monitor_ids: Vec<u32> = monitors.iter().map(|m| m.id().unwrap()).collect();

            tracing::debug!(
                "Found {} monitors with IDs: {:?}",
                monitor_count,
                monitor_ids
            );

            monitors
                .into_iter()
                .find(|m| m.id().unwrap() == id)
                .map(SafeMonitor::new)
        }
        Err(e) => {
            tracing::error!("Failed to list monitors: {}", e);
            None
        }
    })
    .await
    .unwrap_or_else(|e| {
        tracing::error!("Task to get monitor by ID {} panicked: {}", id, e);
        None
    })
}

/// Resolve a preferred monitor id to capture.
/// Strategy (Windows): if a foreground window's monitor can be determined, map to the xcap monitor
/// that matches its dimensions, preferring primary if ambiguous. Otherwise, use the original id if
/// present, else fallback to primary, else the first available.
/// On non-Windows, resolve original id if present, else primary or first.
pub async fn resolve_preferred_monitor_id(original_id: u32) -> Option<u32> {
    tokio::task::spawn_blocking(move || {
        #[cfg(target_os = "windows")]
        {
            use tracing::{debug, info, warn};
            use windows::Win32::UI::WindowsAndMessaging::GetForegroundWindow;
            use crate::windows_monitor_utils::get_monitor_rect_from_hwnd;

            let monitors_all = match Monitor::all() {
                Ok(m) => m,
                Err(e) => {
                    warn!("failed to list monitors: {}", e);
                    return None;
                }
            };

            let mut monitors: Vec<(u32, u32, u32, bool)> = Vec::new();
            for m in &monitors_all {
                let id = m.id().unwrap_or(0);
                let w = m.width().unwrap_or(0);
                let h = m.height().unwrap_or(0);
                let primary = m.is_primary().unwrap_or(false);
                monitors.push((id, w, h, primary));
            }
            debug!("monitor set: {:?}", monitors);

            // If original id still exists, we can use it unless foreground mapping says otherwise
            let original_still_exists = monitors.iter().any(|(id, _, _, _)| *id == original_id);

            // Try to map to the foreground window's monitor
            let hwnd = unsafe { GetForegroundWindow() };
            if hwnd.0 != std::ptr::null_mut() {
                if let Some(rect) = get_monitor_rect_from_hwnd(hwnd) {
                    let fw = (rect.right - rect.left).max(0) as u32;
                    let fh = (rect.bottom - rect.top).max(0) as u32;
                    if fw > 0 && fh > 0 {
                        let mut candidates: Vec<(u32, bool)> = monitors
                            .iter()
                            .filter(|(_, w, h, _)| *w == fw && *h == fh)
                            .map(|(id, _, _, primary)| (*id, *primary))
                            .collect();
                        if !candidates.is_empty() {
                            // prefer primary among equal resolution monitors
                            candidates.sort_by_key(|&(_id, primary)| if primary { 0 } else { 1 });
                            let chosen = candidates[0].0;
                            if !original_still_exists || chosen != original_id {
                                info!(
                                    "foreground-driven monitor mapping: {}x{} -> id {} (orig {:?})",
                                    fw, fh, chosen, original_id
                                );
                            }
                            return Some(chosen);
                        }
                    }
                }
            }

            // Fallbacks
            if original_still_exists {
                return Some(original_id);
            }
            if let Some((id, _, _, _)) = monitors.iter().find(|(_, _, _, p)| *p).copied() {
                info!("mapping to primary monitor id {}", id);
                return Some(id);
            }
            monitors.first().map(|(id, _, _, _)| *id)
        }

        #[cfg(not(target_os = "windows"))]
        {
            use tracing::{info, warn};
            let monitors_all = match Monitor::all() {
                Ok(m) => m,
                Err(e) => {
                    warn!("failed to list monitors: {}", e);
                    return None;
                }
            };
            for m in &monitors_all {
                if m.id().unwrap_or(0) == original_id {
                    return Some(original_id);
                }
            }
            if let Some(primary) = monitors_all.iter().find(|m| m.is_primary().unwrap_or(false)) {
                let id = primary.id().unwrap_or(0);
                info!("mapping to primary monitor id {}", id);
                return Some(id);
            }
            monitors_all.first().and_then(|m| m.id()).ok()
        }
    })
    .await
    .unwrap_or(None)
}
