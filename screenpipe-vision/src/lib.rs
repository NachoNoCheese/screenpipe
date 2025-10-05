#[cfg(target_os = "macos")]
pub mod apple;
pub mod core;
pub mod custom_ocr;
#[cfg(target_os = "windows")]
pub mod microsoft;
pub mod monitor;
#[cfg(target_os = "macos")]
pub mod run_ui_monitoring_macos;
pub mod tesseract;
pub mod utils;
#[cfg(target_os = "macos")]
pub use apple::perform_ocr_apple;
pub use core::{
    continuous_capture, process_ocr_task, CaptureResult, RealtimeVisionEvent, UIFrame, UiSnapshot,
};
// pub use types::CaptureResult;
pub use utils::OcrEngine;
pub mod capture_screenshot_by_window;
pub use custom_ocr::perform_ocr_custom;
#[cfg(target_os = "windows")]
pub use microsoft::perform_ocr_windows;
#[cfg(target_os = "macos")]
pub use run_ui_monitoring_macos::run_ui;
pub use tesseract::perform_ocr_tesseract;
pub mod browser_utils;

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub mod live_ax;

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub use live_ax::get_live_accessibility_text;
