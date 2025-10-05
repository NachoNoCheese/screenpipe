// Use the Swift binary for traversal; no direct AX bindings here.

#[cfg(target_os = "macos")]
pub fn get_live_accessibility_text() -> Option<(String, String, String)> {
    use std::process::Command;
    use tracing::{info, warn};
    // Invoke the Swift UI monitor in one-shot mode and parse its JSON output
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let bin = format!("{}/bin/ui_monitor", crate_dir);
    // Allow overriding timeout via env; default 1500ms for stability
    let timeout_ms: u64 = std::env::var("SCREENPIPE_AX_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500);
    let output = match Command::new(&bin)
        .args(["--snapshot", "--timeout-ms", &timeout_ms.to_string()])
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            warn!(?err, binary=%bin, "failed to launch ui_monitor snapshot");
            return None;
        }
    };
    if !output.status.success() {
        let code = output.status.code();
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(%bin, ?code, %stderr, "ui_monitor snapshot returned non-zero status");
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Some builds print banner lines before JSON. Extract the JSON object span.
    let json_slice = match (stdout.find('{'), stdout.rfind('}')) {
        (Some(start), Some(end)) if end >= start => &stdout[start..=end],
        _ => stdout.trim(),
    };
    let v: serde_json::Value = match serde_json::from_str(json_slice) {
        Ok(v) => v,
        Err(err) => {
            warn!(?err, %json_slice, "failed to parse ui_monitor snapshot JSON");
            return None;
        }
    };
    let app = v.get("app")?.as_str()?.to_string();
    let window = v.get("window")?.as_str()?.to_string();
    let text = v
        .get("text_output")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    info!(app=%app, window=%window, text_len=text.len(), "ui_monitor snapshot captured");
    Some((app, window, text))
}

#[cfg(target_os = "windows")]
pub fn get_live_accessibility_text() -> Option<(String, String, String)> {
    use uiautomation::types::{TreeScope, UIProperty};
    use uiautomation::{controls::ControlType, UIAutomation};

    let automation = UIAutomation::new().ok()?;
    let focused = automation.get_focused_element().ok()?;

    // app: process name; window: top-level element name if available
    let pid = focused
        .get_property_value(UIProperty::ProcessId)
        .ok()?
        .get_i32()
        .ok()?;

    // Walk up to top-level window
    let mut top = focused.clone();
    for _ in 0..8 {
        if let Ok(ctrl) = top.get_control_type() {
            if matches!(ctrl, ControlType::Window | ControlType::Pane) {
                break;
            }
        }
        if let Ok(parent) = top.get_parent() {
            top = parent;
        } else {
            break;
        }
    }

    let window_name = top
        .get_property_value(UIProperty::Name)
        .ok()
        .and_then(|v| v.get_string().ok())
        .unwrap_or_default()
        .to_lowercase();

    // Collect visible text breadth-first with limits
    let mut queue = vec![(top, 0usize)];
    let mut seen = 0usize;
    let mut lines: Vec<String> = Vec::new();
    const MAX_DEPTH: usize = 8; // Reduced from 50 to capture only surface-level content
    const MAX_NODES: usize = 1000; // Reduced from 5000 to limit traversal
    const MAX_CHARS: usize = 50_000; // Reduced from 200k to focus on essential content

    while let Some((el, depth)) = queue.pop() {
        if depth > MAX_DEPTH
            || seen > MAX_NODES
            || lines.iter().map(|s| s.len()).sum::<usize>() > MAX_CHARS
        {
            break;
        }
        seen += 1;

        if let Ok(name) = el.get_property_value(UIProperty::Name) {
            if let Ok(name_text) = name.get_string() {
                let t = name_text.trim();
                if !t.is_empty() && t != "0" && t != "3" {
                    let indent = " ".repeat(depth.min(8));
                    lines.push(format!("{}[{}]", indent, t));
                }
            }
        }
        if let Ok(val) = el.get_property_value(UIProperty::ValueValue) {
            if let Ok(val_text) = val.get_string() {
                let t = val_text.trim();
                if !t.is_empty() {
                    let indent = " ".repeat(depth.min(8));
                    lines.push(format!("{}[{}]", indent, t));
                }
            }
        }

        if let Ok(children) = el.get_children() {
            for child in children.into_iter().rev() {
                queue.push((child, depth + 1));
            }
        }
    }

    // Resolve process name → app lowercased
    let app = screenpipe_core::operator::platforms::windows::get_process_name(pid as u32)
        .unwrap_or_else(|| "unknown app".to_string())
        .to_lowercase();

    let text = lines.join("\n");
    Some((app, window_name, text))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub fn get_live_accessibility_text() -> Option<(String, String, String)> {
    None
}
