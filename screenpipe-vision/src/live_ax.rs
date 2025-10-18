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
    use std::collections::{HashMap, HashSet, VecDeque};
    use tracing::{debug, info, warn};
    use uiautomation::types::{TreeScope, UIProperty};
    use uiautomation::variants::Value;
    use uiautomation::{controls::ControlType, UIAutomation};

    debug!("win uia capture: starting accessibility snapshot attempt");

    let automation = match UIAutomation::new() {
        Ok(a) => a,
        Err(e) => {
            warn!(error=%e, "uia init failed");
            return None;
        }
    };
    let focused = match automation.get_focused_element() {
        Ok(f) => f,
        Err(e) => {
            warn!(error=%e, "failed to get focused element");
            return None;
        }
    };

    let focused_name = focused
        .get_property_value(UIProperty::Name)
        .ok()
        .and_then(|v| v.get_string().ok())
        .unwrap_or_default();
    let focused_ctrl = focused.get_control_type().ok();
    debug!(
        "focused element info: name=\"{}\" ctrl={:?}",
        focused_name, focused_ctrl
    );

    let pid_variant = match focused.get_property_value(UIProperty::ProcessId) {
        Ok(v) => v,
        Err(e) => {
            warn!(error=%e, "win uia capture: focused element missing process id");
            return None;
        }
    };
    let pid = match pid_variant.get_value() {
        Ok(Value::I4(v)) | Ok(Value::INT(v)) => v,
        Ok(Value::UI4(v)) | Ok(Value::UINT(v)) => v as i32,
        Ok(Value::I8(v)) => v as i32,
        Ok(Value::UI8(v)) => v as i32,
        Ok(other) => {
            let desc = other.to_string();
            warn!(value=%desc, "win uia capture: unexpected process id variant");
            return None;
        }
        Err(e) => {
            warn!(error=%e, "win uia capture: unable to read process id variant");
            return None;
        }
    };

    let control_walker = automation.get_control_view_walker().ok();
    let mut highest_any = focused.clone();
    let mut highest_window: Option<uiautomation::UIElement> = None;
    let mut highest_pane: Option<uiautomation::UIElement> = None;

    if let Some(walker) = control_walker.as_ref() {
        let mut current = focused.clone();
        for _ in 0..64 {
            if let Ok(ctrl) = current.get_control_type() {
                match ctrl {
                    ControlType::Window => highest_window = Some(current.clone()),
                    ControlType::Pane => highest_pane = Some(current.clone()),
                    _ => {}
                }
            }
            highest_any = current.clone();
            match walker.get_parent(&current) {
                Ok(parent) => current = parent,
                Err(_) => break,
            }
        }
    } else {
        let mut current = focused.clone();
        for _ in 0..64 {
            if let Ok(ctrl) = current.get_control_type() {
                match ctrl {
                    ControlType::Window => highest_window = Some(current.clone()),
                    ControlType::Pane => highest_pane = Some(current.clone()),
                    _ => {}
                }
            }
            highest_any = current.clone();
            if let Ok(parent) = current.get_cached_parent() {
                current = parent;
            } else {
                break;
            }
        }
    }

    let had_window = highest_window.is_some();
    let had_pane = highest_pane.is_some();
    let walker_available = control_walker.is_some();

    let root_element = highest_window
        .or_else(|| highest_pane.clone())
        .unwrap_or_else(|| highest_any.clone());

    debug!(
        walker_available=?walker_available,
        has_window=had_window,
        has_pane=had_pane,
        "win uia capture: resolved traversal root candidate"
    );

    let root_ctrl = root_element.get_control_type().ok();
    let root_role = match root_ctrl {
        Some(ControlType::Window) => "Window",
        Some(ControlType::Pane) => "Pane",
        Some(other) => match other {
            ControlType::Document => "Document",
            ControlType::Edit => "Edit",
            _ => "Element",
        },
        None => "Element",
    };

    let root_name = root_element
        .get_property_value(UIProperty::Name)
        .ok()
        .and_then(|v| v.get_string().ok())
        .unwrap_or_default();
    let root_name_lower = root_name.to_lowercase();

    info!(
        "win uia root selected: role={} name=\"{}\"",
        root_role, root_name
    );

    fn control_type_label(ctrl: ControlType) -> Option<&'static str> {
        match ctrl {
            ControlType::Edit => Some("text field"),
            ControlType::Button => Some("button"),
            ControlType::Hyperlink => Some("link"),
            ControlType::Tree => Some("outline"),
            ControlType::TreeItem => Some("outline row"),
            ControlType::SplitButton | ControlType::Separator => None,
            ControlType::ScrollBar => Some("scroll bar"),
            ControlType::Pane | ControlType::Window => None,
            ControlType::Table | ControlType::DataGrid => Some("table"),
            ControlType::List => Some("list"),
            ControlType::ListItem => Some("list item"),
            ControlType::Document => Some("editor"),
            ControlType::Menu | ControlType::MenuItem => Some("menu"),
            ControlType::TitleBar => Some("title bar"),
            ControlType::Text => Some("text"),
            ControlType::Tab => Some("tab"),
            ControlType::TabItem => Some("tab"),
            _ => None,
        }
    }

    fn is_priority_container(ctrl: ControlType) -> bool {
        matches!(
            ctrl,
            ControlType::Document
                | ControlType::Edit
                | ControlType::Text
                | ControlType::List
                | ControlType::ListItem
                | ControlType::Tree
                | ControlType::TreeItem
                | ControlType::Table
                | ControlType::DataGrid
        )
    }

    fn fallback_visit_key(element: &uiautomation::UIElement) -> Option<String> {
        let name = element
            .get_property_value(UIProperty::Name)
            .ok()
            .and_then(|v| v.get_string().ok())
            .unwrap_or_default();
        let ctrl = element.get_control_type().ok();
        if name.is_empty() && ctrl.is_none() {
            None
        } else {
            Some(format!("{}::{:?}", name, ctrl))
        }
    }

    fn push_line(
        lines: &mut Vec<String>,
        total_chars: &mut usize,
        max_chars: usize,
        line: String,
    ) -> bool {
        let new_total = *total_chars + line.len();
        if new_total > max_chars {
            false
        } else {
            *total_chars = new_total;
            lines.push(line);
            true
        }
    }

    fn process_queue(
        queue: &mut VecDeque<(uiautomation::UIElement, usize)>,
        visited_runtime_ids: &mut HashSet<Vec<i32>>,
        visited_fallback: &mut HashSet<String>,
        depth_map: &mut HashMap<Vec<i32>, usize>,
        lines: &mut Vec<String>,
        total_chars: &mut usize,
        visited_nodes: &mut usize,
        max_depth: usize,
        max_nodes: usize,
        max_chars: usize,
        true_condition: Option<&uiautomation::core::UICondition>,
    ) -> bool {
        while let Some((element, depth)) = queue.pop_front() {
            if depth > max_depth || *visited_nodes >= max_nodes || *total_chars >= max_chars {
                return true;
            }

            let runtime_id = element.get_runtime_id().ok();
            if let Some(ref id) = runtime_id {
                if !visited_runtime_ids.insert(id.clone()) {
                    continue;
                }
                depth_map.insert(id.clone(), depth);
            } else if let Some(key) = fallback_visit_key(&element) {
                if !visited_fallback.insert(key) {
                    continue;
                }
            }

            *visited_nodes += 1;
            let ctrl_type = element.get_control_type().ok();
            let indent = " ".repeat(depth.min(16));
            let mut pushed_any = false;

            if let Ok(name_var) = element.get_property_value(UIProperty::Name) {
                if let Ok(name_text) = name_var.get_string() {
                    let t = name_text.trim();
                    if !t.is_empty() && t != "0" && t != "3" {
                        let line = if let Some(ctrl) = ctrl_type {
                            if let Some(label) = control_type_label(ctrl) {
                                format!("{}[{}] [{}]", indent, t, label)
                            } else {
                                format!("{}[{}]", indent, t)
                            }
                        } else {
                            format!("{}[{}]", indent, t)
                        };
                        if !push_line(lines, total_chars, max_chars, line) {
                            return true;
                        }
                        pushed_any = true;
                    }
                }
            }

            if let Ok(val_var) = element.get_property_value(UIProperty::ValueValue) {
                if let Ok(val_text) = val_var.get_string() {
                    let t = val_text.trim();
                    if !t.is_empty() {
                        let line = format!("{}[{}]", indent, t);
                        if !push_line(lines, total_chars, max_chars, line) {
                            return true;
                        }
                        pushed_any = true;
                    }
                }
            }

            if let Ok(help_var) = element.get_property_value(UIProperty::HelpText) {
                if let Ok(help_text) = help_var.get_string() {
                    let t = help_text.trim();
                    if !t.is_empty() {
                        let line = format!("{}[{}]", indent, t);
                        if !push_line(lines, total_chars, max_chars, line) {
                            return true;
                        }
                        pushed_any = true;
                    }
                }
            }

            if let Ok(full_var) = element.get_property_value(UIProperty::FullDescription) {
                if let Ok(full_text) = full_var.get_string() {
                    let t = full_text.trim();
                    if !t.is_empty() {
                        let line = format!("{}[{}]", indent, t);
                        if !push_line(lines, total_chars, max_chars, line) {
                            return true;
                        }
                        pushed_any = true;
                    }
                }
            }

            if let Some(ctrl) = ctrl_type {
                if matches!(ctrl, ControlType::Document | ControlType::Edit) {
                    match element.get_pattern::<uiautomation::patterns::UITextPattern>() {
                        Ok(text_pat) => match text_pat.get_document_range() {
                            Ok(range) => match range.get_text(-1) {
                                Ok(tp_text) => {
                                    let t = tp_text.trim();
                                    debug!("textpattern found: length={}", t.len());
                                    if !t.is_empty() {
                                        let line = format!("{}[{}]", indent, t);
                                        if !push_line(lines, total_chars, max_chars, line) {
                                            return true;
                                        }
                                        pushed_any = true;
                                    }
                                }
                                Err(e) => debug!(error=%e, "textpattern get_text failed"),
                            },
                            Err(e) => debug!(error=%e, "textpattern get_document_range failed"),
                        },
                        Err(e) => debug!(error=%e, "textpattern not available"),
                    }
                }
            }

            if !pushed_any {
                if let Some(ctrl) = ctrl_type {
                    if let Some(label) = control_type_label(ctrl) {
                        let line = format!("{}[{}]", indent, label);
                        if !push_line(lines, total_chars, max_chars, line) {
                            return true;
                        }
                    }
                }
            }

            if depth >= max_depth {
                continue;
            }

            let mut children: Vec<uiautomation::UIElement> = Vec::new();
            if let Some(cond) = true_condition {
                match element.find_all(TreeScope::Children, cond) {
                    Ok(found) => {
                        debug!(
                            "find_all children: depth={} count={}",
                            depth,
                            found.len()
                        );
                        children = found;
                    }
                    Err(e) => debug!(depth, error=%e, "find_all children failed"),
                }
            }

            if children.is_empty() {
                match element.get_cached_children() {
                    Ok(found) => {
                        debug!(
                            "cached children fallback: depth={} count={}",
                            depth,
                            found.len()
                        );
                        children = found;
                    }
                    Err(_) => debug!("no children for element at depth={}", depth),
                }
            }

            if children.is_empty() {
                continue;
            }

            let mut priority = Vec::new();
            let mut remaining = Vec::new();
            for child in children {
                match child.get_control_type() {
                    Ok(ctrl) if is_priority_container(ctrl) => {
                        priority.push((child, depth + 1))
                    }
                    _ => remaining.push((child, depth + 1)),
                }
            }

            for (child, child_depth) in priority.into_iter().chain(remaining.into_iter()) {
                queue.push_back((child, child_depth));
            }
        }

        false
    }

    const MAX_DEPTH: usize = 32;
    const MAX_NODES: usize = 5000;
    const MAX_CHARS: usize = 200_000;

    let mut queue: VecDeque<(uiautomation::UIElement, usize)> = VecDeque::new();
    queue.push_back((root_element.clone(), 0));
    let mut visited_runtime_ids: HashSet<Vec<i32>> = HashSet::new();
    let mut visited_fallback: HashSet<String> = HashSet::new();
    let mut depth_map: HashMap<Vec<i32>, usize> = HashMap::new();
    let mut lines: Vec<String> = Vec::new();
    let mut visited_nodes: usize = 0;
    let mut total_chars: usize = 0;

    let true_condition = match automation.create_true_condition() {
        Ok(cond) => Some(cond),
        Err(e) => {
            debug!(error=%e, "true condition unavailable; using cached traversal only");
            None
        }
    };

    debug!(
        "BFS pass start: root role={} name=\"{}\" priority containers first",
        root_role, root_name
    );
    let root_limit_hit = process_queue(
        &mut queue,
        &mut visited_runtime_ids,
        &mut visited_fallback,
        &mut depth_map,
        &mut lines,
        &mut total_chars,
        &mut visited_nodes,
        MAX_DEPTH,
        MAX_NODES,
        MAX_CHARS,
        true_condition.as_ref(),
    );

    if !root_limit_hit {
        let mut anchors: Vec<uiautomation::UIElement> = vec![focused.clone()];
        let mut current = focused.clone();

        if let Some(walker) = control_walker.as_ref() {
            for _ in 0..6 {
                match walker.get_parent(&current) {
                    Ok(parent) => {
                        anchors.push(parent.clone());
                        if automation
                            .compare_elements(&parent, &root_element)
                            .unwrap_or(false)
                        {
                            break;
                        }
                        current = parent;
                    }
                    Err(_) => break,
                }
            }
        } else {
            for _ in 0..6 {
                match current.get_cached_parent() {
                    Ok(parent) => {
                        anchors.push(parent.clone());
                        if automation
                            .compare_elements(&parent, &root_element)
                            .unwrap_or(false)
                        {
                            break;
                        }
                        current = parent;
                    }
                    Err(_) => break,
                }
            }
        }

        let anchor_count = anchors.len();
        for (idx, anchor) in anchors.into_iter().enumerate() {
            let runtime_id = anchor.get_runtime_id().ok();
            let already_seen = if let Some(ref id) = runtime_id {
                visited_runtime_ids.contains(id)
            } else if let Some(key) = fallback_visit_key(&anchor) {
                visited_fallback.contains(&key)
            } else {
                false
            };
            if already_seen {
                continue;
            }

            let base_depth = runtime_id
                .as_ref()
                .and_then(|id| depth_map.get(id).copied())
                .unwrap_or_else(|| anchor_count.saturating_sub(idx + 1));

            let anchor_name = anchor
                .get_property_value(UIProperty::Name)
                .ok()
                .and_then(|v| v.get_string().ok())
                .unwrap_or_default();
            debug!(
                "BFS pass start: focus anchor level={} name=\"{}\" priority containers first",
                idx, anchor_name
            );

            let mut anchor_queue: VecDeque<(uiautomation::UIElement, usize)> = VecDeque::new();
            anchor_queue.push_back((anchor.clone(), base_depth));
            if process_queue(
                &mut anchor_queue,
                &mut visited_runtime_ids,
                &mut visited_fallback,
                &mut depth_map,
                &mut lines,
                &mut total_chars,
                &mut visited_nodes,
                MAX_DEPTH,
                MAX_NODES,
                MAX_CHARS,
                true_condition.as_ref(),
            ) {
                break;
            }
        }
    }

    let mut app = screenpipe_core::operator::platforms::windows::get_process_name(pid as u32)
        .unwrap_or_else(|| "unknown app".to_string())
        .to_lowercase();

    if matches!(app.as_str(), "msedgewebview2" | "outlookwebviewhost" | "unknown app") {
        let inferred = if root_name_lower.contains("outlook") {
            Some("microsoft outlook")
        } else if root_name_lower.contains("word") {
            Some("microsoft word")
        } else if root_name_lower.contains("acrobat") || root_name_lower.contains("adobe") {
            Some("adobe acrobat")
        } else {
            None
        };

        if let Some(remap) = inferred {
            info!(
                original_app=%app,
                inferred_app=remap,
                "win uia capture: remapping embedded webview app"
            );
            app = remap.to_string();
        }
    }

    let window_return = root_name_lower;

    let text_output = lines.join("\n");
    if lines.is_empty() || text_output.trim().is_empty() {
        debug!(
            lines=lines.len(),
            chars=text_output.len(),
            root_role,
            root_name,
            "win uia traversal produced empty text"
        );
        info!(
            "ui snapshot unavailable for app={} window={}",
            app, window_return
        );
        return None;
    }

    info!(
        "windows uia snapshot: app='{}' window='{}' lines={} chars={}",
        app,
        window_return,
        lines.len(),
        text_output.len()
    );

    Some((app, window_return, text_output))
}
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub fn get_live_accessibility_text() -> Option<(String, String, String)> {
    None
}
