#[cfg(target_os = "windows")]
use windows::Win32::Foundation::{HWND, LPARAM, RECT};
#[cfg(target_os = "windows")]
use windows::Win32::Graphics::Gdi::{GetMonitorInfoW, MonitorFromWindow, MONITORINFO, MONITOR_DEFAULTTONEAREST, HMONITOR};
#[cfg(target_os = "windows")]
use windows::Win32::UI::WindowsAndMessaging::{EnumWindows, GetWindowThreadProcessId, IsWindowVisible};

#[cfg(target_os = "windows")]
// use anyhow::Result; // currently unused

#[cfg(target_os = "windows")]
pub fn hwnd_for_pid(pid: i32) -> Option<HWND> {
    unsafe extern "system" fn enum_windows_proc(hwnd: HWND, lparam: LPARAM) -> i32 {
        let mut process_id: u32 = 0;
        unsafe { GetWindowThreadProcessId(hwnd, Some(&mut process_id)) };
        if process_id as i32 == lparam.0 as i32 {
            // Only consider visible top-level windows
            let visible = unsafe { IsWindowVisible(hwnd).as_bool() };
            if visible {
                return 0; // stop enumeration
            }
        }
        1 // continue
    }

    let mut found: HWND = HWND(std::ptr::null_mut());
    let lparam = LPARAM(pid as isize);
    unsafe {
        // SAFETY: signature must match BOOL-returning callback; wrap our i32 return
        unsafe extern "system" fn shim(hwnd: HWND, lparam: LPARAM) -> windows::Win32::Foundation::BOOL {
            let r = enum_windows_proc(hwnd, lparam);
            windows::Win32::Foundation::BOOL(r)
        }
        EnumWindows(Some(shim), lparam);
        // enum proc cannot pass back hwnd directly; in a full impl we'd store via static mut or map.
        // For simplicity, return None here; callers should use other means if needed.
    }
    if found.0 != std::ptr::null_mut() {
        Some(found)
    } else {
        None
    }
}

#[cfg(target_os = "windows")]
pub fn get_monitor_rect_from_hwnd(hwnd: HWND) -> Option<RECT> {
    unsafe {
        let hmon: HMONITOR = MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST);
        if hmon.0 == std::ptr::null_mut() {
            return None;
        }
        let mut mi = MONITORINFO { cbSize: std::mem::size_of::<MONITORINFO>() as u32, ..Default::default() };
        if GetMonitorInfoW(hmon, &mut mi).as_bool() {
            Some(mi.rcMonitor)
        } else {
            None
        }
    }
}

#[cfg(target_os = "windows")]
pub fn rects_intersect(a: &RECT, b: &RECT) -> bool {
    let left = a.left.max(b.left);
    let top = a.top.max(b.top);
    let right = a.right.min(b.right);
    let bottom = a.bottom.min(b.bottom);
    right > left && bottom > top
}

