use crate::parallel_processor::ParallelProcessor;
use crate::VideoCapture;
use anyhow::Result;
use futures::future::join_all;
use screenpipe_core::pii_removal::remove_pii;
use screenpipe_core::Language;
use screenpipe_db::{DatabaseManager, Speaker};
use screenpipe_events::{poll_meetings_events, send_event};
use screenpipe_vision::core::WindowOcr;
#[cfg(target_os = "macos")]
use screenpipe_vision::get_live_accessibility_text;
use screenpipe_vision::OcrEngine;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn};

#[allow(clippy::too_many_arguments)]
pub async fn start_continuous_recording(
    db: Arc<DatabaseManager>,
    output_path: Arc<String>,
    fps: f64,
    video_chunk_duration: Duration,
    ocr_engine: Arc<OcrEngine>,
    monitor_ids: Vec<u32>,
    use_pii_removal: bool,
    vision_disabled: bool,
    vision_handle: &Handle,
    ignored_windows: &[String],
    include_windows: &[String],
    languages: Vec<Language>,
    capture_unfocused_windows: bool,
    realtime_vision: bool,
) -> Result<()> {
    info!("Starting video recording for monitors {:?}", monitor_ids);
    let video_tasks = if !vision_disabled {
        monitor_ids
            .iter()
            .map(|&monitor_id| {
                let db_manager_video = Arc::clone(&db);
                let output_path_video = Arc::clone(&output_path);
                let ocr_engine = Arc::clone(&ocr_engine);
                let ignored_windows_video = ignored_windows.to_vec();
                let include_windows_video = include_windows.to_vec();

                let languages = languages.clone();

                info!("Starting video recording for monitor {}", monitor_id);
                vision_handle.spawn(async move {
                    // Wrap in a loop with recovery logic
                    loop {
                        info!("Starting/restarting vision capture for monitor {}", monitor_id);
                        match record_video(
                            db_manager_video.clone(),
                            output_path_video.clone(),
                            fps,
                            ocr_engine.clone(),
                            monitor_id,
                            use_pii_removal,
                            &ignored_windows_video,
                            &include_windows_video,
                            video_chunk_duration,
                            languages.clone(),
                            capture_unfocused_windows,
                            realtime_vision,
                        )
                        .await
                        {
                            Ok(_) => {
                                warn!("record_video for monitor {} completed unexpectedly but without error", monitor_id);
                                // Short delay before restarting to prevent CPU spinning
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                            Err(e) => {
                                error!("record_video for monitor {} failed with error: {}", monitor_id, e);
                                // Short delay before restarting to prevent CPU spinning
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>()
    } else {
        vec![vision_handle.spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), anyhow::Error>(())
        })]
    };

    if !vision_disabled {
        vision_handle.spawn(async move {
            info!("Starting meeting events polling");
            match poll_meetings_events().await {
                Ok(_) => warn!("Meeting events polling completed unexpectedly"),
                Err(e) => error!("Meeting events polling failed: {}", e),
            }
        });
    }

    // Join all video tasks
    let video_results = join_all(video_tasks);

    // Handle any errors from the tasks
    for (i, result) in video_results.await.into_iter().enumerate() {
        if let Err(e) = result {
            if !e.is_cancelled() {
                error!("Video recording error for monitor {}: {:?}", i, e);
            }
        }
    }

    Ok(())
}

/// Start continuous recording with parallel processing pipeline
#[allow(clippy::too_many_arguments)]
pub async fn start_continuous_recording_parallel(
    db: Arc<DatabaseManager>,
    output_path: Arc<String>,
    fps: f64,
    video_chunk_duration: Duration,
    ocr_engine: Arc<OcrEngine>,
    monitor_ids: Vec<u32>,
    use_pii_removal: bool,
    vision_disabled: bool,
    vision_handle: &Handle,
    ignored_windows: &[String],
    include_windows: &[String],
    languages: Vec<Language>,
    capture_unfocused_windows: bool,
    realtime_vision: bool,
) -> Result<()> {
    info!(
        "Starting parallel video recording for monitors {:?}",
        monitor_ids
    );

    // Create parallel processor
    let parallel_processor = Arc::new(ParallelProcessor::new(
        db.clone(),
        Arc::new((*ocr_engine).clone().into()), // Convert vision::OcrEngine to db::OcrEngine
        use_pii_removal,
        languages.clone(),
        100, // Queue size
    ));

    let video_tasks = if !vision_disabled {
        monitor_ids
            .iter()
            .map(|&monitor_id| {
                let db_manager_video = Arc::clone(&db);
                let output_path_video = Arc::clone(&output_path);
                let ocr_engine = Arc::clone(&ocr_engine);
                let ignored_windows_video = ignored_windows.to_vec();
                let include_windows_video = include_windows.to_vec();
                let parallel_processor = Arc::clone(&parallel_processor);

                let languages = languages.clone();

                info!("Starting parallel video recording for monitor {}", monitor_id);
                vision_handle.spawn(async move {
                    // Wrap in a loop with recovery logic
                    loop {
                        info!("Starting/restarting parallel vision capture for monitor {}", monitor_id);
                        match record_video_parallel(
                            db_manager_video.clone(),
                            output_path_video.clone(),
                            fps,
                            ocr_engine.clone(),
                            monitor_id,
                            use_pii_removal,
                            &ignored_windows_video,
                            &include_windows_video,
                            video_chunk_duration,
                            languages.clone(),
                            capture_unfocused_windows,
                            realtime_vision,
                            parallel_processor.clone(),
                        )
                        .await
                        {
                            Ok(_) => {
                                warn!("record_video_parallel for monitor {} completed unexpectedly but without error", monitor_id);
                                // Short delay before restarting to prevent CPU spinning
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                            Err(e) => {
                                error!("record_video_parallel for monitor {} failed with error: {}", monitor_id, e);
                                // Short delay before restarting to prevent CPU spinning
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>()
    } else {
        vec![vision_handle.spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), anyhow::Error>(())
        })]
    };

    if !vision_disabled {
        vision_handle.spawn(async move {
            info!("Starting meeting events polling");
            match poll_meetings_events().await {
                Ok(_) => warn!("Meeting events polling completed unexpectedly"),
                Err(e) => error!("Meeting events polling failed: {}", e),
            }
        });
    }

    // Start parallel processor
    let parallel_processor_clone = Arc::clone(&parallel_processor);
    vision_handle.spawn(async move {
        if let Err(e) = parallel_processor_clone.process_frames().await {
            error!("Parallel processor failed: {}", e);
        }
    });

    // Join all video tasks
    let video_results = join_all(video_tasks);

    // Handle any errors from the tasks
    for (i, result) in video_results.await.into_iter().enumerate() {
        if let Err(e) = result {
            if !e.is_cancelled() {
                error!("Video recording error for monitor {}: {:?}", i, e);
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn record_video(
    db: Arc<DatabaseManager>,
    output_path: Arc<String>,
    fps: f64,
    ocr_engine: Arc<OcrEngine>,
    monitor_id: u32,
    use_pii_removal: bool,
    ignored_windows: &[String],
    include_windows: &[String],
    video_chunk_duration: Duration,
    languages: Vec<Language>,
    capture_unfocused_windows: bool,
    realtime_vision: bool,
) -> Result<()> {
    info!("record_video: Starting for monitor {}", monitor_id);
    let device_name = Arc::new(format!("monitor_{}", monitor_id));

    // Add heartbeat counter
    let mut heartbeat_counter: u64 = 0;
    let heartbeat_interval = 100; // Log every 100 iterations
    let db_health_check_interval = 1000; // Check DB health every 1000 iterations
    let start_time = std::time::Instant::now();

    // Add health check interval
    let health_check_interval = 500; // Check task health every 500 iterations

    // Define a simpler callback that just returns the file path
    let new_chunk_callback = {
        let db_clone = Arc::clone(&db);
        let device_name_clone = Arc::clone(&device_name);
        move |file_path: &str| {
            let file_path = file_path.to_string();
            let db = Arc::clone(&db_clone);
            let device_name = Arc::clone(&device_name_clone);

            info!("Video chunk callback triggered for: {}", file_path);

            // Just spawn the task directly
            tokio::spawn(async move {
                info!("Starting video chunk DB insertion for: {}", file_path);
                match db.insert_video_chunk(&file_path, &device_name).await {
                    Ok(chunk_id) => {
                        info!(
                            "Successfully inserted video chunk ID {} for file: {}",
                            chunk_id, file_path
                        );
                    }
                    Err(e) => {
                        error!("Failed to insert new video chunk '{}': {}", file_path, e);
                        error!("Error details: {:?}", e);
                    }
                }
            });
        }
    };

    info!("Creating VideoCapture for monitor {}", monitor_id);
    let video_capture = VideoCapture::new(
        &output_path,
        fps,
        video_chunk_duration,
        new_chunk_callback,
        Arc::clone(&ocr_engine),
        monitor_id,
        ignored_windows,
        include_windows,
        languages,
        capture_unfocused_windows,
    );

    info!(
        "Starting main video processing loop for monitor {}",
        monitor_id
    );
    let mut last_frame_time = std::time::Instant::now();
    let mut frames_processed = 0;

    // Keep count of consecutive errors to detect unhealthy state
    let mut consecutive_db_errors = 0;
    const MAX_CONSECUTIVE_DB_ERRORS: u32 = 100; // Threshold before reporting unhealthy state

    loop {
        // Increment and check heartbeat
        heartbeat_counter += 1;
        if heartbeat_counter % heartbeat_interval == 0 {
            let uptime = start_time.elapsed().as_secs();
            let frames_per_sec = if uptime > 0 {
                frames_processed as f64 / uptime as f64
            } else {
                0.0
            };
            info!(
                    "record_video: Heartbeat for monitor {} - iteration {}, uptime: {}s, frames processed: {}, frames/sec: {:.2}",
                    monitor_id, heartbeat_counter, uptime, frames_processed, frames_per_sec
                );
        }

        // Periodically check database health
        if heartbeat_counter % db_health_check_interval == 0 {
            debug!("Checking database health for monitor {}", monitor_id);
            // Just log that we're checking the DB health
            debug!("Database health check periodic reminder");
            // We'll rely on the actual DB operations during normal processing to detect issues
        }

        // In the try-catch block inside the loop, add health checks
        if heartbeat_counter % health_check_interval == 0 {
            debug!(
                "Checking VideoCapture task health for monitor {}",
                monitor_id
            );
            if !video_capture.check_health() {
                error!(
                    "One or more VideoCapture tasks have terminated for monitor {}",
                    monitor_id
                );
                // Instead of immediately failing, log the error and continue
                // This helps us diagnose which task is failing
            }
        }

        if let Some(frame) = video_capture.ocr_frame_queue.pop() {
            let time_since_last_frame = last_frame_time.elapsed();
            last_frame_time = std::time::Instant::now();
            frames_processed += 1;

            debug!(
                "record_video: Processing frame {} with {} window results ({}ms since last frame)",
                frames_processed,
                frame.window_ocr_results.len(),
                time_since_last_frame.as_millis()
            );

            if !frame.window_ocr_results.is_empty() {
                // Pick focused window's metadata if present, else first
                let best = frame
                    .window_ocr_results
                    .iter()
                    .find(|w| w.focused)
                    .unwrap_or(&frame.window_ocr_results[0]);

                // Convert capture Instant to an approximate wall-clock timestamp
                let frame_wallclock = {
                    let elapsed = frame.timestamp.elapsed();
                    let dur =
                        chrono::Duration::from_std(elapsed).unwrap_or(chrono::Duration::zero());
                    chrono::Utc::now() - dur
                };

                let insert_frame_start = std::time::Instant::now();
                let result = db
                    .insert_frame(
                        &device_name,
                        Some(frame_wallclock),
                        best.browser_url.as_deref(),
                        Some(best.app_name.to_lowercase().as_str()),
                        Some(best.window_name.to_lowercase().as_str()),
                        best.focused,
                    )
                    .await;

                let insert_duration = insert_frame_start.elapsed();
                if insert_duration.as_millis() > 100 {
                    warn!(
                        "Slow DB insert_frame operation: {}ms",
                        insert_duration.as_millis()
                    );
                }

                match result {
                    Ok(frame_id) => {
                        debug!(
                            "Successfully inserted frame {} in {}ms",
                            frame_id,
                            insert_duration.as_millis()
                        );
                        // derive wall-clock DateTime from capture Instant for UI upsert
                        let ts_wallclock = {
                            let elapsed = frame.timestamp.elapsed();
                            let dur = chrono::Duration::from_std(elapsed)
                                .unwrap_or(chrono::Duration::zero());
                            chrono::Utc::now() - dur
                        };

                        // Build OCR insert futures for all windows
                        let engine: Arc<screenpipe_db::OcrEngine> =
                            Arc::new((*ocr_engine).clone().into());
                        let mut ocr_inserts = Vec::new();
                        for window_result in &frame.window_ocr_results {
                            let db_for_insert = db.clone();
                            let text_json =
                                serde_json::to_string(&window_result.text_json).unwrap_or_default();
                            let text_owned;
                            let text = if use_pii_removal {
                                text_owned = remove_pii(&window_result.text);
                                &text_owned
                            } else {
                                &window_result.text
                            };

                            if realtime_vision {
                                let send_event_start = std::time::Instant::now();
                                match send_event(
                                    "ocr_result",
                                    WindowOcr {
                                        image: Some(frame.image.clone()),
                                        text: text.clone(),
                                        text_json: window_result.text_json.clone(),
                                        app_name: window_result.app_name.clone(),
                                        window_name: window_result.window_name.clone(),
                                        focused: window_result.focused,
                                        confidence: window_result.confidence,
                                        timestamp: frame.timestamp,
                                        browser_url: window_result.browser_url.clone(),
                                    },
                                ) {
                                    Ok(_) => {
                                        let event_duration = send_event_start.elapsed();
                                        if event_duration.as_millis() > 100 {
                                            warn!(
                                                "Slow event sending: {}ms",
                                                event_duration.as_millis()
                                            );
                                        }
                                    }
                                    Err(e) => error!("Failed to send OCR event: {}", e),
                                }
                            }

                            let ui_text = text.clone();
                            let text_json_cloned = text_json.clone();
                            let engine_cloned = engine.clone();
                            let winn = window_result.window_name.clone();
                            ocr_inserts.push(async move {
                                let insert_ocr_start = std::time::Instant::now();
                                let res = db_for_insert
                                    .insert_ocr_text(
                                        frame_id,
                                        &ui_text,
                                        &text_json_cloned,
                                        engine_cloned,
                                    )
                                    .await;
                                if let Err(e) = res {
                                    error!(
                                        "Failed to insert OCR text: {}, skipping window {} of frame {}",
                                        e, winn, frame_id
                                    );
                                } else {
                                    let ocr_insert_duration = insert_ocr_start.elapsed();
                                    if ocr_insert_duration.as_millis() > 100 {
                                        warn!(
                                            "Slow DB insert_ocr_text operation: {}ms",
                                            ocr_insert_duration.as_millis()
                                        );
                                    }
                                    debug!(
                                        "OCR text inserted for frame {} in {}ms",
                                        frame_id,
                                        ocr_insert_duration.as_millis()
                                    );
                                }
                            });
                        }

                        // Single-shot live AX capture and single UI upsert per frame using the focused/best window
                        let appn = best.app_name.to_lowercase();
                        let winn = best.window_name.to_lowercase();
                        let best_text_owned = if use_pii_removal {
                            remove_pii(&best.text)
                        } else {
                            best.text.clone()
                        };

                        let insert_ui = async {
                            // skip if ui already exists for this frame to avoid duplicate snapshots
                            if db
                                .ui_monitoring_exists_for_frame(frame_id)
                                .await
                                .unwrap_or(false)
                            {
                                info!("ui_upsert_skip: frame_id={} already has ui row", frame_id);
                                return;
                            }
                            #[cfg(any(target_os = "macos", target_os = "windows"))]
                            let live_ax = get_live_accessibility_text();
                            #[cfg(not(any(target_os = "macos", target_os = "windows")))]
                            let live_ax: Option<(
                                String,
                                String,
                                String,
                            )> = None;

                            // TEMP: remove
                            let live_summary = live_ax
                                .as_ref()
                                .map(|(a, w, t)| (a.as_str(), w.as_str(), t.len()));
                            info!(
                                "live_ax snapshot: frame_id={}, live={:?}",
                                frame_id, live_summary
                            );

                            let ax_text = if let Some((live_app, live_win, live_text)) = live_ax {
                                if !live_text.is_empty() && live_app == appn && live_win == winn {
                                    live_text
                                } else {
                                    db.get_latest_ui_text_for_app_window(&appn, &winn)
                                        .await
                                        .ok()
                                        .flatten()
                                        .unwrap_or_else(|| best_text_owned.clone())
                                }
                            } else {
                                db.get_latest_ui_text_for_app_window(&appn, &winn)
                                    .await
                                    .ok()
                                    .flatten()
                                    .unwrap_or_else(|| best_text_owned.clone())
                            };

                            match db
                                .upsert_ui_monitoring(
                                    ts_wallclock,
                                    None,
                                    &appn,
                                    &winn,
                                    &ax_text,
                                    frame_id,
                                )
                                .await
                            {
                                // TEMP: remove
                                Ok(_) => {
                                    info!(
                                    "ui_upsert_ok: frame_id={}, app='{}', window='{}', text_len={}",
                                    frame_id, appn, winn, ax_text.len()
                                )
                                }
                                // TEMP: remove
                                Err(e) => info!("ui_upsert_err: frame_id={}, err={}", frame_id, e),
                            }
                        };

                        // Run OCR inserts and UI upsert concurrently
                        let _ = tokio::join!(async { join_all(ocr_inserts).await }, insert_ui);
                    }
                    Err(e) => {
                        warn!("Failed to insert frame: {}", e);
                        consecutive_db_errors += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        // skip this capture iteration
                    }
                }
            } else {
                // No window OCR results, but we still want one UI row per saved frame
                // 1) Insert the frame with minimal metadata
                let frame_wallclock = {
                    let elapsed = frame.timestamp.elapsed();
                    let dur =
                        chrono::Duration::from_std(elapsed).unwrap_or(chrono::Duration::zero());
                    chrono::Utc::now() - dur
                };

                let result = db
                    .insert_frame(&device_name, Some(frame_wallclock), None, None, None, false)
                    .await;

                if let Ok(frame_id) = result {
                    if frame_id > 0 {
                        // 2) Live AX snapshot and single UI upsert for this frame
                        // skip if ui already exists for this frame to avoid duplicate snapshots
                        if db
                            .ui_monitoring_exists_for_frame(frame_id)
                            .await
                            .unwrap_or(false)
                        {
                            info!(
                                "ui_upsert_skip(no-ocr): frame_id={} already has ui row",
                                frame_id
                            );
                            continue;
                        }
                        let ts_wallclock = frame_wallclock;
                        #[cfg(any(target_os = "macos", target_os = "windows"))]
                        let live_ax = screenpipe_vision::get_live_accessibility_text();
                        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
                        let live_ax: Option<(String, String, String)> = None;

                        let (appn, winn, ax_text) = match live_ax {
                            Some((app, win, text)) => {
                                (app.to_lowercase(), win.to_lowercase(), text)
                            }
                            None => (
                                "unknown app".to_string(),
                                "unknown window".to_string(),
                                String::new(),
                            ),
                        };

                        match db
                            .upsert_ui_monitoring(
                                ts_wallclock,
                                None,
                                &appn,
                                &winn,
                                &ax_text,
                                frame_id,
                            )
                            .await
                        {
                            // TEMP: remove
                            Ok(_) => info!(
                                "ui_upsert_ok(no-ocr): frame_id={}, app='{}', window='{}', text_len={}",
                                frame_id, appn, winn, ax_text.len()
                            ),
                            // TEMP: remove
                            Err(e) => info!(
                                "ui_upsert_err(no-ocr): frame_id={}, err={}",
                                frame_id, e
                            ),
                        }
                    }
                } else if let Err(e) = result {
                    debug!("insert_frame failed for no-ocr frame: {}", e);
                }
            }
        } else {
            // Log when frame queue is empty
            if heartbeat_counter % 10 == 0 {
                debug!(
                    "record_video: No frames in queue for monitor {}",
                    monitor_id
                );
            }
        }

        // Check if we're seeing too many consecutive DB errors
        if consecutive_db_errors > MAX_CONSECUTIVE_DB_ERRORS {
            error!(
                "Excessive consecutive database errors ({}), vision processing may be impaired",
                consecutive_db_errors
            );
            // Instead of failing, we'll continue but log the issue clearly
            consecutive_db_errors = 0; // Reset to prevent continuous error logging
        }

        // Sleep for the frame interval
        tokio::time::sleep(Duration::from_secs_f64(1.0 / fps)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_video_parallel(
    db: Arc<DatabaseManager>,
    output_path: Arc<String>,
    fps: f64,
    ocr_engine: Arc<OcrEngine>,
    monitor_id: u32,
    use_pii_removal: bool,
    ignored_windows: &[String],
    include_windows: &[String],
    video_chunk_duration: Duration,
    languages: Vec<Language>,
    capture_unfocused_windows: bool,
    realtime_vision: bool,
    parallel_processor: Arc<ParallelProcessor>,
) -> Result<()> {
    info!("record_video_parallel: Starting for monitor {}", monitor_id);
    let device_name = Arc::new(format!("monitor_{}", monitor_id));

    // Define a simpler callback that just returns the file path
    let new_chunk_callback = {
        let db_clone = Arc::clone(&db);
        let device_name_clone = Arc::clone(&device_name);
        move |file_path: &str| {
            let file_path = file_path.to_string();
            let db = Arc::clone(&db_clone);
            let device_name = Arc::clone(&device_name_clone);

            info!("Video chunk callback triggered for: {}", file_path);

            // Just spawn the task directly
            tokio::spawn(async move {
                info!("Starting video chunk DB insertion for: {}", file_path);
                match db.insert_video_chunk(&file_path, &device_name).await {
                    Ok(chunk_id) => {
                        info!(
                            "Successfully inserted video chunk ID {} for file: {}",
                            chunk_id, file_path
                        );
                    }
                    Err(e) => {
                        error!("Failed to insert new video chunk '{}': {}", file_path, e);
                        error!("Error details: {:?}", e);
                    }
                }
            });
        }
    };

    // Create video capture
    let video_capture = VideoCapture::new(
        &output_path,
        fps,
        video_chunk_duration,
        new_chunk_callback,
        ocr_engine,
        monitor_id,
        ignored_windows,
        include_windows,
        languages,
        capture_unfocused_windows,
    );

    let mut last_frame_time = std::time::Instant::now();
    let mut frames_processed = 0u64;
    let mut consecutive_db_errors = 0u32;

    info!(
        "Starting parallel video recording loop for monitor {}",
        monitor_id
    );

    loop {
        // Check for frames from the video capture
        if let Some(frame) = video_capture.ocr_frame_queue.pop() {
            let time_since_last_frame = last_frame_time.elapsed();
            last_frame_time = std::time::Instant::now();
            frames_processed += 1;

            debug!(
                "record_video_parallel: Processing frame {} with {} window results ({}ms since last frame)",
                frames_processed,
                frame.window_ocr_results.len(),
                time_since_last_frame.as_millis()
            );

            // Add frame to parallel processor with device name
            if let Err(e) = parallel_processor.add_frame(frame, (*device_name).clone()) {
                warn!("Failed to add frame to parallel processor: {}", e);
            }

            // Log statistics every 100 frames
            if frames_processed % 100 == 0 {
                let (processed, dropped, queue_len) = parallel_processor.get_stats();
                info!(
                    "Parallel processor stats: processed={}, dropped={}, queue_len={}, monitor={}",
                    processed, dropped, queue_len, monitor_id
                );
            }
        } else {
            // No frames available, sleep briefly
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Handle consecutive DB errors
        if consecutive_db_errors > 10 {
            error!(
                "Too many consecutive DB errors ({}), monitor {} may have issues",
                consecutive_db_errors, monitor_id
            );
            consecutive_db_errors = 0; // Reset to prevent continuous error logging
        }

        // Do not sleep by fps here — capture pipeline already paces production
        // Sleeping here throttles consumption and can cause upstream drops
    }
}

pub async fn merge_speakers(
    db: &DatabaseManager,
    speaker_to_keep_id: i64,
    speaker_to_merge_id: i64,
) -> Result<Speaker, anyhow::Error> {
    // make sure both speakers exist
    let _ = db.get_speaker_by_id(speaker_to_keep_id).await?;
    let _ = db.get_speaker_by_id(speaker_to_merge_id).await?;

    // call merge method from db
    match db
        .merge_speakers(speaker_to_keep_id, speaker_to_merge_id)
        .await
    {
        Ok(speaker) => Ok(speaker),
        Err(e) => Err(anyhow::anyhow!("Failed to merge speakers: {}", e)),
    }
}
