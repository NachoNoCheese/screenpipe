use anyhow::Result;
use crossbeam::queue::ArrayQueue;
use screenpipe_core::Language;
use screenpipe_db::{DatabaseManager, OcrEngine};
use screenpipe_vision::CaptureResult;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Frame processing pipeline with parallel workers
pub struct ParallelProcessor {
    // Input queue for raw frames
    frame_queue: Arc<ArrayQueue<FrameEnvelope>>,

    // Worker channels
    video_tx: mpsc::UnboundedSender<VideoTask>,
    ocr_tx: mpsc::UnboundedSender<OcrTask>,
    ui_tx: mpsc::UnboundedSender<UiTask>,
    db_tx: mpsc::UnboundedSender<DbTask>,

    // Worker handles
    video_worker: JoinHandle<()>,
    ocr_worker: JoinHandle<()>,
    ui_worker: JoinHandle<()>,
    db_worker: JoinHandle<()>,

    // Configuration
    use_pii_removal: bool,

    // Statistics
    frames_processed: Arc<std::sync::atomic::AtomicU64>,
    frames_dropped: Arc<std::sync::atomic::AtomicU64>,

    // Synchronous processing limit
    frames_in_progress: Arc<std::sync::atomic::AtomicU64>,
}
#[derive(Clone)]
struct FrameEnvelope {
    frame: Arc<CaptureResult>,
    device_name: String,
    enqueued_at: Instant,
}

/// Task types for different workers
#[derive(Clone)]
pub struct VideoTask {
    pub frame: Arc<CaptureResult>,
    pub frame_id: i64,
    pub device_name: String,
}

#[derive(Clone)]
pub struct OcrTask {
    pub frame: Arc<CaptureResult>,
    pub frame_id: i64,
    pub window_results: Vec<WindowOcrResult>,
    pub use_pii_removal: bool,
}

#[derive(Clone)]
pub struct UiTask {
    pub frame: Arc<CaptureResult>,
    pub frame_id: i64,
    pub app_name: String,
    pub window_name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub struct DbTask {
    pub frame_id: i64,
    pub device_name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub browser_url: Option<String>,
    pub app_name: Option<String>,
    pub window_name: Option<String>,
    pub focused: bool,
    pub response_tx: Option<oneshot::Sender<i64>>, // send back the actual DB frame id
}

#[derive(Debug, Clone)]
pub struct WindowOcrResult {
    pub text: String,
    pub text_json: String,
    pub app_name: String,
    pub window_name: String,
    pub focused: bool,
    pub confidence: f32,
    pub browser_url: Option<String>,
}

impl ParallelProcessor {
    pub fn new(
        db: Arc<DatabaseManager>,
        ocr_engine: Arc<OcrEngine>,
        use_pii_removal: bool,
        languages: Vec<Language>,
        queue_size: usize,
    ) -> Self {
        let frame_queue = Arc::new(ArrayQueue::new(queue_size));

        // Create channels for worker communication
        let (video_tx, video_rx) = mpsc::unbounded_channel();
        let (ocr_tx, ocr_rx) = mpsc::unbounded_channel();
        let (ui_tx, ui_rx) = mpsc::unbounded_channel();
        let (db_tx, db_rx) = mpsc::unbounded_channel();

        // Statistics
        let frames_processed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let frames_dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let frames_in_progress = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Spawn workers
        let video_worker = Self::spawn_video_worker(video_rx, db.clone());
        let ocr_worker =
            Self::spawn_ocr_worker(ocr_rx, db.clone(), ocr_engine, use_pii_removal, languages);
        let ui_worker = Self::spawn_ui_worker(ui_rx, db.clone());
        let db_worker = Self::spawn_db_worker(db_rx, db.clone());

        Self {
            frame_queue,
            video_tx,
            ocr_tx,
            ui_tx,
            db_tx,
            video_worker,
            ocr_worker,
            ui_worker,
            db_worker,
            use_pii_removal,
            frames_processed,
            frames_dropped,
            frames_in_progress,
        }
    }

    /// Add a frame to the processing pipeline
    pub fn add_frame(&self, frame: Arc<CaptureResult>, device_name: String) -> Result<()> {
        // Try to add to queue, drop oldest if full
        let envelope = FrameEnvelope {
            frame,
            device_name,
            enqueued_at: Instant::now(),
        };
        if self.frame_queue.push(envelope.clone()).is_err() {
            // Queue is full, drop oldest frame
            if let Some(_dropped) = self.frame_queue.pop() {
                self.frames_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!("Dropped oldest frame due to queue full");
            }
            // Try again
            if self.frame_queue.push(envelope).is_err() {
                warn!("Failed to add frame to queue even after dropping oldest");
                return Err(anyhow::anyhow!("Queue full and unable to add frame"));
            }
        }
        Ok(())
    }

    /// Process frames from the queue
    pub async fn process_frames(&self) -> Result<()> {
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(10);
        let mut last_processed_count: u64 = 0;
        let mut last_stats_tick = Instant::now();

        loop {
            if let Some(envelope) = self.frame_queue.pop() {
                // Check if we're at the synchronous limit (3 frames)
                let current_in_progress = self
                    .frames_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed);
                if current_in_progress >= 3 {
                    // At limit, wait for a frame to complete before processing more
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    // Put the frame back in the queue
                    if self.frame_queue.push(envelope).is_err() {
                        // Queue is full, drop this frame
                        self.frames_dropped
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        debug!("Dropped frame due to synchronous limit and queue full");
                    }
                    continue;
                }

                self.frames_processed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Process frame in parallel
                self.process_single_frame(envelope).await?;
            } else {
                // No frames available, sleep briefly
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Log statistics periodically
            if last_stats_time.elapsed() >= stats_interval {
                let processed = self
                    .frames_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let dropped = self
                    .frames_dropped
                    .load(std::sync::atomic::Ordering::Relaxed);
                let queue_len = self.frame_queue.len();
                let in_progress = self
                    .frames_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed);
                let elapsed_secs = last_stats_tick.elapsed().as_secs_f64().max(0.001);
                let processed_delta = processed.saturating_sub(last_processed_count);
                let effective_fps = (processed_delta as f64) / elapsed_secs;

                info!(
                    "parallel processor stats: processed={}, dropped={}, queue_len={}, in_progress={}, effective_fps_last_interval={:.2}",
                    processed, dropped, queue_len, in_progress, effective_fps
                );

                last_stats_time = Instant::now();
                last_stats_tick = Instant::now();
                last_processed_count = processed;
            }
        }
    }

    async fn process_single_frame(&self, envelope: FrameEnvelope) -> Result<()> {
        // Increment the in-progress counter
        self.frames_in_progress
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let frame_start = Instant::now();
        let dequeue_time = Instant::now();
        let queue_wait_ms = dequeue_time
            .saturating_duration_since(envelope.enqueued_at)
            .as_millis();
        let frame = envelope.frame;
        // Extract frame metadata
        let best_window_opt = frame
            .window_ocr_results
            .iter()
            .find(|w| w.focused)
            .or_else(|| frame.window_ocr_results.get(0));

        let frame_wallclock = {
            let elapsed = frame.timestamp.elapsed();
            let dur = chrono::Duration::from_std(elapsed).unwrap_or(chrono::Duration::zero());
            chrono::Utc::now() - dur
        };

        // First insert frame in DB and get the real frame_id
        let (resp_tx, resp_rx) = oneshot::channel::<i64>();

        let (app_name_lower, window_name_lower, browser_url_opt, focused_flag) =
            if let Some(best) = best_window_opt {
                (
                    best.app_name.to_lowercase(),
                    best.window_name.to_lowercase(),
                    best.browser_url.clone(),
                    best.focused,
                )
            } else {
                (
                    "unknown app".to_string(),
                    "unknown window".to_string(),
                    None,
                    false,
                )
            };

        let db_task = DbTask {
            frame_id: 0, // placeholder, DB will assign
            device_name: envelope.device_name.clone(),
            timestamp: frame_wallclock,
            browser_url: browser_url_opt.clone(),
            app_name: Some(app_name_lower.clone()),
            window_name: Some(window_name_lower.clone()),
            focused: focused_flag,
            response_tx: Some(resp_tx),
        };

        if let Err(e) = self.db_tx.send(db_task) {
            warn!("Failed to send DB task: {}", e);
            return Ok(()); // cannot proceed without a frame id
        }

        // Dispatch OCR and UI after DB returns the actual frame_id, without blocking the main loop
        let ocr_tx = self.ocr_tx.clone();
        let ui_tx = self.ui_tx.clone();
        let use_pii_removal = self.use_pii_removal;
        let frame_for_tasks = frame.clone();
        let app_for_tasks = app_name_lower.clone();
        let window_for_tasks = window_name_lower.clone();
        let ts_for_tasks = frame_wallclock;
        let device_name_for_tasks = envelope.device_name.clone();
        info!(
            "frame queued {} ms before processing; app={}, window={}",
            queue_wait_ms, app_for_tasks, window_for_tasks
        );

        tokio::spawn(async move {
            let db_wait_started = Instant::now();
            match resp_rx.await {
                Ok(frame_id) if frame_id > 0 => {
                    let db_roundtrip_ms = db_wait_started.elapsed().as_millis();
                    info!(
                        "db assigned frame_id {} in {} ms (app={}, window={})",
                        frame_id, db_roundtrip_ms, app_for_tasks, window_for_tasks
                    );

                    // Track total processing time from this point
                    let total_processing_start = Instant::now();

                    // OCR task
                    let ocr_task = OcrTask {
                        frame: frame_for_tasks.clone(),
                        frame_id,
                        window_results: frame_for_tasks
                            .window_ocr_results
                            .iter()
                            .map(|w| WindowOcrResult {
                                text: w.text.clone(),
                                text_json: serde_json::to_string(&w.text_json).unwrap_or_default(),
                                app_name: w.app_name.clone(),
                                window_name: w.window_name.clone(),
                                focused: w.focused,
                                confidence: w.confidence as f32,
                                browser_url: w.browser_url.clone(),
                            })
                            .collect(),
                        use_pii_removal,
                    };
                    let _ = ocr_tx.send(ocr_task);

                    // UI task
                    let ui_task = UiTask {
                        frame: frame_for_tasks,
                        frame_id,
                        app_name: app_for_tasks,
                        window_name: window_for_tasks,
                        timestamp: ts_for_tasks,
                    };
                    let _ = ui_tx.send(ui_task);

                    // Log dispatch completion
                    let dispatch_ms = total_processing_start.elapsed().as_millis();
                    info!(
                        "workers dispatched for frame {} in {} ms",
                        frame_id, dispatch_ms
                    );

                    // Note: We can't easily track actual completion without modifying worker signatures
                    // The real bottleneck is likely in the synchronous coordination, not individual worker times
                }
                Ok(_) => {
                    warn!("Received invalid DB frame id: 0");
                }
                Err(e) => {
                    warn!("Failed to receive DB frame id: {}", e);
                }
            }
        });

        // Optionally send a lightweight video task immediately (non-blocking)
        let _ = self.video_tx.send(VideoTask {
            frame,
            frame_id: 0,
            device_name: device_name_for_tasks,
        });

        // Log total frame processing time (from dequeue to completion of main processing)
        let total_processing_ms = frame_start.elapsed().as_millis();
        info!(
            "frame processing completed in {} ms (queue_wait: {}ms)",
            total_processing_ms, queue_wait_ms
        );

        // Decrement the in-progress counter
        self.frames_in_progress
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    fn spawn_video_worker(
        mut rx: mpsc::UnboundedReceiver<VideoTask>,
        _db: Arc<DatabaseManager>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                if task.frame_id == 0 {
                    debug!(
                        "processing video task (pending frame id) for device {}",
                        task.device_name
                    );
                } else {
                    debug!("processing video task for frame {}", task.frame_id);
                }

                // Video processing logic
                // For now, we'll just log the task since the actual video encoding
                // is handled by the existing VideoCapture system
                // In a full implementation, this would:
                // 1. Encode the frame to video format
                // 2. Add to video chunk
                // 3. Handle chunk finalization when needed
                // 4. Manage video file rotation

                // Simulate some processing time
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
    }

    fn spawn_ocr_worker(
        mut rx: mpsc::UnboundedReceiver<OcrTask>,
        db: Arc<DatabaseManager>,
        ocr_engine: Arc<OcrEngine>,
        use_pii_removal: bool,
        _languages: Vec<Language>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("OCR worker started");
            while let Some(task) = rx.recv().await {
                let start = Instant::now();
                info!(
                    "processing ocr task for frame {} ({} windows)",
                    task.frame_id,
                    task.window_results.len()
                );

                // Process each window OCR result
                for window_result in &task.window_results {
                    let w_start = Instant::now();
                    let text = if use_pii_removal {
                        screenpipe_core::pii_removal::remove_pii(&window_result.text)
                    } else {
                        window_result.text.clone()
                    };

                    let engine: Arc<screenpipe_db::OcrEngine> =
                        Arc::new((*ocr_engine).clone().into());

                    match db
                        .insert_ocr_text(task.frame_id, &text, &window_result.text_json, engine)
                        .await
                    {
                        Ok(_) => {
                            let w_ms = w_start.elapsed().as_millis();
                            info!(
                                "ocr inserted for frame {} window={} ({} ms)",
                                task.frame_id, window_result.window_name, w_ms
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to insert OCR text for frame {}: {}",
                                task.frame_id, e
                            );
                        }
                    }
                }
                let total_ms = start.elapsed().as_millis();
                info!(
                    "ocr task completed for frame {} in {} ms",
                    task.frame_id, total_ms
                );
            }
        })
    }

    fn spawn_ui_worker(
        mut rx: mpsc::UnboundedReceiver<UiTask>,
        db: Arc<DatabaseManager>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("UI worker started");
            while let Some(task) = rx.recv().await {
                let start = Instant::now();
                info!(
                    "processing ui task for frame {} app={} window={}",
                    task.frame_id, task.app_name, task.window_name
                );

                // Check if UI already exists for this frame to avoid duplicate snapshots
                match db.ui_monitoring_exists_for_frame(task.frame_id).await {
                    Ok(exists) if exists => {
                        let total_ms = start.elapsed().as_millis();
                        debug!(
                            "UI already exists for frame {}, skipping ({} ms)",
                            task.frame_id, total_ms
                        );
                        continue;
                    }
                    Ok(_) => {
                        // UI doesn't exist, continue processing
                    }
                    Err(e) => {
                        let total_ms = start.elapsed().as_millis();
                        warn!(
                            "Failed to check if UI exists for frame {}: {} ({} ms)",
                            task.frame_id, e, total_ms
                        );
                        continue;
                    }
                }

                let ax_start = Instant::now();
                let ax_text = if let Some(snapshot) = task.frame.ui_snapshot.as_ref() {
                    if snapshot.app == task.app_name && snapshot.window == task.window_name {
                        let age = snapshot
                            .captured_at
                            .checked_duration_since(task.frame.timestamp)
                            .or_else(|| {
                                task.frame
                                    .timestamp
                                    .checked_duration_since(snapshot.captured_at)
                            })
                            .unwrap_or_default();

                        if age > Duration::from_secs(2) {
                            warn!(
                                "ui snapshot stale for frame {} (age={} ms)",
                                task.frame_id,
                                age.as_millis()
                            );
                            String::new()
                        } else {
                            snapshot.text.clone()
                        }
                    } else {
                        warn!(
                            "ui snapshot mismatch for frame {}: expected app/window {}:{}, got {}:{}", 
                            task.frame_id,
                            task.app_name,
                            task.window_name,
                            snapshot.app,
                            snapshot.window
                        );
                        String::new()
                    }
                } else {
                    debug!(
                        "no inline ui snapshot available for frame {} app={} window={}",
                        task.frame_id, task.app_name, task.window_name
                    );
                    String::new()
                };
                let ax_ms = ax_start.elapsed().as_millis();
                info!(
                    "ui text resolved for frame {} in {} ms (len={})",
                    task.frame_id,
                    ax_ms,
                    ax_text.len()
                );

                // Insert UI monitoring data
                match db
                    .upsert_ui_monitoring(
                        task.timestamp,
                        None,
                        &task.app_name,
                        &task.window_name,
                        &ax_text,
                        task.frame_id,
                    )
                    .await
                {
                    Ok(_) => {
                        let total_ms = start.elapsed().as_millis();
                        info!(
                            "ui task completed for frame {} in {} ms",
                            task.frame_id, total_ms
                        );
                    }
                    Err(e) => {
                        let total_ms = start.elapsed().as_millis();
                        warn!(
                            "Failed to insert UI text for frame {}: {} ({} ms)",
                            task.frame_id, e, total_ms
                        );
                    }
                }
            }
        })
    }

    fn spawn_db_worker(
        mut rx: mpsc::UnboundedReceiver<DbTask>,
        db: Arc<DatabaseManager>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("DB worker started");
            while let Some(task) = rx.recv().await {
                let start = Instant::now();
                if task.frame_id == 0 {
                    info!(
                        "processing db task (pending frame id) for device {} at {}",
                        task.device_name, task.timestamp
                    );
                } else {
                    info!("processing db task for frame {}", task.frame_id);
                }

                // Insert frame into database
                match db
                    .insert_frame(
                        &task.device_name,
                        Some(task.timestamp),
                        task.browser_url.as_deref(),
                        task.app_name.as_deref(),
                        task.window_name.as_deref(),
                        task.focused,
                    )
                    .await
                {
                    Ok(frame_id) => {
                        let ms = start.elapsed().as_millis();
                        info!("frame inserted with id {} ({} ms)", frame_id, ms);
                        if let Some(tx) = task.response_tx {
                            let _ = tx.send(frame_id);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to insert frame {}: {}", task.frame_id, e);
                        if let Some(tx) = task.response_tx {
                            let _ = tx.send(0);
                        }
                    }
                }
            }
        })
    }

    /// Get processing statistics
    pub fn get_stats(&self) -> (u64, u64, usize) {
        (
            self.frames_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            self.frames_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            self.frame_queue.len(),
        )
    }

    /// Shutdown the processor
    pub async fn shutdown(self) -> Result<()> {
        // Close channels to signal workers to stop
        drop(self.video_tx);
        drop(self.ocr_tx);
        drop(self.ui_tx);
        drop(self.db_tx);

        // Wait for workers to finish
        let _ = tokio::join!(
            self.video_worker,
            self.ocr_worker,
            self.ui_worker,
            self.db_worker,
        );

        Ok(())
    }
}
