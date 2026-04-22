use std::sync::Arc;
use std::time::Instant;

use agent_settings::NtfyConfig;
use anyhow::Result;
use http_client::{AsyncBody, HttpClient, Method, Request};

/// ntfy notification priority (maps to ntfy.sh priority header values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NtfyPriority {
    /// Task completed notifications.
    High = 4,
    /// Needs-confirmation notifications (bypasses DND on iOS).
    Urgent = 5,
}

impl NtfyPriority {
    fn header_value(self) -> &'static str {
        match self {
            NtfyPriority::High => "4",
            NtfyPriority::Urgent => "5",
        }
    }
}

/// A notification that the monitor has decided should be sent.
#[derive(Debug)]
pub struct NtfySend {
    pub title: String,
    pub body: String,
    pub priority: NtfyPriority,
}

struct PendingNotification {
    title: String,
    body: String,
    priority: NtfyPriority,
}

/// State machine that tracks agent events, user activity, and decides when to
/// fire ntfy push notifications. Runs inside Zed's process, driven directly by
/// `AcpThreadEvent` — no polling, no DB analysis.
///
/// The monitor is **pure** — it never performs I/O itself. `tick` returns a
/// `Vec<NtfySend>` that the caller dispatches via HTTP on a background thread.
pub struct NtfyMonitor {
    // --- audio continuous-play tracking ---
    audio_active_since: Option<Instant>,
    last_audio_app: Option<String>,

    // --- pending events ---
    /// A confirmation request that the agent is waiting on.
    pending_confirmation: Option<PendingNotification>,
    /// A task-done or error notification to send once the user goes AFK.
    deferred_send: Option<PendingNotification>,

    // --- re-notification for pending confirmations ---
    last_confirmation_sent_at: Option<Instant>,

    // --- activity tracking for suppression ---
    suppression_logged_confirmation: bool,
    suppression_logged_deferred: bool,
    user_was_active: bool,
}

impl NtfyMonitor {
    pub fn new() -> Self {
        Self {
            audio_active_since: None,
            last_audio_app: None,
            pending_confirmation: None,
            deferred_send: None,
            last_confirmation_sent_at: None,
            suppression_logged_confirmation: false,
            suppression_logged_deferred: false,
            user_was_active: false,
        }
    }

    /// The agent is waiting for tool confirmation.
    pub fn on_confirmation_requested(&mut self, thread_title: &str) {
        self.suppression_logged_confirmation = false;
        self.pending_confirmation = Some(PendingNotification {
            title: "Needs Confirmation".to_string(),
            body: if thread_title.is_empty() {
                "Agent is waiting for your approval".to_string()
            } else {
                thread_title.to_string()
            },
            priority: NtfyPriority::Urgent,
        });
    }

    /// Tool authorization was granted — confirmation no longer pending.
    pub fn on_confirmation_received(&mut self) {
        self.pending_confirmation = None;
        self.last_confirmation_sent_at = None;
        self.suppression_logged_confirmation = false;
    }

    /// The agent finished its turn (non-subagent only).
    pub fn on_task_done(&mut self, thread_title: &str) {
        self.suppression_logged_deferred = false;
        self.deferred_send = Some(PendingNotification {
            title: "Task Done".to_string(),
            body: if thread_title.is_empty() {
                "Agent completed its task".to_string()
            } else {
                thread_title.to_string()
            },
            priority: NtfyPriority::High,
        });
    }

    /// The agent stopped due to an error (non-subagent only).
    pub fn on_error(&mut self, thread_title: &str) {
        self.suppression_logged_deferred = false;
        self.deferred_send = Some(PendingNotification {
            title: "Agent Error".to_string(),
            body: if thread_title.is_empty() {
                "Agent stopped due to an error".to_string()
            } else {
                format!("Error — {thread_title}")
            },
            priority: NtfyPriority::High,
        });
    }

    /// The user sent a message — they are clearly present, clear all pending state.
    pub fn on_user_message(&mut self) {
        self.pending_confirmation = None;
        self.deferred_send = None;
        self.last_confirmation_sent_at = None;
        self.suppression_logged_confirmation = false;
        self.suppression_logged_deferred = false;
    }

    /// Called every ~2 seconds with fresh activity-check results.
    ///
    /// * `idle_ms` — milliseconds since last keyboard/mouse input (`None` if
    ///   xprintidle is unavailable, treated as AFK).
    /// * `audio_app` — name of any non-excluded app currently playing audio.
    ///
    /// Returns notifications the caller should POST to the ntfy URL.
    pub fn tick(
        &mut self,
        config: &NtfyConfig,
        idle_ms: Option<u64>,
        audio_app: Option<String>,
    ) -> Vec<NtfySend> {
        let mut sends = Vec::new();

        self.update_audio_state(&audio_app, config.audio_min_secs);

        let input_active = idle_ms
            .map(|ms| ms < config.idle_threshold_ms)
            .unwrap_or(false);
        let audio_active = self
            .audio_active_since
            .map(|since| since.elapsed().as_secs() >= config.audio_min_secs)
            .unwrap_or(false);
        let user_active = input_active || audio_active;

        let just_went_afk = self.user_was_active && !user_active;
        self.user_was_active = user_active;

        if !user_active {
            // Deferred task-done / error (was suppressed while user was active).
            if let Some(notification) = self.deferred_send.take() {
                if just_went_afk {
                    log::info!("ntfy: user just went AFK, sending deferred notification");
                }
                sends.push(NtfySend {
                    title: notification.title,
                    body: notification.body,
                    priority: notification.priority,
                });
                self.suppression_logged_deferred = false;
            }

            // Pending confirmation: first send or periodic re-notification.
            if let Some(ref confirmation) = self.pending_confirmation {
                let should_send = match self.last_confirmation_sent_at {
                    None => true,
                    Some(last_sent) => last_sent.elapsed().as_secs() >= config.renotify_secs,
                };

                if should_send {
                    if self.last_confirmation_sent_at.is_some() {
                        log::info!("ntfy: re-notifying for pending confirmation");
                    }
                    sends.push(NtfySend {
                        title: confirmation.title.clone(),
                        body: confirmation.body.clone(),
                        priority: confirmation.priority,
                    });
                    self.last_confirmation_sent_at = Some(Instant::now());
                    self.suppression_logged_confirmation = false;
                }
            }
        } else {
            // User is active — suppress and log once per event.
            if self.pending_confirmation.is_some() && !self.suppression_logged_confirmation {
                log::info!("ntfy: confirmation notification suppressed — user active");
                self.suppression_logged_confirmation = true;
            }
            if self.deferred_send.is_some() && !self.suppression_logged_deferred {
                log::info!("ntfy: task-done notification suppressed — user active");
                self.suppression_logged_deferred = true;
            }
        }

        sends
    }

    fn update_audio_state(&mut self, audio_app: &Option<String>, audio_min_secs: u64) {
        match audio_app {
            Some(app) => {
                if self.audio_active_since.is_none() {
                    self.audio_active_since = Some(Instant::now());
                }
                if self.last_audio_app.as_deref() != Some(app.as_str()) {
                    let elapsed = self
                        .audio_active_since
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0);
                    if elapsed >= audio_min_secs {
                        log::debug!("ntfy: active audio from {app} ({elapsed}s)");
                    }
                    self.last_audio_app = Some(app.clone());
                }
            }
            None => {
                self.audio_active_since = None;
                self.last_audio_app = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Activity checking (Linux / X11 + PulseAudio/PipeWire)
// ---------------------------------------------------------------------------

/// Returns keyboard/mouse idle time in milliseconds, or `None` if xprintidle
/// is not available.
pub fn check_idle_ms() -> Option<u64> {
    let output = std::process::Command::new("xprintidle").output().ok()?;
    String::from_utf8(output.stdout)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Returns the application name of the first non-excluded PulseAudio/PipeWire
/// sink-input that is actively playing audio, or `None`.
///
/// Handles both PulseAudio (`State: RUNNING`) and PipeWire (`Corked: no`).
/// Excluded apps: strawberry, speech-dispatcher, sd_dummy, godot.
pub fn check_active_audio_app() -> Option<String> {
    let output = std::process::Command::new("pactl")
        .args(["list", "sink-inputs"])
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;

    for block in text.split("Sink Input #").skip(1) {
        let active = block.lines().any(|line| {
            let trimmed = line.trim();
            trimmed == "Corked: no" || trimmed.contains("State: RUNNING")
        });
        if !active {
            continue;
        }

        let app: String = block
            .lines()
            .filter(|line| {
                let lower = line.to_lowercase();
                lower.contains("application.name") || lower.contains("application.process.binary")
            })
            .map(|line| line.trim().to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let lower = app.to_lowercase();
        let excluded = lower.contains("strawberry")
            || lower.contains("speech-dispatcher")
            || lower.contains("sd_dummy")
            || lower.contains("godot");

        if !excluded {
            return Some(app);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// HTTP sending
// ---------------------------------------------------------------------------

/// POST a notification to the configured ntfy topic URL.
///
/// Single attempt — the monitor's re-notification mechanism for confirmations
/// provides natural retry semantics.
pub async fn send_ntfy(
    http_client: &Arc<dyn HttpClient>,
    url: &str,
    title: &str,
    body: &str,
    priority: NtfyPriority,
) -> Result<()> {
    let request = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("Title", title)
        .header("Priority", priority.header_value())
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(AsyncBody::from(body.to_string()))?;

    let response = http_client.send(request).await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("ntfy returned HTTP {status}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NtfyConfig {
        NtfyConfig {
            url: "http://unused".to_string(),
            idle_threshold_ms: 60_000,
            audio_min_secs: 10,
            renotify_secs: 300,
        }
    }

    #[test]
    fn sends_confirmation_when_user_is_afk() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].title, "Needs Confirmation");
        assert_eq!(sends[0].body, "My thread");
        assert_eq!(sends[0].priority, NtfyPriority::Urgent);
    }

    #[test]
    fn suppresses_when_user_is_active() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");

        let sends = monitor.tick(&cfg, Some(5_000), None);
        assert!(sends.is_empty());
    }

    #[test]
    fn defers_task_done_until_afk() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();

        monitor.tick(&cfg, Some(5_000), None);
        monitor.on_task_done("Build succeeded");

        let sends = monitor.tick(&cfg, Some(5_000), None);
        assert!(sends.is_empty());

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].title, "Task Done");
        assert_eq!(sends[0].body, "Build succeeded");
    }

    #[test]
    fn clears_pending_on_user_message() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");
        monitor.on_user_message();

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert!(sends.is_empty());
    }

    #[test]
    fn confirmation_received_clears_pending() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");
        monitor.on_confirmation_received();

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert!(sends.is_empty());
    }

    #[test]
    fn no_duplicate_confirmation_before_renotify_interval() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends.len(), 1);

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert!(sends.is_empty());
    }

    #[test]
    fn audio_counts_as_active() {
        let mut cfg = test_config();
        cfg.audio_min_secs = 0;
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");

        let sends = monitor.tick(&cfg, Some(120_000), Some("firefox".to_string()));
        assert!(sends.is_empty());
    }

    #[test]
    fn xprintidle_unavailable_treated_as_afk() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("My thread");

        let sends = monitor.tick(&cfg, None, None);
        assert_eq!(sends.len(), 1);
    }

    #[test]
    fn error_event_defers_like_task_done() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();

        monitor.tick(&cfg, Some(5_000), None);
        monitor.on_error("Build thread");

        let sends = monitor.tick(&cfg, Some(5_000), None);
        assert!(sends.is_empty());

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].title, "Agent Error");
    }

    #[test]
    fn just_went_afk_sends_deferred() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();

        monitor.tick(&cfg, Some(5_000), None);
        monitor.on_task_done("Deploy finished");
        monitor.tick(&cfg, Some(5_000), None);

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].title, "Task Done");
    }

    #[test]
    fn empty_thread_title_uses_fallback() {
        let cfg = test_config();
        let mut monitor = NtfyMonitor::new();
        monitor.on_confirmation_requested("");

        let sends = monitor.tick(&cfg, Some(120_000), None);
        assert_eq!(sends[0].body, "Agent is waiting for your approval");
    }
}
