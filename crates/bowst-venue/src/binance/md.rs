//! Binance Spot market-data session: one WebSocket carrying the diff-depth streams of every
//! configured instrument, kept in sync with REST snapshots.
//!
//! Threads:
//!
//! - The **market-data thread** runs [`MdSession::run`]. It owns the WebSocket, the decoder
//!   and every [`BookSync`], busy-polls the socket, and reports books and status changes to an
//!   [`MdHandler`].
//! - A **snapshot thread** fetches REST depth snapshots on request, within a request-weight
//!   budget, and honors `Retry-After`. A resync of one instrument therefore never stalls
//!   updates for the others.
//!
//! Fail-closed behavior (CLAUDE.md §1.4):
//!
//! - A sequence gap, invalid data or a crossed book takes that instrument down
//!   ([`MdStatus::InstrumentDown`]) until a fresh snapshot resynchronizes it.
//! - Any message that cannot be decoded, a protocol error, silence longer than `stale_after`,
//!   or the connection reaching `max_connection_age` (Binance closes connections after 24 hours)
//!   drops the connection. Every book goes down, and the session reconnects with jittered
//!   exponential backoff.
//! - Snapshot results from before a reconnect are discarded, so a stale snapshot can never be
//!   applied to a new stream.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use bowst_book::{Book, BookSync, DeltaOutcome, Level, SyncConfig, SyncError};
use bowst_core::{Instrument, InstrumentId, InstrumentTable, MonoTime, WallTime};

use super::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use super::rest::{RestError, depth_url, depth_weight, require_ok};
use crate::backoff::Backoff;
use crate::net::{NetError, TlsConfig, Url, http};
use crate::ratelimit::TokenBucket;
use crate::ws::client::WsClient;
use crate::ws::frame::Opcode;
use crate::ws::reader::{ReaderConfig, WsEvent};

/// Session settings.
#[derive(Clone, Debug)]
pub struct MdConfig {
    /// WebSocket base URL, for example `wss://stream.binance.com:9443`.
    pub stream_url: String,
    /// REST base URL, for example `https://api.binance.com`.
    pub rest_url: String,
    /// Levels per side requested in each snapshot (Binance allows up to 5,000).
    pub snapshot_limit: u32,
    /// Book and delta-buffer sizes, per instrument.
    pub sync: SyncConfig,
    /// WebSocket receive buffer sizes.
    pub reader: ReaderConfig,
    /// Largest number of levels one diff-depth message may carry.
    pub max_levels_per_message: usize,
    /// Timeout for each connection setup step and each REST call.
    pub connect_timeout: Duration,
    /// Reconnect if nothing (data or ping) arrives for this long.
    pub stale_after: Duration,
    /// Reconnect proactively once a connection is this old.
    pub max_connection_age: Duration,
    /// First reconnect delay; doubles per failed attempt.
    pub reconnect_initial: Duration,
    /// Longest reconnect delay.
    pub reconnect_max: Duration,
    /// A connection that stays up this long resets the reconnect backoff.
    pub healthy_after: Duration,
    /// Wait before re-requesting a snapshot that failed.
    pub snapshot_retry: Duration,
    /// REST request weight this session may spend per minute. Keep well under the venue's
    /// limit, which is shared with every other client on the same IP.
    pub rest_weight_per_minute: u64,
    /// `None` busy-polls the socket (production, on an isolated core). `Some` sleeps this long
    /// when no data is waiting (tools and tests on shared machines).
    pub idle_sleep: Option<Duration>,
}

impl MdConfig {
    /// Production-style defaults for the given endpoints.
    #[must_use]
    pub fn new(stream_url: &str, rest_url: &str) -> Self {
        Self {
            stream_url: stream_url.to_owned(),
            rest_url: rest_url.to_owned(),
            snapshot_limit: 1_000,
            sync: SyncConfig {
                levels_per_side: 5_000,
                buffered_messages: 4_096,
                buffered_updates: 1 << 18,
            },
            reader: ReaderConfig {
                buffer: 4 << 20,
                max_frame: 2 << 20,
                max_message: 2 << 20,
            },
            max_levels_per_message: 20_000,
            connect_timeout: Duration::from_secs(10),
            stale_after: Duration::from_secs(60),
            max_connection_age: Duration::from_secs(23 * 3_600),
            reconnect_initial: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(30),
            healthy_after: Duration::from_secs(60),
            snapshot_retry: Duration::from_secs(2),
            rest_weight_per_minute: 1_200,
            idle_sleep: None,
        }
    }
}

/// A change in session or instrument state, for logs, metrics and the risk layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MdStatus {
    /// Opening the WebSocket.
    Connecting,
    /// The WebSocket is open; snapshots are being fetched.
    Connected,
    /// The connection ended; every book is down until it resynchronizes.
    Disconnected {
        /// Why.
        reason: String,
    },
    /// An instrument's book is synchronized and may be used.
    InstrumentLive(InstrumentId),
    /// An instrument's book may not be used until it resynchronizes.
    InstrumentDown {
        /// Which instrument.
        instrument: InstrumentId,
        /// Why.
        reason: String,
    },
    /// A snapshot request failed; it will be retried.
    SnapshotFailed {
        /// Which instrument.
        instrument: InstrumentId,
        /// Why.
        reason: String,
    },
}

/// Receives the session's output on the market-data thread.
pub trait MdHandler {
    /// A book changed and is live. Called on the hot path: must not block or allocate.
    fn on_book(&mut self, instrument: InstrumentId, book: &Book, event_time: WallTime);
    /// Session or instrument state changed. Rare; may log.
    fn on_status(&mut self, status: MdStatus);
}

/// Counters since the session started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MdStats {
    /// WebSocket data messages received.
    pub messages: u64,
    /// Deltas applied to live books.
    pub deltas_applied: u64,
    /// Books taken down by a gap, invalid data or a crossed book.
    pub book_invalidations: u64,
    /// Connections opened.
    pub connections: u64,
    /// Snapshots requested.
    pub snapshots_requested: u64,
    /// Snapshots that brought a book live.
    pub snapshots_applied: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotState {
    /// Nothing outstanding.
    Idle,
    /// A request is in flight.
    Requested,
    /// Waiting before the next request.
    RetryAt(Instant),
}

/// The market-data session. See the module docs.
#[derive(Debug)]
pub struct MdSession<H> {
    config: MdConfig,
    instruments: InstrumentTable,
    tls: TlsConfig,
    stream_url: Url,
    syncs: Vec<BookSync>,
    states: Vec<SnapshotState>,
    decoder: DepthUpdateDecoder,
    jobs: Sender<SnapshotJob>,
    results: Receiver<SnapshotResult>,
    /// Incremented on every reconnect; snapshot results from older generations are ignored.
    generation: u64,
    handler: H,
    stats: MdStats,
}

impl<H: MdHandler> MdSession<H> {
    /// Allocates every book and buffer and starts the snapshot thread.
    ///
    /// # Errors
    /// [`MdError`] for an invalid configuration.
    pub fn new(
        config: MdConfig,
        instruments: InstrumentTable,
        tls: TlsConfig,
        handler: H,
    ) -> Result<Self, MdError> {
        let streams = instruments
            .iter()
            .map(|i| format!("{}@depth@100ms", i.symbol.as_str().to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        if streams.is_empty() {
            return Err(MdError::NoInstruments);
        }
        let stream_url = Url::parse(&format!(
            "{}/stream?streams={streams}",
            config.stream_url.trim_end_matches('/')
        ))?;
        let syncs = instruments
            .iter()
            .map(|_| BookSync::new(config.sync))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| MdError::InvalidSizes)?;
        let (jobs, job_rx) = mpsc::channel();
        let (result_tx, results) = mpsc::channel();
        let worker = SnapshotWorker {
            rest_url: config.rest_url.clone(),
            limit: config.snapshot_limit,
            timeout: config.connect_timeout,
            tls: tls.clone(),
            weight_per_minute: config.rest_weight_per_minute,
        };
        thread::Builder::new()
            .name("md-snapshots".into())
            .spawn(move || worker.run(&job_rx, &result_tx))
            .map_err(|_| MdError::Spawn)?;
        Ok(Self {
            states: vec![SnapshotState::Idle; syncs.len()],
            decoder: DepthUpdateDecoder::new(config.max_levels_per_message),
            config,
            instruments,
            tls,
            stream_url,
            syncs,
            jobs,
            results,
            generation: 0,
            handler,
            stats: MdStats::default(),
        })
    }

    /// Counters since the session started.
    #[must_use]
    pub fn stats(&self) -> MdStats {
        self.stats
    }

    /// The handler, for inspection after [`run`](Self::run) returns.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Runs until `stop` is set: connect, stream, resync on gaps, reconnect on failure.
    pub fn run(&mut self, stop: &AtomicBool) {
        let mut backoff = Backoff::new(self.config.reconnect_initial, self.config.reconnect_max);
        while !stop.load(Ordering::Relaxed) {
            self.handler.on_status(MdStatus::Connecting);
            let connected = WsClient::connect(
                &self.stream_url,
                &self.tls,
                self.config.reader,
                self.config.connect_timeout,
            );
            let reason = match connected {
                Ok(mut client) => {
                    self.stats.connections = self.stats.connections.saturating_add(1);
                    self.handler.on_status(MdStatus::Connected);
                    let started = Instant::now();
                    let reason = self.stream(&mut client, stop);
                    client.close();
                    if started.elapsed() >= self.config.healthy_after {
                        backoff.reset();
                    }
                    reason
                }
                Err(err) => err.to_string(),
            };
            self.take_all_down();
            self.handler.on_status(MdStatus::Disconnected { reason });
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let mut random = [0_u8; 4];
            let _ = self.tls.fill_random(&mut random);
            sleep_unless_stopped(backoff.next_delay(u32::from_le_bytes(random)), stop);
        }
    }

    /// Streams on one connection until it must be dropped; returns why.
    fn stream(&mut self, client: &mut WsClient, stop: &AtomicBool) -> String {
        let connected_at = Instant::now();
        let mut last_activity = connected_at;
        let mut pong = [0_u8; 125];
        loop {
            if stop.load(Ordering::Relaxed) {
                return "stopped".into();
            }
            // Drain everything already received before touching the socket again.
            loop {
                let event = match client.next_event() {
                    Ok(Some(event)) => event,
                    Ok(None) => break,
                    Err(err) => return err.to_string(),
                };
                last_activity = Instant::now();
                match event {
                    WsEvent::Text(message) | WsEvent::Binary(message) => {
                        if let Err(reason) = self.on_message(message) {
                            return reason;
                        }
                    }
                    WsEvent::Ping(payload) => {
                        let n = payload.len().min(pong.len());
                        if let (Some(dst), Some(src)) = (pong.get_mut(..n), payload.get(..n)) {
                            dst.copy_from_slice(src);
                        }
                        if let Err(err) =
                            client.send(Opcode::Pong, pong.get(..n).unwrap_or_default())
                        {
                            return err.to_string();
                        }
                    }
                    WsEvent::Pong(_) => {}
                    WsEvent::Close { code, .. } => {
                        return format!("server closed the connection (code {code:?})");
                    }
                }
            }
            self.collect_snapshots();
            self.request_snapshots();

            let now = Instant::now();
            if now.duration_since(last_activity) >= self.config.stale_after {
                return format!("no data for {:?}", self.config.stale_after);
            }
            if now.duration_since(connected_at) >= self.config.max_connection_age {
                return "connection reached its maximum age".into();
            }
            match client.fill() {
                Ok(0) => match self.config.idle_sleep {
                    Some(pause) => thread::sleep(pause),
                    None => std::hint::spin_loop(),
                },
                Ok(_) => last_activity = Instant::now(),
                Err(err) => return err.to_string(),
            }
        }
    }

    /// Decodes and applies one stream message. Hot path: no allocation on success.
    fn on_message(&mut self, message: &[u8]) -> Result<(), String> {
        self.stats.messages = self.stats.messages.saturating_add(1);
        let update = match self.decoder.decode(message, &self.instruments) {
            Ok(update) => update,
            // Unattributable data means unknown state: drop the connection (all books resync).
            Err(err) => return Err(format!("undecodable message: {err}")),
        };
        let index = index_of(update.instrument);
        let Some(sync) = self.syncs.get_mut(index) else {
            return Err("message for an unconfigured instrument".into());
        };
        match sync.on_delta(update.range, update.updates.iter().copied()) {
            Ok(DeltaOutcome::Applied) => {
                self.stats.deltas_applied = self.stats.deltas_applied.saturating_add(1);
                if let Some(book) = sync.book() {
                    self.handler
                        .on_book(update.instrument, book, update.event_time);
                }
            }
            Ok(DeltaOutcome::Buffered | DeltaOutcome::Stale) => {}
            Err(err) => {
                self.stats.book_invalidations = self.stats.book_invalidations.saturating_add(1);
                if let Some(state) = self.states.get_mut(index) {
                    *state = SnapshotState::Idle;
                }
                self.handler.on_status(MdStatus::InstrumentDown {
                    instrument: update.instrument,
                    reason: err.to_string(),
                });
            }
        }
        Ok(())
    }

    fn request_snapshots(&mut self) {
        let now = Instant::now();
        for (index, (sync, state)) in self.syncs.iter().zip(self.states.iter_mut()).enumerate() {
            let due = match *state {
                SnapshotState::Idle => true,
                SnapshotState::RetryAt(at) => now >= at,
                SnapshotState::Requested => false,
            };
            if sync.is_live() || !due {
                continue;
            }
            let Some(instrument) = self.instruments.get(id_of(index)) else {
                continue;
            };
            if self
                .jobs
                .send(SnapshotJob {
                    instrument: *instrument,
                    generation: self.generation,
                })
                .is_ok()
            {
                *state = SnapshotState::Requested;
                self.stats.snapshots_requested = self.stats.snapshots_requested.saturating_add(1);
            }
        }
    }

    fn collect_snapshots(&mut self) {
        loop {
            let result = match self.results.try_recv() {
                Ok(result) => result,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            };
            if result.generation != self.generation {
                continue; // Fetched for a previous connection.
            }
            self.apply_snapshot(result);
        }
    }

    fn apply_snapshot(&mut self, result: SnapshotResult) {
        let index = index_of(result.instrument);
        let retry_at = later(self.config.snapshot_retry);
        let (Some(sync), Some(state)) = (self.syncs.get_mut(index), self.states.get_mut(index))
        else {
            return;
        };
        let snapshot = match result.outcome {
            Ok(snapshot) => snapshot,
            Err(failure) => {
                let wait = failure.retry_after.map_or(self.config.snapshot_retry, |w| {
                    w.max(self.config.snapshot_retry)
                });
                *state = SnapshotState::RetryAt(later(wait));
                self.handler.on_status(MdStatus::SnapshotFailed {
                    instrument: result.instrument,
                    reason: failure.reason,
                });
                return;
            }
        };
        match sync.on_snapshot(snapshot.last_update_id, snapshot.bids, snapshot.asks) {
            Ok(true) => {
                *state = SnapshotState::Idle;
                if let Some(book) = sync.book() {
                    self.stats.snapshots_applied = self.stats.snapshots_applied.saturating_add(1);
                    self.handler
                        .on_status(MdStatus::InstrumentLive(result.instrument));
                    self.handler
                        .on_book(result.instrument, book, snapshot.fetched_at);
                }
            }
            // `Ok(false)`: older than the already-live book, so nothing changes (and no second
            // "live" event). `SnapshotTooOld`: the stream has moved past it, so fetch a newer
            // one right away. Either way the instrument is idle again.
            Ok(false) | Err(SyncError::SnapshotTooOld { .. }) => *state = SnapshotState::Idle,
            Err(err) => {
                *state = SnapshotState::RetryAt(retry_at);
                self.stats.book_invalidations = self.stats.book_invalidations.saturating_add(1);
                self.handler.on_status(MdStatus::InstrumentDown {
                    instrument: result.instrument,
                    reason: err.to_string(),
                });
            }
        }
    }

    /// Takes every book down after the connection ends.
    fn take_all_down(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        for (sync, state) in self.syncs.iter_mut().zip(self.states.iter_mut()) {
            sync.reset();
            *state = SnapshotState::Idle;
        }
    }
}

/// `now + wait`, saturating far in the future instead of overflowing.
fn later(wait: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(wait)
        .unwrap_or_else(|| now.checked_add(Duration::from_secs(86_400)).unwrap_or(now))
}

fn index_of(id: InstrumentId) -> usize {
    usize::try_from(id.get()).unwrap_or(usize::MAX)
}

fn id_of(index: usize) -> InstrumentId {
    InstrumentId::new(u32::try_from(index).unwrap_or(u32::MAX))
}

/// Sleeps for `total`, waking every 50 ms to honor `stop`.
fn sleep_unless_stopped(total: Duration, stop: &AtomicBool) {
    let started = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let left = total.saturating_sub(started.elapsed());
        if left.is_zero() {
            return;
        }
        thread::sleep(left.min(Duration::from_millis(50)));
    }
}

/// Invalid session configuration.
#[derive(Debug, thiserror::Error)]
pub enum MdError {
    /// No instruments configured.
    #[error("no instruments configured")]
    NoInstruments,
    /// A configured size is zero.
    #[error("invalid book or buffer sizes")]
    InvalidSizes,
    /// The stream URL is invalid.
    #[error(transparent)]
    Url(#[from] NetError),
    /// The snapshot thread could not be started.
    #[error("could not start the snapshot thread")]
    Spawn,
}

struct SnapshotJob {
    instrument: Instrument,
    generation: u64,
}

struct SnapshotResult {
    instrument: InstrumentId,
    generation: u64,
    outcome: Result<OwnedSnapshot, SnapshotFailure>,
}

/// A decoded snapshot handed from the snapshot thread. Allocates, but only on the rare
/// resync path.
struct OwnedSnapshot {
    last_update_id: u64,
    bids: Vec<Level>,
    asks: Vec<Level>,
    fetched_at: WallTime,
}

struct SnapshotFailure {
    reason: String,
    retry_after: Option<Duration>,
}

/// Fetches snapshots on its own thread, within a request-weight budget.
struct SnapshotWorker {
    rest_url: String,
    limit: u32,
    timeout: Duration,
    tls: TlsConfig,
    weight_per_minute: u64,
}

impl SnapshotWorker {
    fn run(self, jobs: &Receiver<SnapshotJob>, results: &Sender<SnapshotResult>) {
        let clock = bowst_core::SystemClock::new();
        let now = || bowst_core::Clock::mono(&clock);
        let mut budget = TokenBucket::new(self.weight_per_minute, Duration::from_secs(60), now());
        let mut decoder = DepthSnapshotDecoder::new(usize::try_from(self.limit).unwrap_or(5_000));
        let (mut raw, mut body) = (Vec::new(), Vec::new());
        let mut paused_until: Option<Instant> = None;
        // Ends when the session is dropped and the job channel closes.
        while let Ok(job) = jobs.recv() {
            if let Some(until) = paused_until.take() {
                thread::sleep(until.saturating_duration_since(Instant::now()));
            }
            wait_for_budget(&mut budget, depth_weight(self.limit), now);
            let outcome = self.fetch(
                &job.instrument,
                &mut decoder,
                &mut raw,
                &mut body,
                &mut budget,
                now(),
            );
            if let Err(SnapshotFailure {
                retry_after: Some(wait),
                ..
            }) = &outcome
            {
                // Rate limited by the venue: pause every request, not just this instrument's.
                paused_until = Some(later(*wait));
            }
            let result = SnapshotResult {
                instrument: job.instrument.id,
                generation: job.generation,
                outcome,
            };
            if results.send(result).is_err() {
                return;
            }
        }
    }

    fn fetch(
        &self,
        instrument: &Instrument,
        decoder: &mut DepthSnapshotDecoder,
        raw: &mut Vec<u8>,
        body: &mut Vec<u8>,
        budget: &mut TokenBucket,
        now: MonoTime,
    ) -> Result<OwnedSnapshot, SnapshotFailure> {
        let failure = |reason: String| SnapshotFailure {
            reason,
            retry_after: None,
        };
        let url = depth_url(&self.rest_url, instrument.symbol, self.limit)
            .map_err(|e| failure(e.to_string()))?;
        let response = http::get(&url, &self.tls, self.timeout, 16 << 20, raw, body)
            .map_err(|e| failure(e.to_string()))?;
        if let Some(used) = response.used_weight_1m {
            budget.observe_used(u64::from(used), now);
        }
        require_ok(response).map_err(|e| match e {
            // 429: rate limited. 418: IP banned for ignoring 429s. Both carry Retry-After.
            RestError::Status {
                status: 418 | 429,
                retry_after_secs,
            } => SnapshotFailure {
                reason: e.to_string(),
                retry_after: Some(Duration::from_secs(u64::from(
                    retry_after_secs.unwrap_or(60),
                ))),
            },
            other => failure(other.to_string()),
        })?;
        let snapshot = decoder
            .decode(body, instrument)
            .map_err(|e| failure(format!("decode: {e}")))?;
        let fetched_at = bowst_core::Clock::wall(&bowst_core::SystemClock::new());
        Ok(OwnedSnapshot {
            last_update_id: snapshot.last_update_id,
            bids: snapshot.bids.to_vec(),
            asks: snapshot.asks.to_vec(),
            fetched_at,
        })
    }
}

/// Blocks the snapshot thread until the budget allows `weight`.
fn wait_for_budget(budget: &mut TokenBucket, weight: u64, now: impl Fn() -> MonoTime) {
    loop {
        match budget.try_take(weight, now()) {
            // A cost above capacity can never be met: proceed and let the venue decide, rather
            // than stalling resyncs forever.
            Ok(()) | Err(Duration::MAX) => return,
            Err(wait) => thread::sleep(wait),
        }
    }
}
