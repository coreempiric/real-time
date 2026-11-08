#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Realtime OS helpers: thread placement and priority-inheritance mutexes.
//!
//! # How to use
//!
//! Prefer trait seams so tests inject mocks:
//!
//! | Need | Type |
//! |------|------|
//! | Spawn with affinity / policy | [`ThreadSpawner`] / [`LiveThreadSpawner`] |
//! | Join handle | [`JoinHandle`] |
//! | PI mutex around data | [`PrioInheritMutex`] |
//! | Mockable PI lock | [`RawPiMutex`] / [`RawPiMutexFactory`] |
//!
//! Timing stays in `std` (`Instant`, `sleep`). Messaging stays in `std::sync::mpsc`.
//!
//! ```
//! use rt::{RealtimeThread, SchedPolicy, ThreadConfig};
//!
//! let mut t = RealtimeThread::spawn(
//!     ThreadConfig {
//!         name: "worker".into(),
//!         policy: SchedPolicy::Other,
//!         priority: 0,
//!         cpu: None,
//!     },
//!     || { /* work */ },
//! ).unwrap();
//! t.join().unwrap();
//! ```

mod api;
mod error;
mod live;
mod mutex;
mod thread;

pub use api::{JoinHandle, RawPiMutex, RawPiMutexFactory, ThreadJob, ThreadSpawner};
pub use error::RtError;
pub use live::{LiveJoinHandle, LivePiMutexFactory, LiveThreadSpawner};
pub use mutex::{PrioInheritMutex, PrioInheritMutexGuard};
pub use thread::{MAX_THREAD_NAME_LEN, RealtimeThread, SchedPolicy, ThreadConfig};

#[cfg(feature = "mocks")]
pub use api::{MockJoinHandle, MockRawPiMutex, MockRawPiMutexFactory, MockThreadSpawner};
