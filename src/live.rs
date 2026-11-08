//! @file live.rs
//!
//! @brief
//! Production RT adapters for [`ThreadSpawner`] and [`RawPiMutexFactory`].
//!
//! @details
//! [`LiveThreadSpawner`] adopts a [`RealtimeThread`] into [`LiveJoinHandle`].
//! [`LivePiMutexFactory`] adopts a [`PrioInheritMutex`] into [`LiveRawPiMutex`].
//! Product calls the trait methods. Tests call [`LiveThreadSpawner::adopt`]
//! and [`LivePiMutexFactory::adopt`] with a scripted `Result` so spawn and
//! mutex-create failure run without an OS fault.
//!
//! @responsibilities
//! - Spawn a realtime thread and box the join handle.
//! - Create a PI mutex and box the raw lock.
//! - Fail closed on a second join; drop joins if still live.

use super::api::{JoinHandle, RawPiMutex, RawPiMutexFactory, ThreadJob, ThreadSpawner};
use super::error::RtError;
use super::mutex::PrioInheritMutex;
use super::thread::{Job, RealtimeThread, ThreadConfig, ThreadOs};
#[cfg(target_os = "linux")]
use super::thread::{SchedPolicy, cpu_count_from_os, sched_policy_num};
use std::mem::size_of_val;
use std::sync::Arc;
use std::thread::{self, JoinHandle as StdJoinHandle};

/// Spawns real [`RealtimeThread`]s.
#[derive(Debug, Default, Clone, Copy)]
pub struct LiveThreadSpawner;

impl LiveThreadSpawner {
    /// Adopt a spawn result as the boxed [`JoinHandle`].
    fn adopt(thread: Result<RealtimeThread, RtError>) -> Result<Box<dyn JoinHandle>, RtError> {
        match thread {
            Ok(t) => Ok(Box::new(LiveJoinHandle { inner: Some(t) })),
            Err(err) => Err(err),
        }
    }
}

impl ThreadSpawner for LiveThreadSpawner {
    fn spawn(&self, config: ThreadConfig, job: ThreadJob) -> Result<Box<dyn JoinHandle>, RtError> {
        Self::adopt(RealtimeThread::spawn_job(config, job))
    }
}

/// Live pthread / std-thread OS used by [`RealtimeThread::spawn`].
pub(crate) struct LiveThreadOs;

pub(crate) fn live_thread_os() -> Arc<dyn ThreadOs> {
    Arc::new(LiveThreadOs)
}

/// Map a libc return code. Zero succeeds; any other value is thread OS failure.
fn map_libc_rc(rc: i32) -> Result<(), RtError> {
    if rc != 0 {
        return Err(RtError::ThreadOs);
    }

    Ok(())
}

/// Map `thread::Builder::spawn`.
fn map_io_spawn(result: std::io::Result<StdJoinHandle<()>>) -> Result<StdJoinHandle<()>, RtError> {
    match result {
        Ok(handle) => Ok(handle),
        Err(_) => Err(RtError::ThreadOs),
    }
}

#[cfg(target_os = "linux")]
impl ThreadOs for LiveThreadOs {
    fn spawn_named(&self, name: String, body: Job) -> Result<StdJoinHandle<()>, RtError> {
        map_io_spawn(thread::Builder::new().name(name).spawn(move || body()))
    }

    fn set_affinity(&self, cpu: usize) -> Result<(), RtError> {
        use libc::{CPU_SET, CPU_ZERO, cpu_set_t, pthread_self, pthread_setaffinity_np};
        let rc = unsafe {
            let mut set: cpu_set_t = std::mem::zeroed();
            CPU_ZERO(&mut set);
            CPU_SET(cpu, &mut set);
            pthread_setaffinity_np(pthread_self(), size_of_val(&set), &set)
        };

        map_libc_rc(rc)
    }

    fn set_sched(&self, policy: SchedPolicy, priority: i32) -> Result<(), RtError> {
        use libc::{pthread_self, pthread_setschedparam, sched_param};
        let param = sched_param {
            sched_priority: priority,
        };
        let rc = unsafe { pthread_setschedparam(pthread_self(), sched_policy_num(policy), &param) };

        map_libc_rc(rc)
    }

    fn cpu_count(&self) -> usize {
        cpu_count_from_os(thread::available_parallelism())
    }
}

/// Join handle wrapping [`RealtimeThread`].
pub struct LiveJoinHandle {
    inner: Option<RealtimeThread>,
}

impl JoinHandle for LiveJoinHandle {
    fn join(&mut self) -> Result<(), RtError> {
        match self.inner.take() {
            Some(mut t) => t.join(),
            None => Err(RtError::AlreadyJoined),
        }
    }
}

impl Drop for LiveJoinHandle {
    fn drop(&mut self) {
        let Some(t) = self.inner.take() else {
            return;
        };
        drop(t);
    }
}

/// Creates real pthread PI mutexes.
#[derive(Debug, Default, Clone, Copy)]
pub struct LivePiMutexFactory;

impl LivePiMutexFactory {
    /// Adopt a mutex-create result as the boxed [`RawPiMutex`].
    fn adopt(inner: Result<PrioInheritMutex<()>, RtError>) -> Result<Box<dyn RawPiMutex>, RtError> {
        match inner {
            Ok(m) => Ok(Box::new(LiveRawPiMutex { inner: m })),
            Err(err) => Err(err),
        }
    }
}

impl RawPiMutexFactory for LivePiMutexFactory {
    fn create(&self) -> Result<Box<dyn RawPiMutex>, RtError> {
        Self::adopt(PrioInheritMutex::new(()))
    }
}

struct LiveRawPiMutex {
    inner: PrioInheritMutex<()>,
}

impl RawPiMutex for LiveRawPiMutex {
    fn lock(&self) -> Result<(), RtError> {
        self.inner.lock_os()
    }

    fn unlock(&self) -> Result<(), RtError> {
        self.inner.unlock_os()
    }
}

#[cfg(test)]
mod tests {
    /***
     * @test-list
     * 1. Drop joins when the handle is still live.
     * 2. Spawn runs the job and join succeeds.
     * 3. A second join is already joined.
     * 4. Adopt maps spawn ThreadOs.
     * 5. Adopt maps spawn InvalidConfig.
     * 6. Adopt maps mutex MutexOs.
     * 7. LiveThreadSpawner debug names the type.
     * 8. LivePiMutexFactory debug names the type.
     * 9. Factory create then lock and unlock succeed.
     * 10. map_libc_rc zero is ok.
     * 11. map_libc_rc negative is ThreadOs.
     * 12. map_libc_rc positive is ThreadOs.
     * 13. map_io_spawn error is ThreadOs.
     */

    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn rt_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("rt test lock")
    }
    use crate::SchedPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn other_config(name: &str) -> ThreadConfig {
        ThreadConfig {
            name: name.into(),
            policy: SchedPolicy::Other,
            priority: 0,
            cpu: None,
        }
    }

    #[test]
    fn live_join_handle_drop_joins_without_explicit_join() {
        let _g = rt_test_lock();
        let flag = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&flag);
        let spawner = LiveThreadSpawner;
        let handle = spawner
            .spawn(
                other_config("rt-drop"),
                Box::new(move || f2.store(true, Ordering::SeqCst)),
            )
            .expect("spawn");
        drop(handle);

        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn live_thread_spawner_runs_job() {
        let _g = rt_test_lock();
        let flag = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&flag);
        let spawner = LiveThreadSpawner::default();
        let mut pinned = other_config("rt-live");
        pinned.cpu = Some(0);
        let mut handle = spawner
            .spawn(pinned, Box::new(move || f2.store(true, Ordering::SeqCst)))
            .expect("spawn");
        let joined = handle.join();

        assert_eq!(joined, Ok(()));
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn live_thread_spawner_second_join_is_already_joined() {
        let _g = rt_test_lock();
        let spawner = LiveThreadSpawner::default();
        let mut handle = spawner
            .spawn(other_config("rt-2nd"), Box::new(|| {}))
            .expect("spawn");
        handle.join().expect("join");
        let second = handle.join();

        assert_eq!(second.err(), Some(RtError::AlreadyJoined));
    }

    #[test]
    fn adopt_maps_spawn_thread_os() {
        let result = LiveThreadSpawner::adopt(Err(RtError::ThreadOs));

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn adopt_maps_spawn_invalid_config() {
        let result = LiveThreadSpawner::adopt(Err(RtError::InvalidConfig));

        assert_eq!(result.err(), Some(RtError::InvalidConfig));
    }

    #[test]
    fn adopt_maps_mutex_os() {
        let result = LivePiMutexFactory::adopt(Err(RtError::MutexOs));

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn live_thread_spawner_debug_names_the_type() {
        let debug = format!("{:?}", LiveThreadSpawner);

        assert_eq!(debug, "LiveThreadSpawner");
    }

    #[test]
    fn live_pi_mutex_factory_debug_names_the_type() {
        let debug = format!("{:?}", LivePiMutexFactory);

        assert_eq!(debug, "LivePiMutexFactory");
    }

    #[test]
    fn live_pi_factory_lock_unlock() {
        let factory = LivePiMutexFactory;
        let mutex = factory.create().expect("pi mutex");
        let locked = mutex.lock();
        let unlocked = mutex.unlock();

        assert_eq!(locked, Ok(()));
        assert_eq!(unlocked, Ok(()));
    }

    #[test]
    fn map_libc_rc_zero_is_ok() {
        let result = map_libc_rc(0);

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn map_libc_rc_negative_is_thread_os() {
        let result = map_libc_rc(-1);

        assert_eq!(result, Err(RtError::ThreadOs));
    }

    #[test]
    fn map_libc_rc_positive_is_thread_os() {
        let result = map_libc_rc(1);

        assert_eq!(result, Err(RtError::ThreadOs));
    }

    #[test]
    fn map_io_spawn_error_is_thread_os() {
        let result = map_io_spawn(Err(std::io::Error::other("spawn")));

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }
}
