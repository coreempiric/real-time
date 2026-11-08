//! @file thread.rs
//!
//! @brief
//! Realtime thread config, spawn sequencing, and join.
//!
//! @details
//! [`RealtimeThread::spawn`] validates [`ThreadConfig`], then
//! [`RealtimeThread::from_os`] sequences CPU check, named spawn, apply, and
//! ready handshake through a [`ThreadOs`]. Product passes the live OS from
//! [`super::live`]. Tests inject [`ScriptedOs`] so spawn, apply, and ready
//! failures run without pthread faults.
//!
//! @responsibilities
//! - Validate name, policy, priority, and CPU index.
//! - Spawn a child, apply params on that child, then run the job.
//! - Fail closed on a second join. Drop joins if still live.

use super::error::RtError;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

/// Linux pthread name length limit excluding NUL.
pub const MAX_THREAD_NAME_LEN: usize = 15;

/// Boxed job passed across the OS seam.
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

/// Scheduling policy for a realtime thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchedPolicy {
    /// Normal time-sharing (`SCHED_OTHER`). Priority must be 0.
    #[default]
    Other,
    /// FIFO realtime (`SCHED_FIFO`).
    Fifo,
    /// Round-robin realtime (`SCHED_RR`).
    RoundRobin,
}

/// Configuration for [`RealtimeThread::spawn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadConfig {
    /// Thread name (truncated for the OS; empty is invalid).
    pub name: String,
    /// Scheduling policy.
    pub policy: SchedPolicy,
    /// Priority (0 for [`SchedPolicy::Other`]; within OS min/max for RT policies).
    pub priority: i32,
    /// Optional CPU core index to pin (`None` = no affinity request).
    pub cpu: Option<usize>,
}

impl ThreadConfig {
    /// Validate name, priority, and optional core index (fail closed).
    ///
    /// Does not ask the OS whether FIFO is allowed. That is checked at spawn.
    pub fn validate(&self) -> Result<(), RtError> {
        if self.name.is_empty() {
            return Err(RtError::InvalidConfig);
        }
        if self.name.len() > MAX_THREAD_NAME_LEN {
            return Err(RtError::InvalidConfig);
        }
        if self.name.contains('\0') {
            return Err(RtError::InvalidConfig);
        }
        match self.policy {
            SchedPolicy::Other => {
                if self.priority != 0 {
                    return Err(RtError::InvalidConfig);
                }
            }
            SchedPolicy::Fifo => {
                if self.priority < 1 {
                    return Err(RtError::InvalidConfig);
                }
            }
            SchedPolicy::RoundRobin => {
                if self.priority < 1 {
                    return Err(RtError::InvalidConfig);
                }
            }
        }
        if let Some(cpu) = self.cpu {
            if cpu >= 1024 {
                return Err(RtError::InvalidConfig);
            }
        }

        Ok(())
    }
}

/// OS steps [`RealtimeThread::from_os`] sequences. Product uses the live
/// adapter. Tests inject a double.
pub(crate) trait ThreadOs: Send + Sync {
    fn spawn_named(&self, name: String, body: Job) -> Result<JoinHandle<()>, RtError>;
    fn set_affinity(&self, cpu: usize) -> Result<(), RtError>;
    fn set_sched(&self, policy: SchedPolicy, priority: i32) -> Result<(), RtError>;
    fn cpu_count(&self) -> usize;
}

/// Join handle for a thread with RT attributes applied at start.
#[derive(Debug)]
pub struct RealtimeThread {
    handle: Option<JoinHandle<()>>,
}

impl RealtimeThread {
    /// Spawn `f` on a new OS thread, then apply policy / priority / affinity.
    ///
    /// Application of RT parameters happens on the child thread before `f`
    /// runs. If application fails, `f` is not run and this function returns
    /// [`RtError::ThreadOs`]. The child is still joined internally in that
    /// case.
    ///
    /// @return
    /// On success, returns `Ok(Self)`. On error, returns [`RtError::InvalidConfig`],
    /// [`RtError::ThreadOs`], or [`RtError::Unsupported`] on non-Linux.
    pub fn spawn<F>(config: ThreadConfig, f: F) -> Result<Self, RtError>
    where
        F: FnOnce() + Send + 'static,
    {
        Self::spawn_job(config, Box::new(f))
    }

    /// Adopt an already-boxed job through the live OS.
    pub(crate) fn spawn_job(config: ThreadConfig, job: Job) -> Result<Self, RtError> {
        Self::from_os(super::live::live_thread_os(), config, job)
    }

    /// Validate `config` and sequence spawn through `os`.
    pub(crate) fn from_os(
        os: Arc<dyn ThreadOs>,
        config: ThreadConfig,
        job: Job,
    ) -> Result<Self, RtError> {
        config.validate()?;

        spawn_with(os, config, job)
    }

    /// Wait for the thread to finish. A second call fails closed.
    ///
    /// @return
    /// On success, returns `Ok(())`. On error, returns [`RtError::AlreadyJoined`] or
    /// [`RtError::ThreadOs`] if the thread panicked.
    pub fn join(&mut self) -> Result<(), RtError> {
        match self.handle.take() {
            Some(handle) => join_result(handle.join()),
            None => Err(RtError::AlreadyJoined),
        }
    }
}

impl Drop for RealtimeThread {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let _ = handle.join();
    }
}

fn spawn_with(
    os: Arc<dyn ThreadOs>,
    config: ThreadConfig,
    job: Job,
) -> Result<RealtimeThread, RtError> {
    if let Some(cpu) = config.cpu {
        if cpu >= os.cpu_count() {
            return Err(RtError::InvalidConfig);
        }
    }
    let (tx, rx) = mpsc::sync_channel::<Result<(), RtError>>(1);
    let cfg = config.clone();
    let os_child = Arc::clone(&os);
    let name = truncate_name(&config.name);
    let handle = os.spawn_named(
        name,
        Box::new(move || {
            child_run_body(os_child, cfg, job, tx);
        }),
    )?;

    match rx.recv() {
        Ok(Ok(())) => Ok(RealtimeThread {
            handle: Some(handle),
        }),
        Ok(Err(e)) => {
            let _ = handle.join();
            Err(e)
        }
        Err(_) => {
            let _ = handle.join();
            Err(RtError::ThreadOs)
        }
    }
}

fn child_run_body(
    os: Arc<dyn ThreadOs>,
    config: ThreadConfig,
    job: Job,
    tx: mpsc::SyncSender<Result<(), RtError>>,
) {
    let apply = apply_config(os.as_ref(), &config);
    let _ = tx.send(apply);

    if apply.is_ok() {
        job();
    }
}

fn apply_config(os: &dyn ThreadOs, config: &ThreadConfig) -> Result<(), RtError> {
    if let Some(cpu) = config.cpu {
        os.set_affinity(cpu)?;
    }

    os.set_sched(config.policy, config.priority)
}

fn join_result(result: thread::Result<()>) -> Result<(), RtError> {
    match result {
        Ok(()) => Ok(()),
        Err(_) => Err(RtError::ThreadOs),
    }
}

fn truncate_name(name: &str) -> String {
    name.chars().take(MAX_THREAD_NAME_LEN).collect()
}

/// Map [`SchedPolicy`] to a libc policy constant.
#[cfg(target_os = "linux")]
pub(crate) fn sched_policy_num(policy: SchedPolicy) -> libc::c_int {
    match policy {
        SchedPolicy::Other => libc::SCHED_OTHER,
        SchedPolicy::Fifo => libc::SCHED_FIFO,
        SchedPolicy::RoundRobin => libc::SCHED_RR,
    }
}

/// Map `available_parallelism` at the std boundary.
pub(crate) fn cpu_count_from_os(result: Result<NonZeroUsize, std::io::Error>) -> usize {
    match result {
        Ok(n) => n.get(),
        Err(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    /***
     * @test-list
     * 1. Validate rejects empty name.
     * 2. Validate rejects Other with nonzero priority.
     * 3. Validate rejects a name longer than the pthread limit.
     * 4. Validate rejects a name with NUL.
     * 5. Validate rejects Fifo with priority 0.
     * 6. Validate rejects a CPU index at or above 1024.
     * 7. Validate accepts Other with priority 0.
     * 8. Validate rejects RoundRobin with priority 0.
     * 9. Validate accepts RoundRobin with a positive priority.
     * 10. SchedPolicy default is Other.
     * 11. ThreadConfig clone equals the original.
     * 12. from_os maps spawn failure.
     * 13. from_os maps affinity failure.
     * 14. from_os maps sched failure.
     * 15. from_os maps CPU out of range.
     * 16. from_os maps a dropped ready channel.
     * 17. from_os maps panic before ready.
     * 18. from_os runs the job and join succeeds.
     * 19. A second join is already joined.
     * 20. Drop joins a live handle.
     * 21. spawn rejects invalid config before the OS.
     * 22. join_result maps Ok.
     * 23. join_result maps panic to ThreadOs.
     * 24. cpu_count_from_os maps OS failure to 1.
     * 25. cpu_count_from_os keeps a nonzero count.
     * 26. truncate_name caps at the pthread limit.
     * 27. truncate_name keeps a short name.
     * 28. child_run_body skips the job when apply fails.
     * 29. sched_policy_num maps Other.
     * 30. sched_policy_num maps Fifo.
     * 31. sched_policy_num maps RoundRobin.
     */

    use super::*;

    fn noop() {}

    fn other_cfg(name: &str, cpu: Option<usize>) -> ThreadConfig {
        ThreadConfig {
            name: name.into(),
            policy: SchedPolicy::Other,
            priority: 0,
            cpu,
        }
    }

    struct ScriptedOs {
        spawn: Result<(), RtError>,
        affinity: Result<(), RtError>,
        sched: Result<(), RtError>,
        cpus: usize,
        drop_ready: bool,
        panic_before: bool,
    }

    impl Default for ScriptedOs {
        fn default() -> Self {
            Self {
                spawn: Ok(()),
                affinity: Ok(()),
                sched: Ok(()),
                cpus: 8,
                drop_ready: false,
                panic_before: false,
            }
        }
    }

    impl ThreadOs for ScriptedOs {
        fn spawn_named(&self, name: String, body: Job) -> Result<JoinHandle<()>, RtError> {
            if self.spawn.is_err() {
                return Err(RtError::ThreadOs);
            }
            if self.drop_ready {
                return Ok(thread::Builder::new()
                    .name(name)
                    .spawn(|| {})
                    .expect("drop-ready thread"));
            }
            if self.panic_before {
                return Ok(thread::Builder::new()
                    .name(name)
                    .spawn(|| panic!("rt test inject: panic before ready"))
                    .expect("panic-before thread"));
            }

            Ok(thread::Builder::new()
                .name(name)
                .spawn(move || body())
                .expect("scripted thread"))
        }

        fn set_affinity(&self, _cpu: usize) -> Result<(), RtError> {
            self.affinity
        }

        fn set_sched(&self, _policy: SchedPolicy, _priority: i32) -> Result<(), RtError> {
            self.sched
        }

        fn cpu_count(&self) -> usize {
            self.cpus
        }
    }

    fn from_scripted(
        os: ScriptedOs,
        config: ThreadConfig,
        job: Job,
    ) -> Result<RealtimeThread, RtError> {
        RealtimeThread::from_os(Arc::new(os), config, job)
    }

    fn other_ok() -> ThreadConfig {
        other_cfg("ok", None)
    }

    #[test]
    fn validate_rejects_empty_name() {
        let cfg = ThreadConfig {
            name: String::new(),
            policy: SchedPolicy::Other,
            priority: 0,
            cpu: None,
        };
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_rejects_other_with_nonzero_priority() {
        let mut cfg = other_ok();
        cfg.priority = 1;
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_rejects_name_longer_than_limit() {
        let mut cfg = other_ok();
        cfg.name = "a".repeat(MAX_THREAD_NAME_LEN + 1);
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_rejects_name_with_nul() {
        let mut cfg = other_ok();
        cfg.name = "has\0nul".into();
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_rejects_fifo_priority_zero() {
        let mut cfg = other_ok();
        cfg.policy = SchedPolicy::Fifo;
        cfg.priority = 0;
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_rejects_cpu_at_or_above_1024() {
        let mut cfg = other_ok();
        cfg.cpu = Some(1024);
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_accepts_other_with_priority_zero() {
        let cfg = other_ok();
        let result = cfg.validate();

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn validate_rejects_round_robin_priority_zero() {
        let mut cfg = other_ok();
        cfg.policy = SchedPolicy::RoundRobin;
        cfg.priority = 0;
        let result = cfg.validate();

        assert_eq!(result, Err(RtError::InvalidConfig));
    }

    #[test]
    fn validate_accepts_round_robin_with_positive_priority() {
        let mut cfg = other_ok();
        cfg.policy = SchedPolicy::RoundRobin;
        cfg.priority = 5;
        let result = cfg.validate();

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn sched_policy_default_is_other() {
        let policy = SchedPolicy::default();

        assert_eq!(policy, SchedPolicy::Other);
    }

    #[test]
    fn thread_config_clone_equals_original() {
        let cfg = other_ok();
        let cloned = cfg.clone();

        assert_eq!(cloned, cfg);
    }

    #[test]
    fn from_os_maps_spawn_failure() {
        let result = from_scripted(
            ScriptedOs {
                spawn: Err(RtError::ThreadOs),
                ..ScriptedOs::default()
            },
            other_cfg("rt-sp", None),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn from_os_maps_affinity_failure() {
        let result = from_scripted(
            ScriptedOs {
                affinity: Err(RtError::ThreadOs),
                ..ScriptedOs::default()
            },
            other_cfg("rt-aff", Some(0)),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn from_os_maps_sched_failure() {
        let result = from_scripted(
            ScriptedOs {
                sched: Err(RtError::ThreadOs),
                ..ScriptedOs::default()
            },
            other_cfg("rt-sched", None),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn from_os_maps_cpu_out_of_range() {
        let result = from_scripted(
            ScriptedOs {
                cpus: 1,
                ..ScriptedOs::default()
            },
            other_cfg("rt-cpu", Some(2)),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::InvalidConfig));
    }

    #[test]
    fn from_os_maps_dropped_ready_channel() {
        let result = from_scripted(
            ScriptedOs {
                drop_ready: true,
                ..ScriptedOs::default()
            },
            other_cfg("rt-dropch", None),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn from_os_maps_panic_before_ready() {
        let result = from_scripted(
            ScriptedOs {
                panic_before: true,
                ..ScriptedOs::default()
            },
            other_cfg("rt-pbr", None),
            Box::new(noop),
        );

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn from_os_runs_job_and_join_succeeds() {
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = Arc::clone(&ran);
        let mut t = from_scripted(
            ScriptedOs::default(),
            other_cfg("rt-ok", Some(0)),
            Box::new(move || ran2.store(true, std::sync::atomic::Ordering::SeqCst)),
        )
        .expect("scripted spawn");
        let joined = t.join();

        assert_eq!(joined, Ok(()));
        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn from_os_second_join_is_already_joined() {
        let mut t = from_scripted(
            ScriptedOs::default(),
            other_cfg("rt-2nd", None),
            Box::new(noop),
        )
        .expect("scripted spawn");
        t.join().expect("join");
        let second = t.join();

        assert_eq!(second.err(), Some(RtError::AlreadyJoined));
    }

    #[test]
    fn from_os_drop_joins_live_handle() {
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = Arc::clone(&ran);
        let t = from_scripted(
            ScriptedOs::default(),
            other_cfg("rt-drop", None),
            Box::new(move || ran2.store(true, std::sync::atomic::Ordering::SeqCst)),
        )
        .expect("scripted drop");
        drop(t);

        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn spawn_rejects_invalid_config() {
        let result = RealtimeThread::spawn(
            ThreadConfig {
                name: String::new(),
                policy: SchedPolicy::Other,
                priority: 0,
                cpu: None,
            },
            noop,
        );

        assert_eq!(result.err(), Some(RtError::InvalidConfig));
    }

    #[test]
    fn join_result_maps_ok() {
        let result = join_result(Ok(()));

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn join_result_maps_panic_to_thread_os() {
        let result = join_result(Err(Box::new(())));

        assert_eq!(result, Err(RtError::ThreadOs));
    }

    #[test]
    fn cpu_count_from_os_maps_failure_to_one() {
        let result = cpu_count_from_os(Err(std::io::Error::other("no cpus")));

        assert_eq!(result, 1);
    }

    #[test]
    fn cpu_count_from_os_keeps_nonzero_count() {
        let result = cpu_count_from_os(Ok(NonZeroUsize::new(4).expect("4")));

        assert_eq!(result, 4);
    }

    #[test]
    fn truncate_name_caps_len() {
        let long = "x".repeat(MAX_THREAD_NAME_LEN + 5);
        let truncated = truncate_name(&long);

        assert_eq!(truncated.len(), MAX_THREAD_NAME_LEN);
    }

    #[test]
    fn truncate_name_keeps_short_name() {
        let truncated = truncate_name("short");

        assert_eq!(truncated, "short");
    }

    #[test]
    fn child_run_body_skips_job_when_apply_fails() {
        let (tx, rx) = mpsc::sync_channel(1);
        let os: Arc<dyn ThreadOs> = Arc::new(ScriptedOs {
            sched: Err(RtError::ThreadOs),
            ..ScriptedOs::default()
        });
        child_run_body(os, other_cfg("child-fail", None), Box::new(noop), tx);
        let result = rx.recv().expect("recv");

        assert_eq!(result, Err(RtError::ThreadOs));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sched_policy_num_maps_other() {
        let num = sched_policy_num(SchedPolicy::Other);

        assert_eq!(num, libc::SCHED_OTHER);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sched_policy_num_maps_fifo() {
        let num = sched_policy_num(SchedPolicy::Fifo);

        assert_eq!(num, libc::SCHED_FIFO);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sched_policy_num_maps_round_robin() {
        let num = sched_policy_num(SchedPolicy::RoundRobin);

        assert_eq!(num, libc::SCHED_RR);
    }
}
