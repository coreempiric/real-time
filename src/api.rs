//! @file api.rs
//!
//! @brief
//! Mockable seams for thread spawn, join, and PI mutex locks.
//!
//! @details
//! Application glue depends on [`ThreadSpawner`], [`JoinHandle`],
//! [`RawPiMutex`], and [`RawPiMutexFactory`]. Product adapters live in
//! [`super::live`]. Tests inject [`MockThreadSpawner`], [`MockJoinHandle`],
//! [`MockRawPiMutex`], and [`MockRawPiMutexFactory`]. Do not put rustdoc
//! examples on these traits; mockall copies them onto the `Mock*` types.
//!
//! @responsibilities
//! - Name the spawn and join contract.
//! - Name the PI mutex factory and lock contract.

use super::error::RtError;
use super::thread::ThreadConfig;

/// Boxed job for [`ThreadSpawner::spawn`].
pub type ThreadJob = Box<dyn FnOnce() + Send + 'static>;

/// Join handle from [`ThreadSpawner::spawn`].
///
/// @details
/// [`Self::join`] blocks until the thread finishes. A second call fails
/// closed. Do not put rustdoc examples on this trait.
#[cfg_attr(feature = "mocks", mockall::automock)]
pub trait JoinHandle: Send {
    /// @brief
    /// Block until the thread finishes.
    ///
    /// @return
    /// On success, returns `Ok(())`. On error, returns [`RtError::AlreadyJoined`] on a second call,
    /// or [`RtError::ThreadOs`].
    fn join(&mut self) -> Result<(), RtError>;
}

/// Spawns threads with optional policy and CPU affinity.
///
/// @details
/// Product [`super::live::LiveThreadSpawner`] applies `config` on the child.
/// Tests inject [`MockThreadSpawner`]. Do not put rustdoc examples on this
/// trait.
#[cfg_attr(feature = "mocks", mockall::automock)]
pub trait ThreadSpawner: Send + Sync {
    /// @brief
    /// Spawn `job` after applying `config` on the child.
    ///
    /// @return
    /// On success, returns `Ok(Box<dyn JoinHandle>)`. On error, returns [`RtError::InvalidConfig`]
    /// or [`RtError::ThreadOs`].
    fn spawn(&self, config: ThreadConfig, job: ThreadJob) -> Result<Box<dyn JoinHandle>, RtError>;
}

/// Priority-inheritance mutex OS lock (no user data).
///
/// @details
/// Tests inject [`MockRawPiMutex`]. Do not put rustdoc examples on this trait.
#[cfg_attr(feature = "mocks", mockall::automock)]
pub trait RawPiMutex: Send + Sync {
    /// @brief
    /// Acquire the lock (blocking).
    ///
    /// @return
    /// On success, returns `Ok(())`. On error, returns [`RtError::MutexOs`].
    fn lock(&self) -> Result<(), RtError>;

    /// @brief
    /// Release the lock.
    ///
    /// @return
    /// On success, returns `Ok(())`. On error, returns [`RtError::MutexOs`].
    fn unlock(&self) -> Result<(), RtError>;
}

/// Factory for [`RawPiMutex`] instances.
///
/// @details
/// Product [`super::live::LivePiMutexFactory`] builds a live PI mutex. Tests
/// inject [`MockRawPiMutexFactory`]. Do not put rustdoc examples on this trait.
#[cfg_attr(feature = "mocks", mockall::automock)]
pub trait RawPiMutexFactory: Send + Sync {
    /// @brief
    /// Create a new PI mutex.
    ///
    /// @return
    /// On success, returns `Ok(Box<dyn RawPiMutex>)`. On error, returns [`RtError::MutexOs`].
    fn create(&self) -> Result<Box<dyn RawPiMutex>, RtError>;
}

#[cfg(test)]
mod tests {
    /***
     * @test-list
     * 1. MockThreadSpawner runs the job and join succeeds.
     * 2. MockThreadSpawner forwards spawn failure after taking the job.
     * 3. MockJoinHandle forwards join failure.
     * 4. MockRawPiMutexFactory create then lock and unlock succeed.
     * 5. MockRawPiMutexFactory create fails closed.
     * 6. MockRawPiMutex lock fails closed.
     * 7. MockRawPiMutex unlock fails closed.
     */

    use super::*;
    use crate::SchedPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn other_config() -> ThreadConfig {
        ThreadConfig {
            name: "t".into(),
            policy: SchedPolicy::Other,
            priority: 0,
            cpu: None,
        }
    }

    #[test]
    fn mock_thread_spawner_controls_spawn_and_join() {
        let ran = Arc::new(AtomicBool::new(false));
        let ran_job = Arc::clone(&ran);
        let mut spawner = MockThreadSpawner::new();
        spawner.expect_spawn().times(1).returning(move |_, job| {
            job();
            let mut handle = MockJoinHandle::new();
            handle.expect_join().return_once(|| Ok(()));
            Ok(Box::new(handle))
        });
        let mut handle = spawner
            .spawn(
                other_config(),
                Box::new(move || ran_job.store(true, Ordering::SeqCst)),
            )
            .expect("spawn");
        let joined = handle.join();

        assert_eq!(joined, Ok(()));
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn mock_thread_spawner_can_fail_spawn() {
        let mut spawner = MockThreadSpawner::new();
        spawner.expect_spawn().return_once(|_, job| {
            job();
            Err(RtError::ThreadOs)
        });
        let result = spawner.spawn(other_config(), Box::new(|| {}));

        assert_eq!(result.err(), Some(RtError::ThreadOs));
    }

    #[test]
    fn mock_join_handle_can_fail() {
        let mut handle = MockJoinHandle::new();
        handle
            .expect_join()
            .return_once(|| Err(RtError::AlreadyJoined));
        let result = handle.join();

        assert_eq!(result.err(), Some(RtError::AlreadyJoined));
    }

    #[test]
    fn mock_pi_mutex_factory_create_lock_unlock_ok() {
        let mut factory = MockRawPiMutexFactory::new();
        factory.expect_create().times(1).returning(|| {
            let mut mutex = MockRawPiMutex::new();
            mutex.expect_lock().return_once(|| Ok(()));
            mutex.expect_unlock().return_once(|| Ok(()));
            Ok(Box::new(mutex))
        });
        let mutex = factory.create().expect("create");
        let locked = mutex.lock();
        let unlocked = mutex.unlock();

        assert_eq!(locked, Ok(()));
        assert_eq!(unlocked, Ok(()));
    }

    #[test]
    fn mock_pi_mutex_factory_create_fails() {
        let mut factory = MockRawPiMutexFactory::new();
        factory
            .expect_create()
            .return_once(|| Err(RtError::MutexOs));
        let result = factory.create();

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn mock_pi_mutex_lock_fails() {
        let mut mutex = MockRawPiMutex::new();
        mutex.expect_lock().return_once(|| Err(RtError::MutexOs));
        let result = mutex.lock();

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn mock_pi_mutex_unlock_fails() {
        let mut mutex = MockRawPiMutex::new();
        mutex.expect_unlock().return_once(|| Err(RtError::MutexOs));
        let result = mutex.unlock();

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }
}
