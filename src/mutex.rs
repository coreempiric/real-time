//! Priority-inheritance mutex (`PTHREAD_PRIO_INHERIT`).

use super::error::RtError;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Mutual exclusion with POSIX priority inheritance.
pub struct PrioInheritMutex<T> {
    #[cfg(target_os = "linux")]
    raw: libc::pthread_mutex_t,
    data: UnsafeCell<T>,
    #[cfg(not(target_os = "linux"))]
    _unsupported: (),
}

unsafe impl<T: Send> Send for PrioInheritMutex<T> {}
unsafe impl<T: Send> Sync for PrioInheritMutex<T> {}

/// RAII guard that unlocks a [`PrioInheritMutex`] on drop.
pub struct PrioInheritMutexGuard<'a, T> {
    mutex: &'a PrioInheritMutex<T>,
    _marker: PhantomData<*const ()>,
}

impl<T> PrioInheritMutex<T> {
    /**
     * Creates a mutex with `PTHREAD_PRIO_INHERIT` and stores `value`.
     *
     * # Errors
     *
     * - [`RtError::MutexOs`] — attr/mutex init failed  
     * - [`RtError::Unsupported`] — non-Linux  
     */
    pub fn new(value: T) -> Result<Self, RtError> {
        new_impl(value)
    }

    /**
     * Locks the mutex, blocking until acquired.
     *
     * # Errors
     *
     * - [`RtError::MutexOs`] — `pthread_mutex_lock` failed  
     * - [`RtError::Unsupported`] — non-Linux  
     */
    pub fn lock(&self) -> Result<PrioInheritMutexGuard<'_, T>, RtError> {
        lock_impl(self)?;

        Ok(PrioInheritMutexGuard {
            mutex: self,
            _marker: PhantomData,
        })
    }

    /**
     * Acquire the OS lock without an RAII guard (for [`super::api::RawPiMutex`]).
     *
     * Caller **must** pair with [`unlock_os`](Self::unlock_os). Prefer [`lock`](Self::lock)
     * for normal product use.
     */
    pub fn lock_os(&self) -> Result<(), RtError> {
        lock_impl(self)
    }

    /**
     * Release the OS lock after [`lock_os`](Self::lock_os).
     */
    pub fn unlock_os(&self) -> Result<(), RtError> {
        unlock_impl(self)
    }
}

impl<T> Drop for PrioInheritMutex<T> {
    fn drop(&mut self) {
        destroy_impl(self);
    }
}

impl<T> Deref for PrioInheritMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for PrioInheritMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Drop for PrioInheritMutexGuard<'_, T> {
    fn drop(&mut self) {
        let _ = unlock_impl(self.mutex);
    }
}

/// Which POSIX step to force-fail under test (covers OS error arms).
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum MutexFault {
    AttrInit,
    SetProtocol,
    MutexInit,
    Lock,
    Unlock,
}

#[cfg(test)]
thread_local! {
    static MUTEX_FAULT: std::cell::Cell<Option<MutexFault>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn take_mutex_fault(kind: MutexFault) -> bool {
    MUTEX_FAULT.with(|c| {
        if c.get() == Some(kind) {
            c.set(None);
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
fn inject_mutex_fault(kind: MutexFault) {
    MUTEX_FAULT.with(|c| c.set(Some(kind)));
}

#[cfg(target_os = "linux")]
fn new_impl<T>(value: T) -> Result<PrioInheritMutex<T>, RtError> {
    unsafe {
        let mut attr: libc::pthread_mutexattr_t = std::mem::zeroed();
        let attr_rc = libc::pthread_mutexattr_init(&mut attr);
        #[cfg(test)]
        let attr_rc = if take_mutex_fault(MutexFault::AttrInit) {
            -1
        } else {
            attr_rc
        };
        if attr_rc != 0 {
            return Err(RtError::MutexOs);
        }
        let proto_rc = libc::pthread_mutexattr_setprotocol(&mut attr, libc::PTHREAD_PRIO_INHERIT);
        #[cfg(test)]
        let proto_rc = if take_mutex_fault(MutexFault::SetProtocol) {
            -1
        } else {
            proto_rc
        };
        if proto_rc != 0 {
            libc::pthread_mutexattr_destroy(&mut attr);
            return Err(RtError::MutexOs);
        }
        let mut raw: libc::pthread_mutex_t = std::mem::zeroed();
        let rc = libc::pthread_mutex_init(&mut raw, &attr);
        libc::pthread_mutexattr_destroy(&mut attr);
        #[cfg(test)]
        let rc = if take_mutex_fault(MutexFault::MutexInit) {
            // Mutex was created; destroy before reporting failure.
            libc::pthread_mutex_destroy(&mut raw);
            -1
        } else {
            rc
        };
        if rc != 0 {
            return Err(RtError::MutexOs);
        }
        Ok(PrioInheritMutex {
            raw,
            data: UnsafeCell::new(value),
        })
    }
}

#[cfg(not(target_os = "linux"))]
fn new_impl<T>(_value: T) -> Result<PrioInheritMutex<T>, RtError> {
    Err(RtError::Unsupported)
}

#[cfg(target_os = "linux")]
fn lock_impl<T>(m: &PrioInheritMutex<T>) -> Result<(), RtError> {
    let rc = unsafe { libc::pthread_mutex_lock(&m.raw as *const _ as *mut _) };
    #[cfg(test)]
    let rc = if take_mutex_fault(MutexFault::Lock) {
        // Best-effort unlock so an injected failure does not leave the mutex held.
        let _ = unsafe { libc::pthread_mutex_unlock(&m.raw as *const _ as *mut _) };
        -1
    } else {
        rc
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(RtError::MutexOs)
    }
}

#[cfg(not(target_os = "linux"))]
fn lock_impl<T>(_m: &PrioInheritMutex<T>) -> Result<(), RtError> {
    Err(RtError::Unsupported)
}

#[cfg(target_os = "linux")]
fn unlock_impl<T>(m: &PrioInheritMutex<T>) -> Result<(), RtError> {
    let rc = unsafe { libc::pthread_mutex_unlock(&m.raw as *const _ as *mut _) };
    #[cfg(test)]
    let rc = if take_mutex_fault(MutexFault::Unlock) {
        // Prefer reporting failure after a successful unlock so the mutex is not left held.
        -1
    } else {
        rc
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(RtError::MutexOs)
    }
}

#[cfg(not(target_os = "linux"))]
fn unlock_impl<T>(_m: &PrioInheritMutex<T>) -> Result<(), RtError> {
    Err(RtError::Unsupported)
}

#[cfg(target_os = "linux")]
fn destroy_impl<T>(m: &mut PrioInheritMutex<T>) {
    unsafe {
        libc::pthread_mutex_destroy(&mut m.raw);
    }
}

#[cfg(not(target_os = "linux"))]
fn destroy_impl<T>(_m: &mut PrioInheritMutex<T>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    /***
     * @test-list
     * 1. Lock then unlock mutates the value.
     * 2. Concurrent increments reach the expected total.
     * 3. Attr init fault is MutexOs.
     * 4. Set protocol fault is MutexOs.
     * 5. Mutex init fault is MutexOs.
     * 6. Lock fault is MutexOs.
     * 7. Mutex is usable after an injected lock fault.
     * 8. Unlock fault on guard drop leaves the value intact.
     */

    #[test]
    fn lock_unlock_and_mutate() {
        let m = PrioInheritMutex::new(41u32).unwrap();
        {
            let mut g = m.lock().unwrap();
            *g += 1;
        }
        let value = *m.lock().unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn concurrent_increments() {
        let m = Arc::new(PrioInheritMutex::new(0u32).unwrap());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m2 = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut g = m2.lock().unwrap();
                    *g += 1;
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let value = *m.lock().unwrap();

        assert_eq!(value, 800);
    }

    #[test]
    fn attr_init_fault_is_mutex_os() {
        inject_mutex_fault(MutexFault::AttrInit);
        let result = PrioInheritMutex::new(0u32);

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn set_protocol_fault_is_mutex_os() {
        inject_mutex_fault(MutexFault::SetProtocol);
        let result = PrioInheritMutex::new(0u32);

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn mutex_init_fault_is_mutex_os() {
        inject_mutex_fault(MutexFault::MutexInit);
        let result = PrioInheritMutex::new(0u32);

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn lock_fault_is_mutex_os() {
        let m = PrioInheritMutex::new(0u32).unwrap();
        inject_mutex_fault(MutexFault::Lock);
        let result = m.lock();

        assert_eq!(result.err(), Some(RtError::MutexOs));
    }

    #[test]
    fn mutex_usable_after_injected_lock_fault() {
        let m = PrioInheritMutex::new(0u32).unwrap();
        inject_mutex_fault(MutexFault::Lock);
        let _ = m.lock();
        let value = *m.lock().unwrap();

        assert_eq!(value, 0);
    }

    #[test]
    fn unlock_fault_on_guard_drop_leaves_value() {
        let m = PrioInheritMutex::new(7u32).unwrap();
        {
            let g = m.lock().unwrap();
            inject_mutex_fault(MutexFault::Unlock);
            drop(g);
        }
        let value = *m.lock().unwrap();

        assert_eq!(value, 7);
    }
}
