//! Errors from realtime OS wrappers.

use std::fmt;

/// Failures from thread or PI-mutex construction and operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtError {
    /// Configuration failed validation.
    InvalidConfig = 1,
    /// OS rejected thread attributes, spawn, affinity, or scheduling.
    ThreadOs = 2,
    /// OS rejected mutex attribute or lock/unlock.
    MutexOs = 3,
    /// Operation is not supported on this platform.
    Unsupported = 4,
    /// Thread already joined or handle consumed.
    AlreadyJoined = 5,
}

impl RtError {
    /// Stable numeric code for diagnostics.
    pub fn code(self) -> u32 {
        self as u32
    }
}

impl fmt::Display for RtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RtError::InvalidConfig => write!(f, "rt invalid config"),
            RtError::ThreadOs => write!(f, "rt thread os error"),
            RtError::MutexOs => write!(f, "rt mutex os error"),
            RtError::Unsupported => write!(f, "rt unsupported on this platform"),
            RtError::AlreadyJoined => write!(f, "rt thread already joined"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /***
     * @test-list
     * 1. InvalidConfig code.
     * 2. ThreadOs code.
     * 3. MutexOs code.
     * 4. Unsupported code.
     * 5. AlreadyJoined code.
     * 6. InvalidConfig display.
     * 7. ThreadOs display.
     * 8. MutexOs display.
     * 9. Unsupported display.
     * 10. AlreadyJoined display.
     */

    #[test]
    fn invalid_config_code() {
        let err = RtError::InvalidConfig;

        assert_eq!(err, RtError::InvalidConfig);
        assert_eq!(err.code(), 1);
    }

    #[test]
    fn thread_os_code() {
        let err = RtError::ThreadOs;

        assert_eq!(err, RtError::ThreadOs);
        assert_eq!(err.code(), 2);
    }

    #[test]
    fn mutex_os_code() {
        let err = RtError::MutexOs;

        assert_eq!(err, RtError::MutexOs);
        assert_eq!(err.code(), 3);
    }

    #[test]
    fn unsupported_code() {
        let err = RtError::Unsupported;

        assert_eq!(err, RtError::Unsupported);
        assert_eq!(err.code(), 4);
    }

    #[test]
    fn already_joined_code() {
        let err = RtError::AlreadyJoined;

        assert_eq!(err, RtError::AlreadyJoined);
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn invalid_config_display() {
        let err = RtError::InvalidConfig;

        assert_eq!(err.to_string(), "rt invalid config");
    }

    #[test]
    fn thread_os_display() {
        let err = RtError::ThreadOs;

        assert_eq!(err.to_string(), "rt thread os error");
    }

    #[test]
    fn mutex_os_display() {
        let err = RtError::MutexOs;

        assert_eq!(err.to_string(), "rt mutex os error");
    }

    #[test]
    fn unsupported_display() {
        let err = RtError::Unsupported;

        assert_eq!(err.to_string(), "rt unsupported on this platform");
    }

    #[test]
    fn already_joined_display() {
        let err = RtError::AlreadyJoined;

        assert_eq!(err.to_string(), "rt thread already joined");
    }
}
