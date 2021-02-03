use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::task::JoinHandle;

#[macro_export]
macro_rules! spawn_limit {
    ($limit:expr) => {{
        static SPAWN_LIMIT: $crate::SpawnLimit = $crate::SpawnLimit::new($limit);
        &SPAWN_LIMIT
    }};
}

#[derive(Debug)]
/// Set a static limit on the amount of tasks that
/// can be spawned from a section of code.
///
/// By default [`Ordering::Relaxed`] is used so it's
/// possible for the limit to be breach slightly.
/// If you require tighter control you can set the
/// atomic ordering via [`SpawnLimit::with_ordering`].
pub struct SpawnLimit {
    limit: usize,
    ordering: Ordering,
    current: AtomicUsize,
}

#[derive(Debug)]
pub struct SpawnGuard {
    limit: Option<&'static SpawnLimit>,
}

/// Spawn tasks up to a limit.
/// If the limit is passed then await the task in place.
pub async fn spawn_with_limit<T>(limit: &'static SpawnLimit, task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    if limit.try_add_task() {
        tokio::task::spawn(async move {
            let o = task.await;
            limit.task_finished();
            o
        })
    } else {
        let out = task.await;
        tokio::task::spawn(async move { out })
    }
}

/// Try to spawn a task if the limit has not been reached.
/// If the task successfully spawned then the returned task will
/// contain an option [`JoinHandle`].
pub fn spawn_try_limit<T>(limit: &'static SpawnLimit, task: T) -> Result<JoinHandle<T::Output>, T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    if limit.try_add_task() {
        Ok(tokio::task::spawn(async move {
            let o = task.await;
            limit.task_finished();
            o
        }))
    } else {
        Err(task)
    }
}

impl SpawnLimit {
    /// Create a new limit on the amount of tasks
    /// that can be spawned before tasks are run in place.
    pub const fn new(limit: usize) -> Self {
        Self {
            limit,
            ordering: Ordering::Relaxed,
            current: AtomicUsize::new(0),
        }
    }
    /// Create a new limit with an atomic ordering.
    pub fn with_ordering(limit: usize, ordering: Ordering) -> Self {
        Self {
            limit,
            ordering,
            current: AtomicUsize::new(0),
        }
    }
    /// Check if the current limit has been reached.
    pub fn is_full(&self) -> bool {
        self.current.load(self.ordering) >= self.limit
    }

    /// Try to take a spot below the limit for spawning at a later
    /// point in time.
    pub fn take_limit(&'static self) -> Option<SpawnGuard> {
        if self.try_add_task() {
            Some(SpawnGuard { limit: Some(self) })
        } else {
            None
        }
    }

    /// Show the limit
    pub fn show_limit(&self) -> usize {
        self.limit
    }

    fn try_add_task(&self) -> bool {
        self.current
            .fetch_update(self.ordering, self.ordering, |c| {
                if c < self.limit {
                    Some(c + 1)
                } else {
                    None
                }
            })
            .is_ok()
    }
    fn task_finished(&self) {
        self.current
            .fetch_update(self.ordering, self.ordering, |c| {
                if c > 0 {
                    Some(c - 1)
                } else {
                    None
                }
            })
            .ok();
    }
}

impl SpawnGuard {
    pub fn spawn<T>(self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let limit = self
            .limit
            .expect("SpawnGuard created without limit. This is a bug");
        tokio::task::spawn(async move {
            let o = task.await;
            limit.task_finished();
            o
        })
    }
}

impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if let Some(l) = self.limit {
            l.task_finished();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macro_no_conflict() {
        let a = spawn_limit!(1);
        let b = spawn_limit!(1);
        assert!(!a.is_full());
        assert!(!b.is_full());

        assert!(a.try_add_task());
        assert!(a.is_full());
        assert!(!b.is_full());

        assert!(!a.try_add_task());
        assert!(a.is_full());
        assert!(!b.is_full());

        assert!(b.try_add_task());
        assert!(a.is_full());
        assert!(b.is_full());

        a.task_finished();
        assert!(!a.is_full());
        assert!(b.is_full());

        b.task_finished();
        assert!(!a.is_full());
        assert!(!b.is_full());

        b.task_finished();
        assert!(!a.is_full());
        assert!(!b.is_full());

        assert!(b.try_add_task());
        assert!(!a.is_full());
        assert!(b.is_full());
    }
}
