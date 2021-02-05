use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use observability::tracing;
use observability::tracing::*;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[macro_export]
macro_rules! spawn_limit {
    ($limit:expr) => {{
        static SPAWN_LIMIT: $crate::SpawnLimit =
            $crate::SpawnLimit::new($limit).with_location(file!(), line!());
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
    location: Option<(&'static str, u32)>,
    semaphore: once_cell::race::OnceBox<Semaphore>,
}

#[derive(Debug)]
pub struct SpawnGuard {
    limit: Option<&'static SpawnLimit>,
}

pub enum Spawn<T>
where
    // T: Future + Send + 'static,
    // T::Output: Send + 'static,
    T: Send + 'static,
{
    Spawned(JoinHandle<T>),
    InPlace(BoxFuture<'static, T>),
}

impl<T> Spawn<T>
where
    // T: Future + Send + 'static,
    // T::Output: Send + 'static,
    T: Send + 'static,
{
    /// Did this task successfully spawn?
    pub fn is_spawned(&self) -> bool {
        match &self {
            Self::Spawned(_) => true,
            Self::InPlace(_) => false,
        }
    }

    #[instrument(skip(self))]
    #[must_use]
    /// If the task was spawned return the [`JoinHandle`] otherwise
    /// await the task in place and wrap the output in a [`JoinHandle`].
    pub async fn finish(self) -> JoinHandle<T> {
        match self {
            Self::Spawned(jh) => jh,
            Self::InPlace(t) => {
                let out = t.await;
                tokio::task::spawn(async move { out })
            }
        }
    }
}

#[instrument(skip(limit, task))]
/// Spawn tasks up to a limit.
// / If the limit is passed then await the task in place.
// pub async fn spawn_with_limit<T>(limit: &'static SpawnLimit, task: BoxFuture<'static, T>) -> JoinHandle<T>
pub async fn spawn_with_limit<T>(limit: &'static SpawnLimit, task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
    // T: Send + 'static,
{
    spawn_attempt_limit(limit, task.boxed()).finish().await
}

#[instrument(skip(limit, task))]
/// Similar to [`spawn_try_limit`] with [`Spawn`] helper to run in task in place
/// if limit has been reached.
pub fn spawn_attempt_limit<T>(limit: &'static SpawnLimit, task: BoxFuture<'static, T>) -> Spawn<T>
where
    T: Send + 'static,
{
    if limit.try_add_task() {
        let h = tokio::task::spawn(async move {
            let o = task.await;
            limit.task_finished();
            o
        });
        Spawn::Spawned(h)
    } else {
        Spawn::InPlace(task)
    }
}

#[instrument(skip(limit, task, if_full))]
/// For long running tasks that you want to await when the limit is hit
/// but always want spawn once there is a place in the queue.
/// This has more overhead then the other calls.
///
/// `is_full` closure will run before await if the limit has been hit.
/// There could be races with `is_full` so use as a guide and don't rely
/// on it actually awaiting before spawning.
pub async fn spawn_queue_limit<T, F>(
    limit: &'static SpawnLimit,
    if_full: F,
    task: BoxFuture<'static, T>,
) -> JoinHandle<T>
where
    T: Send + 'static,
    F: FnOnce(),
{
    let (semaphore, full) = limit.add_task();
    if full {
        if_full()
    }
    let guard = semaphore.acquire().await;
    tokio::task::spawn(async move {
        let o = task.await;
        std::mem::drop(guard);
        o
    })
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
            location: None,
            semaphore: once_cell::race::OnceBox::new(),
        }
    }
    /// Create a new limit with an atomic ordering.
    pub const fn with_ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }

    /// Give this limit a location in the code for debugging.
    pub const fn with_location(mut self, file: &'static str, line: u32) -> Self {
        self.location = Some((file, line));
        self
    }

    #[cfg(test)]
    /// Check if the current limit has been reached.
    fn is_full(&self) -> bool {
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

    /// Show the location of this limit if there is one.
    pub fn show_location(&self) -> Option<(&'static str, u32)> {
        self.location.clone()
    }

    fn add_task(&'static self) -> (&Semaphore, bool) {
        let semaphore = self
            .semaphore
            .get_or_init(|| Box::new(Semaphore::new(self.limit)));
        let full = semaphore.available_permits() == 0;
        (semaphore, full)
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
    #[allow(dead_code)]
    fn show(&self) -> usize {
        self.current.load(self.ordering)
    }
}

impl SpawnGuard {
    pub fn spawn<T>(mut self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let limit = self
            .limit
            .take()
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

    #[test]
    fn take_limit() {
        let a = spawn_limit!(2);
        let first = a.take_limit();
        assert!(first.is_some());
        let g = a.take_limit();
        assert!(g.is_some());
        let g = a.take_limit();
        assert!(g.is_none());
        std::mem::drop(first);
        let g = a.take_limit();
        assert!(g.is_some());
        let g = a.take_limit();
        assert!(g.is_none());
    }
}
