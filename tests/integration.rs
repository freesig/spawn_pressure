use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use spawn_pressure::*;
use tokio::task::JoinHandle;

#[tokio::test(threaded_scheduler)]
async fn limit_spawns() {
    let num_running = Arc::new(AtomicUsize::new(0));
    let most_running = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..4 {
        jhs.push(do_work(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Can only have 2 spawned plus one in place running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
    let num_running2 = Arc::new(AtomicUsize::new(0));
    let most_running2 = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..5 {
        jhs.push(do_work_other(num_running2.clone(), most_running2.clone()).await);
        jhs.push(do_work(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Check they don't interfere
    // First can have 2 spawned plus one in place running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
    // Second can have 3 spawned plus one in place running at any moment
    assert_eq!(most_running2.load(Ordering::SeqCst), 4);
}

#[tokio::test(threaded_scheduler)]
async fn macros() {
    let num_running = Arc::new(AtomicUsize::new(0));
    let most_running = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..4 {
        jhs.push(do_work_m(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Can only have 2 spawned plus one in place running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
    let num_running2 = Arc::new(AtomicUsize::new(0));
    let most_running2 = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..5 {
        jhs.push(do_work_other_m(num_running2.clone(), most_running2.clone()).await);
        jhs.push(do_work_m(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Check they don't interfere
    // First can have 2 spawned plus one in place running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
    // Second can have 3 spawned plus one in place running at any moment
    assert_eq!(most_running2.load(Ordering::SeqCst), 4);
}

async fn do_work(num_running: Arc<AtomicUsize>, most_running: Arc<AtomicUsize>) -> JoinHandle<()> {
    static SPAWN_LIMIT: SpawnLimit = SpawnLimit::new(2);
    spawn_with_limit(&SPAWN_LIMIT, async move {
        let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
        most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::delay_for(Duration::from_secs(1)).await;
        num_running.fetch_sub(1, Ordering::SeqCst);
    })
    .await
}

async fn do_work_other(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    static SPAWN_LIMIT: SpawnLimit = SpawnLimit::new(3);
    spawn_with_limit(&SPAWN_LIMIT, async move {
        let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
        most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::delay_for(Duration::from_secs(2)).await;
        num_running.fetch_sub(1, Ordering::SeqCst);
    })
    .await
}

async fn do_work_m(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    spawn_with_limit(spawn_limit!(2), async move {
        let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
        most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::delay_for(Duration::from_secs(1)).await;
        num_running.fetch_sub(1, Ordering::SeqCst);
    })
    .await
}

async fn do_work_other_m(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(3);
    spawn_with_limit(limit, async move {
        let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
        most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::delay_for(Duration::from_secs(2)).await;
        num_running.fetch_sub(1, Ordering::SeqCst);
    })
    .await
}
