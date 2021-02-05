use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::FutureExt;
use observability::tracing::*;
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
    let num_running3 = Arc::new(AtomicUsize::new(0));
    let most_running3 = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..5 {
        jhs.push(do_work_other_m(num_running2.clone(), most_running2.clone()).await);
        jhs.push(do_work_m(num_running.clone(), most_running.clone()).await);
        jhs.push(do_work_take(num_running3.clone(), most_running3.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Check they don't interfere
    // First can have 2 spawned plus one in place running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
    // Second can have 3 spawned plus one in place running at any moment
    assert_eq!(most_running2.load(Ordering::SeqCst), 4);
    assert_eq!(most_running3.load(Ordering::SeqCst), 2);
}

#[tokio::test(threaded_scheduler)]
async fn queues() {
    let num_running = Arc::new(AtomicUsize::new(0));
    let most_running = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..4 {
        jhs.push(do_work_queue(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Can only have 2 spawned at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 2);

    let num_running2 = Arc::new(AtomicUsize::new(0));
    let most_running2 = Arc::new(AtomicUsize::new(0));

    let mut jhs = Vec::new();
    for _ in 0..5 {
        jhs.push(do_work_other_queue(num_running2.clone(), most_running2.clone()).await);
        jhs.push(do_work_queue(num_running.clone(), most_running.clone()).await);
    }

    for jh in jhs {
        jh.await.unwrap();
    }

    // Check they don't interfere
    // First can have 2 spawned running at any moment
    assert_eq!(most_running.load(Ordering::SeqCst), 2);
    // Second can have 3 spawned running at any moment
    assert_eq!(most_running2.load(Ordering::SeqCst), 3);
}

#[tokio::test(threaded_scheduler)]
async fn with_recursive() {
    let _g = observability::test_run();
    let num_running = Arc::new(AtomicUsize::new(0));
    let most_running = Arc::new(AtomicUsize::new(0));

    recursive_1(num_running, most_running.clone())
        .await
        .await
        .unwrap();
    assert_eq!(most_running.load(Ordering::SeqCst), 3);
}

async fn do_work(num_running: Arc<AtomicUsize>, most_running: Arc<AtomicUsize>) -> JoinHandle<()> {
    static SPAWN_LIMIT: SpawnLimit = SpawnLimit::new(2);
    spawn_with_limit(
        &SPAWN_LIMIT,
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(1)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_other(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    static SPAWN_LIMIT: SpawnLimit = SpawnLimit::new(3);
    spawn_with_limit(
        &SPAWN_LIMIT,
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(2)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_m(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    spawn_with_limit(
        spawn_limit!(2),
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(1)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_other_m(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(3);
    spawn_with_limit(
        limit,
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(2)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_queue(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    spawn_queue_limit(
        spawn_limit!(2),
        || debug!("full"),
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(1)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_other_queue(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(3);
    spawn_queue_limit(
        limit,
        || debug!("full {:?}", limit.show_location()),
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_secs(2)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .boxed(),
    )
    .await
}

async fn do_work_take(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(2);
    let guard = match limit.take_limit() {
        Some(l) => l,
        None => return tokio::task::spawn(async {}),
    };
    guard.spawn(async move {
        let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
        most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::delay_for(Duration::from_millis(500)).await;
        num_running.fetch_sub(1, Ordering::SeqCst);
    })
}

async fn recursive_1(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(1);
    spawn_with_limit(limit, {
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            recursive_2(num_running.clone(), most_running.clone()).await;
            tokio::time::delay_for(Duration::from_millis(50)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .instrument(debug_span!("inner_task1"))
    })
    .await
}

async fn recursive_2(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(1);
    spawn_with_limit(limit, {
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            recursive_3(num_running.clone(), most_running.clone()).await;
            tokio::time::delay_for(Duration::from_millis(50)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .instrument(debug_span!("inner_task2"))
    })
    .await
}

async fn recursive_3(
    num_running: Arc<AtomicUsize>,
    most_running: Arc<AtomicUsize>,
) -> JoinHandle<()> {
    let limit = spawn_limit!(1);
    spawn_with_limit(limit, {
        async move {
            let running = num_running.fetch_add(1, Ordering::SeqCst) + 1;
            most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::delay_for(Duration::from_millis(50)).await;
            num_running.fetch_sub(1, Ordering::SeqCst);
        }
        .instrument(debug_span!("inner_task2"))
    })
    .await
}
