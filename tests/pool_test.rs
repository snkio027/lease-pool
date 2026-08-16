use lease_pool::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;

// --- Mock 对象定义 ---

#[derive(Debug)]
struct TestObject {
    id: usize,
    clear_count: usize,
}

struct TestManager;
impl PoolManaged for TestManager {
    type Object = TestObject;

    fn new() -> Self::Object {
        static ID_GEN: AtomicUsize = AtomicUsize::new(0);
        TestObject {
            id: ID_GEN.fetch_add(1, Ordering::SeqCst),
            clear_count: 0,
        }
    }

    fn clear(obj: &mut Self::Object) {
        obj.clear_count += 1;
    }
}

#[derive(Debug)]
struct DropTrackedObject;

static BACKPRESSURE_DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

impl Drop for DropTrackedObject {
    fn drop(&mut self) {
        BACKPRESSURE_DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

struct DropTrackedManager;

impl PoolManaged for DropTrackedManager {
    type Object = DropTrackedObject;

    fn new() -> Self::Object {
        DropTrackedObject
    }

    fn clear(_obj: &mut Self::Object) {}
}

// --- 测试用例 ---

#[test]
fn test_lifecycle_and_reuse() {
    let registry = InboxRegistry::<TestManager>::new(1, 10);
    let mut pool = LocalLeasePool::new(0, &registry, 1);

    // 分配
    let obj = pool.alloc();
    let first_id = obj.id;
    assert_eq!(obj.clear_count, 1, "新对象分配后应调用一次 clear");

    // 归还
    drop(obj);

    // 再次分配：对象先进入 Inbox，再由 owner 搬运到 L1。
    let obj2 = pool.alloc();
    assert_eq!(obj2.id, first_id, "对象应当被重用");
    assert_eq!(obj2.clear_count, 2, "重用对象应再次调用 clear");
}

#[test]
fn test_lease_can_outlive_local_pool() {
    let registry = InboxRegistry::<TestManager>::new(1, 10);

    let obj = {
        let mut pool = LocalLeasePool::new(0, &registry, 0);
        pool.alloc()
    };
    let original_id = obj.id;

    // Lease 只依赖 Registry，不依赖创建它的 LocalLeasePool。
    drop(obj);

    let mut replacement_pool = LocalLeasePool::new(0, &registry, 0);
    let recycled = replacement_pool.alloc();
    assert_eq!(recycled.id, original_id);
}

#[test]
fn test_cross_thread_recycling() {
    // 每一个测试使用独立的 Registry，这没问题
    let registry = InboxRegistry::<TestManager>::new(1, 10);
    let mut pool = LocalLeasePool::new(0, &registry, 0);

    let obj = pool.alloc();
    let original_id = obj.id;

    thread::scope(|scope| {
        scope
            .spawn(move || {
                // 显式丢弃，确保在 join 之前发生
                drop(obj);
            })
            .join()
            .expect("Thread panicked");
    });

    let obj_new = pool.alloc();
    assert_eq!(obj_new.id, original_id, "对象应通过 Inbox 从其他线程回收");
}

#[test]
fn test_stress_concurrency() {
    // 针对 Miri 减小规模
    let (num_threads, iters) = if cfg!(miri) { (2, 10) } else { (8, 1000) };

    let registry = InboxRegistry::<TestManager>::new(num_threads, 128);

    thread::scope(|scope| {
        let (sender, receiver) = mpsc::sync_channel(num_threads * 4);
        let dropper = scope.spawn(move || {
            for obj in receiver {
                drop(obj);
            }
        });

        for i in 0..num_threads {
            let registry = &registry;
            let sender = sender.clone();
            scope.spawn(move || {
                let mut pool = LocalLeasePool::new(i, registry, 32);
                for _ in 0..iters {
                    sender.send(pool.alloc()).unwrap();
                    thread::yield_now();
                }
            });
        }

        drop(sender);
        dropper.join().unwrap();
    });
}

#[test]
fn test_backpressure_overflow() {
    let inbox_cap = 2;
    let registry = InboxRegistry::<DropTrackedManager>::new(1, inbox_cap);
    let mut pool = LocalLeasePool::new(0, &registry, 0);

    let before_drop = BACKPRESSURE_DROP_COUNT.load(Ordering::SeqCst);

    // 分配 5 个，超过 Inbox 容量 (2)
    let mut items = Vec::new();
    for _ in 0..5 {
        items.push(pool.alloc());
    }

    // 释放所有对象
    items.clear();

    // Inbox 存了 2 个，剩下 3 个应该被直接触发 Drop
    let dropped = BACKPRESSURE_DROP_COUNT.load(Ordering::SeqCst) - before_drop;
    assert_eq!(dropped, 3, "溢出的对象应直接被系统销毁");
}
