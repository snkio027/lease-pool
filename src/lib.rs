use crossbeam_queue::ArrayQueue;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

const DRAIN_LIMIT: usize = 128;

pub trait PoolManaged: Sized {
    type Object: Send + Debug;
    fn new() -> Self::Object;
    fn clear(obj: &mut Self::Object);
}

/// 缓存行对齐防止伪共享
#[repr(align(64))]
pub struct Inbox<M: PoolManaged> {
    pub queue: ArrayQueue<M::Object>,
}

/// 全局路由表
pub struct InboxRegistry<M: PoolManaged> {
    inboxes: Box<[Inbox<M>]>,
}

impl<M: PoolManaged> InboxRegistry<M> {
    pub fn new(num_threads: usize, inbox_capacity: usize) -> Self {
        assert!(num_threads > 0, "num_threads must be greater than zero");
        assert!(
            inbox_capacity > 0,
            "inbox_capacity must be greater than zero"
        );

        let mut inboxes = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            inboxes.push(Inbox {
                queue: ArrayQueue::new(inbox_capacity),
            });
        }
        Self {
            inboxes: inboxes.into_boxed_slice(),
        }
    }
}

/// Worker 本地的无锁对象池
pub struct LocalLeasePool<'registry, M: PoolManaged> {
    local_cache: Vec<M::Object>,
    inbox: &'registry Inbox<M>,
}

impl<'registry, M: PoolManaged> LocalLeasePool<'registry, M> {
    pub fn new(
        thread_id: usize,
        registry: &'registry InboxRegistry<M>,
        initial_capacity: usize,
    ) -> Self {
        let inbox = registry.inboxes.get(thread_id).unwrap_or_else(|| {
            panic!(
                "thread_id {thread_id} is out of range for {} workers",
                registry.inboxes.len()
            )
        });
        let mut local_cache = Vec::with_capacity(initial_capacity);

        for _ in 0..initial_capacity {
            local_cache.push(M::new());
        }

        Self { local_cache, inbox }
    }

    pub fn alloc(&mut self) -> Lease<'registry, M> {
        // 1. 优先尝试本地 LIFO
        if let Some(mut item) = self.local_cache.pop() {
            M::clear(&mut item);
            return self.wrap(item);
        }

        // 2. 本地为空，搬运 Inbox
        for _ in 0..DRAIN_LIMIT {
            match self.inbox.queue.pop() {
                Some(item) => self.local_cache.push(item),
                None => break,
            }
        }

        if let Some(mut item) = self.local_cache.pop() {
            M::clear(&mut item);
            self.wrap(item)
        } else {
            // 3. Fallback: 分配新对象
            self.wrap(M::new())
        }
    }

    #[inline(always)]
    fn wrap(&self, item: M::Object) -> Lease<'registry, M> {
        Lease {
            item: Some(item),
            inbox: self.inbox,
        }
    }
}

/// 携带发端元数据的智能指针
pub struct Lease<'registry, M: PoolManaged> {
    item: Option<M::Object>,
    inbox: &'registry Inbox<M>,
}

impl<M: PoolManaged> Deref for Lease<'_, M> {
    type Target = M::Object;
    fn deref(&self) -> &Self::Target {
        self.item.as_ref().unwrap()
    }
}

impl<M: PoolManaged> DerefMut for Lease<'_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item.as_mut().unwrap()
    }
}

impl<M: PoolManaged> Drop for Lease<'_, M> {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            // 如果 push 失败（返回 Err），说明队列满了，触发自动 drop
            if let Err(_dropped_item) = self.inbox.queue.push(item) {
                #[cfg(test)]
                {
                    // 在测试环境下，如果是跨线程回收测试，队列不应该满。
                    // 打印这句话能帮我们确认是不是 push 环节出了问题。
                    println!("Backpressure triggered: Inbox is full!");
                }
            }
        }
    }
}
