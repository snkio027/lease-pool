# LeasePool-RS 🦀

**LeasePool-RS** 是一个面向固定 Worker Pipeline 的低竞争对象池。对象由所属 Worker
分配，任务可以携带对象跨线程执行，并在 `Lease` 销毁时归还到所属 Worker 的 Inbox。

## 核心设计

- **L1（Local Cache）**：owner Worker 私有的 LIFO 分配缓存。L1 命中时不访问共享队列。
- **L2（Inbox）**：基于 `crossbeam-queue` 的有界无锁队列，接收其他线程归还的对象。
- **批量搬运**：L1 为空时，owner 最多从 Inbox 搬运 128 个对象，降低后续分配成本。
- **自动背压**：Inbox 已满时，归还失败的对象直接销毁，避免缓存无限增长。
- **生命周期安全**：`Lease` 通过 Rust 生命周期借用 Inbox，不使用裸指针或手动
  `Send`/`Sync`，编译器保证 Registry 晚于所有 Lease 销毁。
- **缓存行对齐**：每个 Inbox 使用 `#[repr(align(64))]` 隔离，降低相邻 Worker
  队列之间的伪共享风险。

## 对象流转

```text
Owner Worker 分配：
L1 local_cache → L2 Inbox 批量搬运 → 创建新对象

Remote Worker 归还：
drop(Lease) → 原 Owner 的 Inbox → 队列满时销毁对象
```

L1 是 owner 侧的批量分配缓存，并不是同线程 Drop 缓存。所有 Lease 都通过 Inbox
归还；本项目的目标场景预期 Drop 发生在远端线程。

## 快速上手

### 1. 实现管理协议

```rust
use lease_pool::{InboxRegistry, LocalLeasePool, PoolManaged};

#[derive(Debug)]
pub struct MyBuffer(Vec<u8>);

pub struct MyBufferManager;

impl PoolManaged for MyBufferManager {
    type Object = MyBuffer;

    fn new() -> Self::Object {
        MyBuffer(Vec::with_capacity(1024))
    }

    fn clear(obj: &mut Self::Object) {
        obj.0.clear();
    }
}
```

### 2. 初始化与分配

```rust
use lease_pool::{InboxRegistry, LocalLeasePool};

# use lease_pool::PoolManaged;
# #[derive(Debug)]
# struct MyBuffer(Vec<u8>);
# struct MyBufferManager;
# impl PoolManaged for MyBufferManager {
#     type Object = MyBuffer;
#     fn new() -> Self::Object { MyBuffer(Vec::new()) }
#     fn clear(obj: &mut Self::Object) { obj.0.clear(); }
# }
let registry = InboxRegistry::<MyBufferManager>::new(4, 1024);
let mut local_pool = LocalLeasePool::new(0, &registry, 64);

let mut lease = local_pool.alloc();
lease.0.push(1);
// lease 离开作用域后进入 Worker 0 的 Inbox。
```

### 3. 跨线程归还

`Lease` 借用 Registry，因此跨线程任务应使用 scoped threads，确保所有任务在
Registry 销毁前结束：

```rust
use lease_pool::{InboxRegistry, LocalLeasePool, PoolManaged};

#[derive(Debug)]
struct Buffer(Vec<u8>);

struct Manager;

impl PoolManaged for Manager {
    type Object = Buffer;

    fn new() -> Self::Object {
        Buffer(Vec::new())
    }

    fn clear(obj: &mut Self::Object) {
        obj.0.clear();
    }
}

let registry = InboxRegistry::<Manager>::new(1, 1024);
let mut pool = LocalLeasePool::new(0, &registry, 64);
let lease = pool.alloc();

std::thread::scope(|scope| {
    scope.spawn(move || {
        drop(lease); // 在远端线程归还
    });
});
```

需要 `Send + 'static` 的非结构化任务（例如 `tokio::spawn`）无法借用普通 Registry。
这类集成需要由应用为 Registry 提供进程级静态生命周期，或使用带所有权计数的另一种
适配器；不要绕过生命周期制造裸指针。

## 性能调优

| 参数 | 建议值 | 说明 |
| :--- | :--- | :--- |
| `inbox_capacity` | 1024–4096 | 每个 Worker 的远端归还缓冲区；过小会触发背压销毁。 |
| `initial_capacity` | 32–128 | 每个 Worker 预分配的对象数，用于平衡内存与热启动。 |
| `DRAIN_LIMIT` | 128（固定） | L1 为空时单次从 Inbox 搬运的最大数量。 |

具体参数应通过目标负载下的基准测试确定。

## 安全与关停

- `InboxRegistry` 必须位于所有 `LocalLeasePool` 和 `Lease` 的外层作用域；这一约束由
  类型系统强制执行。
- 关停时应先结束 scoped worker，确认所有 Lease 已归还，再离开 Registry 的作用域。
- `num_threads` 和 `inbox_capacity` 必须大于零；`thread_id` 必须小于 Worker 数量。
- 本 crate 的对象池实现不包含 `unsafe` 代码；底层无锁队列由 `crossbeam-queue` 提供。

## 依赖

- `crossbeam-queue`：有界无锁 Inbox。
