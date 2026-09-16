# 08 · 缓存与存储装饰器

> 本章目标：掌握 KV 缓存契约与两个引擎；学会用两个存储装饰器——缓存旁路（`CacheRepo`）与软删除（`SoftDeleteRepo`）——在不动业务代码的前提下改变仓储行为。

前置：[03 · 存储契约](03-storage-contract.md)。

## KV 缓存契约

`rushwind-cache` 只有七个动词，全部 TTL 驱动：

```rust
pub trait Cache: Send + Sync {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, CacheError>>;
    fn set<'a>(&'a self, key: &'a str, value: &'a [u8], ttl: Option<Duration>) -> BoxFuture<'a, Result<(), CacheError>>;
    fn set_nx<'a>(&'a self, key: &'a str, value: &'a [u8], ttl: Option<Duration>) -> BoxFuture<'a, Result<bool, CacheError>>;  // true = 抢到
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), CacheError>>;
    fn has<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>>;
    fn get_multi<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, CacheError>>;
    fn set_multi<'a>(&'a self, items: &'a [Item]) -> BoxFuture<'a, Result<(), CacheError>>;
    fn close(&self) -> BoxFuture<'_, Result<(), CacheError>>;
}
```

三条契约纪律：**缺失/过期是 `Ok(None)` 不是错误**；**值是裸字节**，序列化是调用方的事；**`ttl: None` 意为"引擎默认"**，可能是永不过期。

## 两个引擎

```rust
// 进程内：惰性过期 + 容量逐出（默认 65,536 条，过期者优先逐出）
let cache = LocalCache::with_options(CacheOptions { default_ttl: Some(Duration::from_secs(60)), capacity: 4096 });
cache.set("user:1", b"alice", None).await?;
assert_eq!(cache.get("user:1").await?, Some(b"alice".to_vec()));

// Redis：连接管理器 + 可选键前缀（对调用方透明）
let cache = RedisCache::connect_with("redis://127.0.0.1:6379", "myapp:").await?;
```

- `LocalCache` 支持亚秒级 TTL；`close()` 清空全表。
- `RedisCache` 的 `get_multi` 走原生 MGET、`set_multi` 走流水线；`set_nx` 不带 TTL = 无过期锁，**必须显式 delete**。`close()` 是无操作（连接随引擎析构）。

## 存储装饰器一：缓存旁路 `CacheRepo`

`rushwind-storage-cache` 把任意 `Repository` 包成带缓存的仓储：

```rust
let cached: Arc<dyn Repository> = CacheRepo::with_ttl(Arc::new(inner_repo), Duration::from_secs(30));
```

行为要点：

- 只缓存 `get`（键含**查看者的完整租户范围**，行不会跨租户泄漏）；`list`/`count`/`exists` 直通。
- 并发未命中单飞（singleflight）合并，负行（查无此行）也缓存。
- 写操作按主键推代——写后仍在途的旧加载结果会被直接丢弃，读到陈旧行的窗口压到最小。
- **主键必须是整数**（内部经 `Value::as_i64`），否则 `InvalidQuery`。

## 存储装饰器二：软删除 `SoftDeleteRepo`

`rushwind-storage-soft-delete` 把 `delete` 变成墓碑写：

```rust
// schema 需要先有整型的 deleted_at 列（unix 毫秒，NULL = 活着）
let repo = SoftDeleteRepo::new(Arc::new(inner) as Arc<dyn Repository>)?;
let ctx = QueryCtx::all_access();

repo.delete(ctx.clone(), id).await?;          // 变成墓碑写入（ctx 按值传）
repo.list(ctx.clone(), &query).await?;        // 读自动过滤墓碑
repo.restore_async(&ctx, id).await?;          // 复活；复活活行 = NotFound
repo.purge_async(&ctx, id).await?;            // 真删
repo.list_deleted_async(&ctx, &query).await?; // 只看墓碑
```

`upsert` 可以复活已墓碑的主键；装饰器构造时校验墓碑列存在且为 `Int`。要看穿墓碑用 `inner()` 逃逸口。

## 组装：装饰器叠罗汉

装饰器实现的就是 `Repository` 本身，所以可以叠——业务代码对内层是谁毫无感知：

```rust
let repo: Arc<dyn Repository> = Arc::new(MemoryRepo::new(schema())?);
let repo: Arc<dyn Repository> = Arc::new(SoftDeleteRepo::new(repo.clone())?);
let repo: Arc<dyn Repository> = CacheRepo::with_ttl(repo, Duration::from_secs(30));

// 之后照旧：同一套 CrudApi / route_pack / 业务代码
let router = CrudApi::new(repo).with_viewer(viewer).router();
```

**经验顺序：软删除在内（靠近真相），缓存在外（靠近读者）**——墓碑过滤发生在缓存之前，被删的行才不会以"活着"的面目被缓存。

## 踩坑清单

- **`set_multi` 的 TTL 陷阱**（本地引擎）：条目自带 TTL 才用条目的，否则用引擎默认 TTL——想批量短 TTL 就在 `Item` 里逐条带。
- **`CacheRepo` 只救 `get`**：`list` 的缓存要靠上游自己做（或等 list 缓存语义稳定），别以为包一层全查询都加速了。
- **软删除的审计语义**：墓碑写、purge、restore 分别记为 `Delete`/`Delete`/`Update`，对账时按这个理解。
- **缓存里放裸字节**：跨服务共享 Redis 缓存时统一序列化格式（建议 JSON），并留好版本号字段。

## 动手练习

1. 给 `CacheRepo` 写一个并发测试：同一 id 十个并发 `get` 穿透到底层恰好一次（singleflight）。
2. 验证软删除的三态：活行可见、墓碑不可见、`list_deleted` 可见；再用 upsert 复活同一主键。
3. 把"软删除在内、缓存在外"的顺序反过来搭一遍，写出删行后缓存仍返回旧行的复现用例——理解顺序即正确性。

---
[系列目录](README.md) | 上一章：[07 · 消息通信](07-broker.md) | 下一章：[09 · 注册发现与韧性](09-governance.md)
