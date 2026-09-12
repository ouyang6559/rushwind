//! The in-memory reference engine for the [`Repository`] contract.
//!
//! Two roles:
//!
//! - **Semantic oracle** — filter, sort, paging and tenancy semantics are
//!   evaluated here directly in Rust; the conformance suite pins every other
//!   engine against the same behavior.
//! - **Instant brick** — tests, demos and embedding scenarios get a real
//!   repository with no driver and no latency budget concerns.
//!
//! Rows live in a `BTreeMap` keyed by the integer primary key, guarded by a
//! mutex; every operation is a synchronous step wrapped in the contract's
//! boxed-future shape.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::future::ready;
use std::sync::Mutex;

use rushwind_storage::{
    AuditAction, AuditEntry, Condition, FieldMask, FilterExpr, FilterNode, ListQuery, Op, Page,
    Paging, QueryCtx, Record, RepoFuture, Repository, Schema, Scope, Sort, SortDir, StorageError,
    Value, Viewer,
};

/// A [`Repository`] keeping rows in process memory.
pub struct MemoryRepo {
    schema: Schema,
    table: Mutex<Table>,
}

struct Table {
    rows: BTreeMap<i64, Record>,
    next_id: i64,
}

impl MemoryRepo {
    /// Binds a repository to `schema`.
    ///
    /// The primary key must be an integer column — the in-memory engine's
    /// identity model, and the contract's cursor ordering.
    pub fn new(schema: Schema) -> Result<Self, StorageError> {
        let pk = schema
            .column(&schema.primary_key)
            .ok_or_else(|| StorageError::InvalidQuery("schema has no primary key".into()))?;
        if pk.kind != rushwind_storage::ColumnKind::Int {
            return Err(StorageError::Unsupported(
                "the in-memory engine requires an integer primary key".into(),
            ));
        }
        Ok(Self {
            schema,
            table: Mutex::new(Table {
                rows: BTreeMap::new(),
                next_id: 1,
            }),
        })
    }

    // ---- synchronous core -------------------------------------------------

    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table.lock().expect("memory table poisoned")
    }

    /// Validates a write payload against the schema: every field must be a
    /// declared column carrying a kind-compatible value.
    fn validate_row(&self, row: &Record, payload: &str) -> Result<(), StorageError> {
        for (field, value) in row.iter() {
            let column = self.schema.column(field).ok_or_else(|| {
                StorageError::InvalidQuery(format!("unknown column {field:?} in {payload}"))
            })?;
            if !column.kind.accepts(value.type_name()) {
                return Err(StorageError::InvalidQuery(format!(
                    "column {field:?} does not accept a {} value",
                    value.type_name()
                )));
            }
        }
        Ok(())
    }

    /// A full row: every declared column present, absent fields as `Null`.
    fn materialize(&self, row: &Record) -> Record {
        let mut full = Record::new();
        for column in &self.schema.columns {
            let value = row.get(&column.name).cloned().unwrap_or(Value::Null);
            full.insert(column.name.as_str(), value);
        }
        full
    }

    /// Whether `row` passes the viewer's scope and the query's filter.
    fn visible(&self, ctx: &QueryCtx, extra: Option<&FilterNode>, row: &Record) -> bool {
        let in_scope = match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
            Scope::Deny => false,
            Scope::Unrestricted => true,
            Scope::Scoped(filter) => eval(filter.node(), row),
        };
        if !in_scope {
            return false;
        }
        match extra {
            Some(node) => eval(node, row),
            None => true,
        }
    }

    fn project(&self, mask: &Option<FieldMask>, row: &Record) -> Record {
        let mut projected = row.clone();
        if let Some(mask) = mask {
            projected.retain(|field| mask.allows(field));
        }
        projected
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(AuditEntry::now(
            action,
            self.schema.table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }
}

impl Repository for MemoryRepo {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(ready(self.get_sync(ctx, id)))
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        Box::pin(ready(self.list_sync(ctx, query)))
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        Box::pin(ready(self.count_sync(ctx, filter)))
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(ready(self.create_sync(ctx, row)))
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        Box::pin(ready(self.batch_create_sync(ctx, rows)))
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        Box::pin(ready(self.update_sync(ctx, id, patch)))
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(ready(self.upsert_sync(ctx, row)))
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        Box::pin(ready(self.delete_sync(ctx, id)))
    }
}

impl MemoryRepo {
    fn get_sync(&self, ctx: QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let id = expect_id(&id)?;
        let table = self.lock();
        Ok(table
            .rows
            .get(&id)
            .filter(|row| self.visible(&ctx, None, row))
            .cloned())
    }

    fn list_sync(&self, ctx: QueryCtx, query: &ListQuery) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        let table = self.lock();
        let filter = query.filter.as_ref().map(|f| f.node());

        let mut matches: Vec<&Record> = table
            .rows
            .values()
            .filter(|row| self.visible(&ctx, filter, row))
            .collect();
        let total = matches.len() as u64;
        sort_rows(&mut matches, &query.sort);

        let limit = query.paging.limit() as usize;
        let (kept, has_more): (Vec<&Record>, bool) = match &query.paging {
            Paging::Token { token, .. } => {
                let last = if token.is_empty() {
                    None
                } else {
                    Some(rushwind_storage::decode_cursor(token)?)
                };
                let window: Vec<&Record> = matches
                    .into_iter()
                    .filter(|row| match last {
                        Some(last_id) => pk_of(row) > last_id,
                        None => true,
                    })
                    .collect();
                let has_more = window.len() > limit;
                (window.into_iter().take(limit).collect(), has_more)
            }
            _ => {
                let skip = match &query.paging {
                    Paging::Page { page, .. } => {
                        (((*page - 1) as u64) * (query.paging.limit() as u64)) as usize
                    }
                    Paging::Offset { offset, .. } => *offset as usize,
                    Paging::Token { .. } => unreachable!("token handled above"),
                };
                let has_more = matches.len() > skip + limit;
                (
                    matches.into_iter().skip(skip).take(limit).collect(),
                    has_more,
                )
            }
        };

        let items: Vec<Record> = kept
            .iter()
            .map(|row| self.project(&query.mask, row))
            .collect();
        let next_token = if has_more {
            kept.last()
                .map(|row| rushwind_storage::encode_cursor(pk_of(row)))
        } else {
            None
        };
        Ok(Page {
            items,
            total,
            next_token,
        })
    }

    fn count_sync(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let table = self.lock();
        let node = filter.as_ref().map(|f| f.node());
        Ok(table
            .rows
            .values()
            .filter(|row| self.visible(&ctx, node, row))
            .count() as u64)
    }

    fn create_sync(&self, ctx: QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row, "create payload")?;
        let pk = self.schema.primary_key.clone();
        let mut table = self.lock();
        let mut stored = self.materialize(&row);
        let id = match stored.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => {
                if table.rows.contains_key(existing) {
                    return Err(StorageError::Conflict(format!(
                        "primary key {existing} already exists"
                    )));
                }
                *existing
            }
            _ => loop {
                let next = table.next_id;
                table.next_id += 1;
                if !table.rows.contains_key(&next) {
                    break next;
                }
            },
        };
        stored.insert(pk.as_str(), id);
        table.rows.insert(id, stored.clone());
        drop(table);
        self.audit(&ctx, AuditAction::Create, Some(Value::Int(id)));
        Ok(stored)
    }

    fn batch_create_sync(
        &self,
        ctx: QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        // Validate and stage everything first, then apply — atomic without
        // rollback.
        let pk = self.schema.primary_key.clone();
        let mut table = self.lock();
        let mut staged: Vec<(i64, Record)> = Vec::with_capacity(rows.len());
        let mut claimed = std::collections::BTreeSet::new();
        for row in &rows {
            self.validate_row(row, "batch payload")?;
            let mut full = self.materialize(row);
            let id = match full.get(pk.as_str()) {
                Some(Value::Int(existing)) if *existing > 0 => *existing,
                _ => loop {
                    let next = table.next_id;
                    table.next_id += 1;
                    if !table.rows.contains_key(&next) && !claimed.contains(&next) {
                        break next;
                    }
                },
            };
            if !claimed.insert(id) {
                return Err(StorageError::Conflict(format!(
                    "primary key {id} repeats within the batch"
                )));
            }
            full.insert(pk.as_str(), id);
            staged.push((id, full));
        }
        for (id, _) in &staged {
            if table.rows.contains_key(id) {
                return Err(StorageError::Conflict(format!(
                    "primary key {id} already exists"
                )));
            }
        }
        let stored: Vec<Record> = staged
            .into_iter()
            .map(|(id, row)| {
                table.rows.insert(id, row.clone());
                row
            })
            .collect();
        drop(table);
        self.audit(&ctx, AuditAction::BatchCreate, None);
        Ok(stored)
    }

    fn update_sync(&self, ctx: QueryCtx, id: Value, patch: Record) -> Result<Record, StorageError> {
        self.validate_row(&patch, "update patch")?;
        let pk = self.schema.primary_key.clone();
        if let Some(new_pk) = patch.get(pk.as_str()) {
            if new_pk != &Value::Null && new_pk.as_i64() != expect_id(&id).ok() {
                return Err(StorageError::InvalidQuery(
                    "update patches cannot move the primary key".into(),
                ));
            }
        }
        let id = expect_id(&id)?;
        let mut table = self.lock();
        let current = table
            .rows
            .get(&id)
            .filter(|row| self.visible(&ctx, None, row))
            .cloned()
            .ok_or(StorageError::NotFound)?;
        let mut updated = current;
        for (field, value) in patch.iter() {
            updated.insert(field, value.clone());
        }
        table.rows.insert(id, updated.clone());
        drop(table);
        self.audit(&ctx, AuditAction::Update, Some(Value::Int(id)));
        Ok(updated)
    }

    fn upsert_sync(&self, ctx: QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row, "upsert payload")?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => {
                return Err(StorageError::InvalidQuery(
                    "upsert requires an explicit primary key".into(),
                ))
            }
        };
        let mut table = self.lock();
        let existing = table
            .rows
            .get(&id)
            .filter(|stored| self.visible(&ctx, None, stored))
            .cloned();
        let stored = match existing {
            Some(current) => {
                let mut updated = current;
                for (field, value) in row.iter() {
                    updated.insert(field, value.clone());
                }
                table.rows.insert(id, updated.clone());
                updated
            }
            None => {
                let mut full = self.materialize(&row);
                full.insert(pk.as_str(), id);
                table.rows.insert(id, full.clone());
                full
            }
        };
        drop(table);
        self.audit(&ctx, AuditAction::Upsert, Some(Value::Int(id)));
        Ok(stored)
    }

    fn delete_sync(&self, ctx: QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = expect_id(&id)?;
        let mut table = self.lock();
        if table
            .rows
            .get(&id)
            .filter(|row| self.visible(&ctx, None, row))
            .is_none()
        {
            return Err(StorageError::NotFound);
        }
        table.rows.remove(&id);
        drop(table);
        self.audit(&ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

fn expect_id(id: &Value) -> Result<i64, StorageError> {
    id.as_i64()
        .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
}

fn pk_of(row: &Record) -> i64 {
    row.get("id").and_then(Value::as_i64).unwrap_or_default()
}

/// Deterministic total order: the query's sort terms, then the primary key
/// as the tiebreaker. Default (empty sort) is primary key ascending.
fn sort_rows(rows: &mut [&Record], sort: &Sort) {
    rows.sort_by(|a, b| {
        for term in &sort.fields {
            let ord = a
                .get(&term.field)
                .unwrap_or(&Value::Null)
                .compare(b.get(&term.field).unwrap_or(&Value::Null));
            let ord = match term.dir {
                SortDir::Asc => ord,
                SortDir::Desc => ord.reverse(),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        pk_of(a).cmp(&pk_of(b))
    });
}

/// Evaluates a filter node against a row — the contract's reference
/// semantics every SQL engine must match.
fn eval(node: &FilterNode, row: &Record) -> bool {
    match node {
        FilterNode::All(children) => children.iter().all(|c| eval(c, row)),
        FilterNode::Any(children) => children.iter().any(|c| eval(c, row)),
        FilterNode::Cond(cond) => eval_cond(cond, row),
    }
}

fn eval_cond(cond: &Condition, row: &Record) -> bool {
    // A field absent from the record is NULL, same as in SQL.
    let value = row.get(&cond.field).cloned().unwrap_or(Value::Null);
    let first = || cond.values.first().cloned().unwrap_or(Value::Null);
    let text_arg = |fmt: fn(&str) -> String| match first().as_str() {
        Some(s) => Value::Text(fmt(s)),
        None => Value::Null,
    };
    let cmp = |other: &Value| value.compare(other);
    match cond.op {
        Op::Eq => cmp(&first()) == Ordering::Equal,
        Op::NotEq => cmp(&first()) != Ordering::Equal,
        Op::Gt => cmp(&first()) == Ordering::Greater,
        Op::Gte => cmp(&first()) != Ordering::Less,
        Op::Lt => cmp(&first()) == Ordering::Less,
        Op::Lte => cmp(&first()) != Ordering::Greater,
        Op::In => cond.values.iter().any(|v| cmp(v) == Ordering::Equal),
        Op::NotIn => !cond.values.iter().any(|v| cmp(v) == Ordering::Equal),
        Op::Like => like(&value, &first(), false),
        Op::NotLike => !like(&value, &first(), false),
        Op::Ilike => like(&value, &first(), true),
        Op::IsNull => value.is_null(),
        Op::IsNotNull => !value.is_null(),
        Op::Between => {
            cmp(&first()) != Ordering::Less
                && cmp(&cond.values.get(1).cloned().unwrap_or(Value::Null)) != Ordering::Greater
        }
        Op::NotBetween => {
            cmp(&first()) == Ordering::Less
                || cmp(&cond.values.get(1).cloned().unwrap_or(Value::Null)) == Ordering::Greater
        }
        Op::Contains => like(&value, &text_arg(|s| format!("%{s}%")), false),
        Op::StartsWith => like(&value, &text_arg(|s| format!("{s}%")), false),
        Op::EndsWith => like(&value, &text_arg(|s| format!("%{s}")), false),
    }
}

/// SQL `LIKE` semantics: `%` any sequence, `_` one character, no escapes;
/// `fold` switches to case-insensitive matching.
fn like(value: &Value, pattern: &Value, fold: bool) -> bool {
    let (Some(text), Some(pattern)) = (value.as_str(), pattern.as_str()) else {
        return false;
    };
    let norm = |s: &str| {
        if fold {
            s.to_lowercase().chars().collect::<Vec<char>>()
        } else {
            s.chars().collect()
        }
    };
    let (t, p) = (norm(text), norm(pattern));
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}
