//! The MongoDB engine for the [`Repository`] contract — go-crud's
//! `mongodb/` module.
//!
//! The [`translate`] module compiles the contract's filter tree into BSON
//! predicate documents (SQL-wildcard `LIKE` family becomes escaped,
//! anchored regex; the case-insensitive derived family sets the `i` flag)
//! and is unit-tested entirely offline. This module is the thin collection
//! glue: find/count/insert/update/delete against
//! `Collection<Document>`, viewer scopes conjoined into every filter,
//! token cursors as `{pk: {$gt: last}}` with a one-document peek.
//!
//! Two MongoDB-specific caveats, both deliberate:
//!
//! - **Generated ids** come from `max(pk) + 1` at insert time — a document
//!   store has no rowid alias. Contention on concurrent creates is
//!   arbitrated by MongoDB's unique-index errors, surfaced as
//!   [`StorageError::Conflict`]; give the primary key a unique index
//!   (a production deployment has one) and this is sound.
//! - **`batch_create` atomicity** needs replica-set transactions; without
//!   one, `insert_many` is a single command but not strictly all-or-nothing.
//!   The live suite documents the difference by running against a
//!   single-node setup where possible.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod translate;

use mongodb::bson::{doc, Document};
use mongodb::Collection;

use rushwind_storage::{
    AuditAction, AuditEntry, FieldMask, FilterExpr, ListQuery, Page, Paging, QueryCtx, Record,
    RepoFuture, Repository, Schema, Scope, StorageError, Value, Viewer,
};

/// A [`Repository`] backed by one MongoDB collection.
pub struct MongoRepo {
    schema: Schema,
    collection: Collection<Document>,
}

impl MongoRepo {
    /// Binds a repository to a live collection.
    pub fn new(collection: Collection<Document>, schema: Schema) -> Self {
        Self { schema, collection }
    }

    /// Connects to MongoDB and binds a repository to `database`.`schema.table`.
    pub async fn connect(uri: &str, database: &str, schema: Schema) -> Result<Self, StorageError> {
        let client = mongodb::Client::with_uri_str(uri)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        let collection = client
            .database(database)
            .collection::<Document>(&schema.table);
        Ok(Self::new(collection, schema))
    }

    /// Drops the backing collection — the inverse of a fresh deployment,
    /// used by live-suite setups to guarantee a clean slate.
    pub async fn drop_collection(&self) -> Result<(), StorageError> {
        self.collection
            .drop()
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))
    }

    /// Ensures the unique index on the primary key that production
    /// deployments are expected to carry: without it MongoDB cannot reject
    /// duplicate ids, and conflict semantics (plus `batch_create`
    /// atomicity) do not hold. Idempotent.
    pub async fn ensure_primary_index(&self) -> Result<(), StorageError> {
        use mongodb::options::IndexOptions;
        let model = mongodb::IndexModel::builder()
            .keys(doc! { &self.schema.primary_key: 1i32 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    .name("rushwind_pk".to_owned())
                    .build(),
            )
            .build();
        self.collection
            .create_index(model)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    // ---- shared plumbing ---------------------------------------------------

    fn map_err(&self, err: mongodb::error::Error) -> StorageError {
        let text = err.to_string();
        if text.contains("E11000") || text.contains("duplicate key") {
            StorageError::Conflict(text)
        } else {
            StorageError::Backend(text)
        }
    }

    fn scope_doc(&self, ctx: &QueryCtx) -> Result<ScopeDoc, StorageError> {
        Ok(
            match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                Scope::Deny => ScopeDoc::Deny,
                Scope::Unrestricted => ScopeDoc::Pass(doc! {}),
                Scope::Scoped(filter) => ScopeDoc::Pass(translate::node_to_doc(filter.node())?),
            },
        )
    }

    /// The full predicate for a read: viewer scope conjoined with the
    /// query's filter, plus the token cursor when paging.
    fn read_filter(&self, scope: &ScopeDoc, query: &ListQuery) -> Result<Document, StorageError> {
        let mut parts: Vec<Document> = Vec::new();
        if let ScopeDoc::Pass(scope) = scope {
            if !scope.is_empty() {
                parts.push(scope.clone());
            }
        }
        if let Some(filter) = &query.filter {
            parts.push(translate::node_to_doc(filter.node())?);
        }
        if let Paging::Token { token, .. } = &query.paging {
            if !token.is_empty() {
                let last = rushwind_storage::decode_cursor(token)?;
                parts.push(doc! { &self.schema.primary_key: { "$gt": last } });
            }
        }
        Ok(match parts.len() {
            0 => doc! {},
            1 => parts.into_iter().next().expect("checked length"),
            _ => doc! { "$and": parts },
        })
    }

    fn document_to_record(&self, document: &Document) -> Result<Record, StorageError> {
        let mut record = Record::new();
        for column in &self.schema.columns {
            let value =
                translate::bson_to_value(&self.schema, &column.name, document.get(&column.name))?;
            record.insert(column.name.as_str(), value);
        }
        Ok(record)
    }

    fn project(&self, mask: &Option<FieldMask>, row: Record) -> Record {
        let mut projected = row;
        if let Some(mask) = mask {
            projected.retain(|field| mask.allows(field));
        }
        projected
    }

    fn validate_row(&self, row: &Record) -> Result<(), StorageError> {
        for (field, value) in row.iter() {
            let column = self.schema.column(field).ok_or_else(|| {
                StorageError::InvalidQuery(format!("unknown column {field:?} in write payload"))
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

    /// A full stored row: every declared column present, absent fields
    /// `NULL`, primary key filled in.
    fn materialize(&self, row: &Record, id: i64) -> Result<Document, StorageError> {
        let mut document = Document::new();
        for column in &self.schema.columns {
            let value = if column.name == self.schema.primary_key {
                Value::Int(id)
            } else {
                row.get(&column.name).cloned().unwrap_or(Value::Null)
            };
            document.insert(column.name.as_str(), translate::value_to_bson(&value));
        }
        Ok(document)
    }

    /// `max(pk) + 1` over the collection — a document store has no rowid
    /// alias to lean on.
    async fn next_id(&self) -> Result<i64, StorageError> {
        let pk = self.schema.primary_key.clone();
        let highest = self
            .collection
            .find_one(doc! {})
            .sort(doc! { &pk: -1 })
            .await
            .map_err(|e| self.map_err(e))?;
        Ok(highest
            .and_then(|document| document.get(&pk).and_then(|v| v.as_i64()))
            .unwrap_or_default()
            + 1)
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(AuditEntry::now(
            action,
            self.schema.table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }

    fn expect_id(&self, id: &Value) -> Result<i64, StorageError> {
        id.as_i64()
            .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
    }

    // ---- async implementations ----------------------------------------------

    async fn get_async(&self, ctx: &QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let id = self.expect_id(&id)?;
        let scope = self.scope_doc(ctx)?;
        if matches!(scope, ScopeDoc::Deny) {
            return Ok(None);
        }
        let mut filter = doc! { &self.schema.primary_key: id };
        if let ScopeDoc::Pass(scope) = &scope {
            if !scope.is_empty() {
                filter = doc! { "$and": [filter, scope.clone()] };
            }
        }
        let document = self
            .collection
            .find_one(filter)
            .await
            .map_err(|e| self.map_err(e))?;
        match document {
            Some(document) => Ok(Some(self.document_to_record(&document)?)),
            None => Ok(None),
        }
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        let scope = self.scope_doc(ctx)?;
        if matches!(scope, ScopeDoc::Deny) {
            return Ok(Page {
                items: Vec::new(),
                total: 0,
                next_token: None,
            });
        }
        let filter = self.read_filter(&scope, query)?;
        let pk = self.schema.primary_key.clone();

        let total = self
            .collection
            .count_documents(filter.clone())
            .await
            .map_err(|e| self.map_err(e))?;

        let limit = query.paging.limit();
        let mut find = self
            .collection
            .find(filter)
            .sort(translate::sort_to_doc(&query.sort, &pk));
        match &query.paging {
            Paging::Token { .. } => {
                // Peek one document past the page to learn whether the
                // stream continues.
                find = find.limit(i64::from(limit) + 1);
            }
            Paging::Page { page, .. } => {
                find = find
                    .skip(u64::from(*page - 1) * u64::from(limit))
                    .limit(i64::from(limit));
            }
            Paging::Offset { offset, .. } => {
                find = find.skip(*offset).limit(i64::from(limit));
            }
        }

        let mut cursor = find.await.map_err(|e| self.map_err(e))?;
        let mut items: Vec<Record> = Vec::new();
        while cursor.advance().await.map_err(|e| self.map_err(e))? {
            let document = cursor
                .deserialize_current()
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            items.push(self.document_to_record(&document)?);
        }

        let mut next_token = None;
        if let Paging::Token { limit, .. } = &query.paging {
            if items.len() > usize::try_from(*limit).unwrap_or(usize::MAX) {
                items.pop();
                let last_id = items
                    .last()
                    .and_then(|row| row.get(&pk))
                    .and_then(Value::as_i64)
                    .ok_or_else(|| {
                        StorageError::Backend("cursor page lost its primary key".into())
                    })?;
                next_token = Some(rushwind_storage::encode_cursor(last_id));
            }
        }

        let items = items
            .into_iter()
            .map(|row| self.project(&query.mask, row))
            .collect();
        Ok(Page {
            items,
            total,
            next_token,
        })
    }

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let scope = self.scope_doc(ctx)?;
        if matches!(scope, ScopeDoc::Deny) {
            return Ok(0);
        }
        let filter = self.read_filter(
            &scope,
            &ListQuery {
                filter,
                ..ListQuery::default()
            },
        )?;
        self.collection
            .count_documents(filter)
            .await
            .map_err(|e| self.map_err(e))
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => self.next_id().await?,
        };
        let document = self.materialize(&row, id)?;
        self.collection
            .insert_one(document)
            .await
            .map_err(|e| self.map_err(e))?;
        self.audit(ctx, AuditAction::Create, Some(Value::Int(id)));
        Ok(self.materialized_record(&row, id))
    }

    async fn batch_create_async(
        &self,
        ctx: &QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        for row in &rows {
            self.validate_row(row)?;
        }
        let pk = self.schema.primary_key.clone();
        let next_id = self.next_id().await?;
        let mut documents = Vec::with_capacity(rows.len());
        let mut ids = Vec::with_capacity(rows.len());
        let mut claimed = std::collections::BTreeSet::new();
        let mut generated = next_id;
        for row in &rows {
            let id = match row.get(pk.as_str()) {
                Some(Value::Int(existing)) if *existing > 0 => *existing,
                _ => {
                    let id = generated;
                    generated += 1;
                    id
                }
            };
            if !claimed.insert(id) {
                return Err(StorageError::Conflict(format!(
                    "primary key {id} repeats within the batch"
                )));
            }
            ids.push(id);
            documents.push(self.materialize(row, id)?);
        }
        if !documents.is_empty() {
            // Strict all-or-nothing needs a transaction — a replica-set
            // deployment (the CI mongo:7 runs as a single-node one). A bare
            // insert_many aborts on the unique-index clash but leaves
            // earlier documents of the batch in place.
            let mut session = self
                .collection
                .client()
                .start_session()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            session
                .start_transaction()
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
            let insert = self
                .collection
                .insert_many(documents)
                .session(&mut session)
                .await;
            match insert {
                Ok(_) => session
                    .commit_transaction()
                    .await
                    .map_err(|e| StorageError::Backend(e.to_string()))?,
                Err(error) => {
                    // Dropping the session aborts the transaction.
                    return Err(self.map_err(error));
                }
            }
        }
        let stored = rows
            .into_iter()
            .zip(ids)
            .map(|(row, id)| self.materialized_record(&row, id))
            .collect();
        self.audit(ctx, AuditAction::BatchCreate, None);
        Ok(stored)
    }

    /// The stored record a write returns: the payload materialized over the
    /// schema with the primary key filled — MongoDB echoes no server-side
    /// defaults, so this *is* the stored document.
    fn materialized_record(&self, row: &Record, id: i64) -> Record {
        let mut full = Record::new();
        for column in &self.schema.columns {
            let value = if column.name == self.schema.primary_key {
                Value::Int(id)
            } else {
                row.get(&column.name).cloned().unwrap_or(Value::Null)
            };
            full.insert(column.name.as_str(), value);
        }
        full
    }

    async fn update_async(
        &self,
        ctx: &QueryCtx,
        id: Value,
        patch: Record,
    ) -> Result<Record, StorageError> {
        self.validate_row(&patch)?;
        let id = self.expect_id(&id)?;
        let pk = self.schema.primary_key.clone();
        if let Some(new_pk) = patch.get(pk.as_str()) {
            if new_pk != &Value::Null && new_pk.as_i64() != Some(id) {
                return Err(StorageError::InvalidQuery(
                    "update patches cannot move the primary key".into(),
                ));
            }
        }
        let scope = self.scope_doc(ctx)?;
        if matches!(scope, ScopeDoc::Deny) {
            return Err(StorageError::NotFound);
        }
        let mut filter = doc! { &pk: id };
        if let ScopeDoc::Pass(scope_doc) = &scope {
            if !scope_doc.is_empty() {
                filter = doc! { "$and": [filter, scope_doc.clone()] };
            }
        }
        let mut sets = Document::new();
        for column in &self.schema.columns {
            if let Some(value) = patch.get(&column.name) {
                sets.insert(column.name.as_str(), translate::value_to_bson(value));
            }
        }
        if sets.is_empty() {
            // Nothing to change — the read below decides visibility.
        } else {
            let update = doc! { "$set": sets };
            let updated = self
                .collection
                .find_one_and_update(filter, update)
                .return_document(mongodb::options::ReturnDocument::After)
                .await
                .map_err(|e| self.map_err(e))?;
            let Some(document) = updated else {
                return Err(StorageError::NotFound);
            };
            drop(document);
        }
        match self.get_async(ctx, Value::Int(id)).await? {
            Some(row) => {
                self.audit(ctx, AuditAction::Update, Some(Value::Int(id)));
                Ok(row)
            }
            None => Err(StorageError::NotFound),
        }
    }

    async fn upsert_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => {
                return Err(StorageError::InvalidQuery(
                    "upsert requires an explicit primary key".into(),
                ))
            }
        };
        let document = self.materialize(&row, id)?;
        let filter = doc! { &pk: id };
        let replaced = self
            .collection
            .find_one_and_replace(filter, document)
            .upsert(true)
            .return_document(mongodb::options::ReturnDocument::After)
            .await
            .map_err(|e| self.map_err(e))?;
        let Some(document) = replaced else {
            return Err(StorageError::Backend("upsert produced no document".into()));
        };
        let stored = self.document_to_record(&document)?;
        self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
        Ok(stored)
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        let scope = self.scope_doc(ctx)?;
        if matches!(scope, ScopeDoc::Deny) {
            return Err(StorageError::NotFound);
        }
        let pk = self.schema.primary_key.clone();
        let mut filter = doc! { &pk: id };
        if let ScopeDoc::Pass(scope_doc) = &scope {
            if !scope_doc.is_empty() {
                filter = doc! { "$and": [filter, scope_doc.clone()] };
            }
        }
        let result = self
            .collection
            .delete_one(filter)
            .await
            .map_err(|e| self.map_err(e))?;
        if result.deleted_count == 0 {
            return Err(StorageError::NotFound);
        }
        self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

impl Repository for MongoRepo {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(async move { self.get_async(&ctx, id).await })
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        Box::pin(async move { self.list_async(&ctx, query).await })
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        Box::pin(async move { self.count_async(&ctx, filter).await })
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.create_async(&ctx, row).await })
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        Box::pin(async move { self.batch_create_async(&ctx, rows).await })
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.update_async(&ctx, id, patch).await })
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.upsert_async(&ctx, row).await })
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        Box::pin(async move { self.delete_async(&ctx, id).await })
    }
}

/// The viewer scope rendered for MongoDB: denied (empty everything) or the
/// predicate document to conjoin.
enum ScopeDoc {
    /// The viewer may see nothing; engines short-circuit.
    Deny,
    /// The scope predicate to conjoin (possibly the match-all `{}`).
    Pass(Document),
}
