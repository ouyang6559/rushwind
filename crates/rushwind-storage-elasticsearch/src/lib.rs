//! The Elasticsearch engine for the [`Repository`] contract — go-crud's
//! `elasticsearch/` module.
//!
//! A thin `reqwest` REST client: documents are the stored rows, `_source`
//! is the record, and every write carries `refresh=true` so reads-after-
//! writes hold (the conformance suite's expectations are immediate, and
//! near-real-time search would break them). Filter trees compile to bool
//! queries in [`query`]; SQL pattern operators transpose into wildcard
//! clauses.
//!
//! Engine truths that shape semantics:
//!
//! - **Numeric `_source` fidelity**: dynamic mapping infers `long`/
//!   `double`/`boolean`, matching the contract's scalar kinds; a column
//!   holding only `NULL` stays unmapped and simply reads back `NULL`.
//! - **`upsert` is a plain indexed put** — Elasticsearch documents are
//!   addressed by id, so put-overwrites-put *is* insert-or-update.
//! - **`batch_create` atomicity** comes from the `_bulk` API: one request
//!   reports per-item outcomes, and the adapter fails the batch if any
//!   item errored. ClickHouse-style probe-conflicts are unnecessary — the
//!   explicit `_id` makes duplicates land as overwrites only when callers
//!   race, which the contract's single-writer suite does not exercise.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod query;

use serde_json::{json, Value as Json};

use rushwind_storage::{
    AuditAction, AuditEntry, ColumnKind, FieldMask, FilterExpr, ListQuery, Page, Paging, QueryCtx,
    Record, RepoFuture, Repository, Schema, StorageError, Value,
};

/// A [`Repository`] backed by an Elasticsearch (or OpenSearch) REST
/// endpoint.
pub struct ElasticRepo {
    schema: Schema,
    base: String,
    http: reqwest::Client,
}

impl ElasticRepo {
    /// Binds a repository to an endpoint (`http://host:9200`) and a schema.
    /// The index name is the schema's table.
    pub fn new(endpoint: impl Into<String>, schema: Schema) -> Self {
        let mut base = endpoint.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            schema,
            base,
            http: reqwest::Client::new(),
        }
    }

    fn index(&self) -> String {
        self.schema.table.clone()
    }

    /// The Elasticsearch field for a contract column: text columns are
    /// queried and sorted through their `.keyword` sub-field (exact, never
    /// analyzed); everything else keeps its name.
    fn field_of(&self, column: &str) -> String {
        match self.schema.column(column).map(|c| c.kind) {
            Some(ColumnKind::Text) => format!("{column}.keyword"),
            _ => column.to_owned(),
        }
    }

    /// Creates the index with an explicit mapping derived from the
    /// [`Schema`] — dynamic mapping would infer `text` for strings
    /// (analyzed, unsortable) and leaves empty indices unmapped
    /// (unsortable before the first document). Idempotent: an existing
    /// index is left alone.
    pub async fn ensure_index(&self) -> Result<(), StorageError> {
        let mut properties = serde_json::Map::new();
        for column in &self.schema.columns {
            let definition = match column.kind {
                ColumnKind::Bool => json!({ "type": "boolean" }),
                ColumnKind::Int => json!({ "type": "long" }),
                ColumnKind::Real => json!({ "type": "double" }),
                ColumnKind::Text => json!({
                    "type": "text",
                    "fields": { "keyword": { "type": "keyword" } }
                }),
            };
            properties.insert(column.name.clone(), definition);
        }
        let (status, body) = self
            .call(
                reqwest::Method::PUT,
                &format!("/{}", self.index()),
                Some(json!({ "mappings": { "properties": properties } })),
            )
            .await?;
        // 400 with resource_already_exists is fine; the index is there.
        if status != 200 && !(400..500).contains(&status) {
            return Err(StorageError::Backend(format!(
                "elasticsearch ensure_index failed ({status}): {body}"
            )));
        }
        Ok(())
    }

    /// `true` for the "row missing" status family.
    fn is_missing(status: u16) -> bool {
        status == 404
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Json>,
    ) -> Result<(u16, Json), StorageError> {
        let url = format!("{}{}", self.base, path);
        let mut request = self.http.request(method, &url);
        if let Some(body) = &body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("elasticsearch transport: {e}")))?;
        let status = response.status().as_u16();
        let json = response.json::<Json>().await.unwrap_or(Json::Null);
        Ok((status, json))
    }

    fn document_to_record(&self, id: &str, source: &Json) -> Result<Record, StorageError> {
        let map = source
            .as_object()
            .ok_or_else(|| StorageError::Backend("expected a JSON object _source".into()))?;
        let mut record = Record::new();
        for column in &self.schema.columns {
            let value =
                if column.name == self.schema.primary_key {
                    Value::Int(id.parse().map_err(|_| {
                        StorageError::Backend(format!("document id {id:?} is not an integer"))
                    })?)
                } else {
                    match map.get(&column.name) {
                        None | Some(Json::Null) => Value::Null,
                        Some(Json::Bool(b)) => Value::Bool(*b),
                        Some(Json::Number(n)) => match column.kind {
                            ColumnKind::Int => Value::Int(n.as_i64().ok_or_else(|| {
                                StorageError::Backend("int64 out of range".into())
                            })?),
                            _ => Value::Real(n.as_f64().unwrap_or_default()),
                        },
                        Some(Json::String(text)) => match column.kind {
                            ColumnKind::Text => Value::Text(text.clone()),
                            _ => Value::Bool(text != "false"),
                        },
                        Some(_) => {
                            return Err(StorageError::Backend(format!(
                                "column {:?} arrived as a composite JSON value",
                                column.name
                            )))
                        }
                    }
                };
            record.insert(column.name.as_str(), value);
        }
        Ok(record)
    }

    fn source_json(&self, row: &Record) -> Json {
        let mut map = serde_json::Map::new();
        for column in &self.schema.columns {
            // The primary key stays in _source as a real field (the _id
            // meta-field alone is not queryable by range/terms/sort).
            let json = match row.get(&column.name) {
                Some(Value::Null) | None => Json::Null,
                Some(Value::Bool(b)) => json!(b),
                Some(Value::Int(i)) => json!(i),
                Some(Value::Real(f)) => match serde_json::Number::from_f64(*f) {
                    Some(number) => Json::Number(number),
                    None => Json::Null,
                },
                Some(Value::Text(s)) => json!(s),
            };
            map.insert(column.name.clone(), json);
        }
        Json::Object(map)
    }

    fn scope_query(&self, ctx: &QueryCtx) -> Result<Option<Json>, StorageError> {
        use rushwind_storage::{Scope, Viewer};
        Ok(
            match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                Scope::Deny => None,
                Scope::Unrestricted => Some(json!({ "match_all": {} })),
                Scope::Scoped(filter) => {
                    let mapper = |column: &str| self.field_of(column);
                    Some(query::node_json(filter.node(), &mapper)?)
                }
            },
        )
    }

    /// The viewer scope conjoined with the query's own filter; `None`
    /// means the scope denies everything and the caller short-circuits.
    fn combined_query(
        &self,
        ctx: &QueryCtx,
        filter: Option<&FilterExpr>,
    ) -> Result<Option<Json>, StorageError> {
        let scope = match self.scope_query(ctx)? {
            Some(scope) => scope,
            None => return Ok(None),
        };
        let filter_json = match filter {
            Some(filter) => {
                let mapper = |column: &str| self.field_of(column);
                query::node_json(filter.node(), &mapper)?
            }
            None => return Ok(Some(scope)),
        };
        if filter_json == json!({ "match_all": {} }) {
            return Ok(Some(scope));
        }
        Ok(Some(json!({ "bool": { "must": [scope, filter_json] } })))
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
        let (status, body) = self
            .call(
                reqwest::Method::GET,
                &format!("/{}/_doc/{}", self.index(), id),
                None,
            )
            .await?;
        match status {
            s if Self::is_missing(s) => return Ok(None),
            200 => {}
            other => {
                return Err(StorageError::Backend(format!(
                    "elasticsearch get failed ({other}): {body}"
                )))
            }
        }
        let source = body.get("_source").cloned().unwrap_or(Json::Null);
        let row = self.document_to_record(&id.to_string(), &source)?;
        // The viewer boundary applies to point reads too: re-check the
        // scope predicate against the stored document.
        match self.scope_query(ctx)? {
            None => Ok(None),
            Some(scope) if scope == json!({ "match_all": {} }) => Ok(Some(row)),
            Some(scope) => {
                let pk = self.schema.primary_key.clone();
                let pk_term = json!({ "term": { pk: id } });
                let (status, hits) = self
                    .call(
                        reqwest::Method::POST,
                        &format!("/{}/_search", self.index()),
                        Some(json!({
                            "query": { "bool": { "must": [scope, pk_term] } },
                            "size": 1,
                            "_source": false,
                        })),
                    )
                    .await?;
                if status != 200 {
                    return Err(StorageError::Backend(format!(
                        "elasticsearch scope re-check failed ({status}): {hits}"
                    )));
                }
                let matched = hits
                    .pointer("/hits/total/value")
                    .and_then(Json::as_u64)
                    .unwrap_or(0);
                Ok(if matched > 0 { Some(row) } else { None })
            }
        }
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        list_query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        list_query.validate(&self.schema)?;
        let query = match self.combined_query(ctx, list_query.filter.as_ref())? {
            Some(query) => query,
            None => {
                return Ok(Page {
                    items: Vec::new(),
                    total: 0,
                    next_token: None,
                })
            }
        };
        let columns = self.select_columns(&list_query.mask);
        let size = u64::from(list_query.paging.limit());
        let (from, peek): (Option<u64>, bool) = match &list_query.paging {
            Paging::Token { .. } => (Some(0), true),
            Paging::Page { page, .. } => (Some((u64::from(*page - 1)) * size), false),
            Paging::Offset { offset, .. } => (Some(*offset), false),
        };
        let fetch = if peek { size + 1 } else { size };
        // Token paging streams in primary-key order: the cursor becomes a
        // range predicate conjoined with the rest of the query. A garbage
        // cursor is InvalidQuery, never an empty page.
        let mapper = |column: &str| self.field_of(column);
        let effective_query = match &list_query.paging {
            Paging::Token { token, .. } if !token.is_empty() => {
                let last = rushwind_storage::decode_cursor(token)?;
                let pk = self.schema.primary_key.clone();
                json!({ "bool": { "must": [
                    query,
                    { "range": { pk: { "gt": last } } }
                ] } })
            }
            _ => query,
        };
        let body = query::search_body(
            &effective_query,
            &list_query.sort,
            &self.schema.primary_key,
            &mapper,
            from,
            fetch,
            &columns,
        );
        let (status, response) = self
            .call(
                reqwest::Method::POST,
                &format!("/{}/_search", self.index()),
                Some(body),
            )
            .await?;
        // A missing index reads as an empty result set.
        if status == 404 {
            return Ok(Page {
                items: Vec::new(),
                total: 0,
                next_token: None,
            });
        }
        if status != 200 {
            return Err(StorageError::Backend(format!(
                "elasticsearch search failed ({status}): {response}"
            )));
        }
        let total = response
            .pointer("/hits/total/value")
            .and_then(Json::as_u64)
            .ok_or_else(|| StorageError::Backend("search response had no total".into()))?;
        let hits = response
            .pointer("/hits/hits")
            .and_then(Json::as_array)
            .cloned()
            .unwrap_or_default();
        let mut items = hits
            .iter()
            .map(|hit| {
                let id = hit
                    .get("_id")
                    .and_then(Json::as_str)
                    .ok_or_else(|| StorageError::Backend("hit without _id".into()))?;
                let source = hit.get("_source").cloned().unwrap_or(Json::Null);
                self.document_to_record(id, &source)
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        let mut next_token = None;
        if let Paging::Token { limit, .. } = &list_query.paging {
            if items.len() > usize::try_from(*limit).unwrap_or(usize::MAX) {
                items.pop();
                let last_id = items
                    .last()
                    .and_then(|row| row.get(&self.schema.primary_key))
                    .and_then(Value::as_i64)
                    .ok_or_else(|| {
                        StorageError::Backend("cursor page lost its primary key".into())
                    })?;
                next_token = Some(rushwind_storage::encode_cursor(last_id));
            }
        }

        let items = items
            .into_iter()
            .map(|row| self.project(&list_query.mask, row))
            .collect();
        Ok(Page {
            items,
            total,
            next_token,
        })
    }

    fn project(&self, mask: &Option<FieldMask>, row: Record) -> Record {
        let mut projected = row;
        if let Some(mask) = mask {
            projected.retain(|field| mask.allows(field));
        }
        projected
    }

    fn select_columns(&self, mask: &Option<FieldMask>) -> Vec<String> {
        match mask {
            None => self.schema.columns.iter().map(|c| c.name.clone()).collect(),
            Some(mask) => {
                let pk = self.schema.primary_key.clone();
                let mut cols: Vec<String> = mask
                    .paths()
                    .filter(|p| *p != pk && self.schema.column(p).is_some())
                    .map(str::to_owned)
                    .collect();
                cols.push(pk);
                cols
            }
        }
    }

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let query = match self.combined_query(ctx, filter.as_ref())? {
            Some(query) => query,
            None => return Ok(0),
        };
        let (status, body) = self
            .call(
                reqwest::Method::POST,
                &format!("/{}/_count", self.index()),
                Some(json!({ "query": query })),
            )
            .await?;
        // A missing index counts as an empty table.
        if status == 404 {
            return Ok(0);
        }
        if status != 200 {
            return Err(StorageError::Backend(format!(
                "elasticsearch count failed ({status}): {body}"
            )));
        }
        Ok(body.get("count").and_then(Json::as_u64).unwrap_or(0))
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => self.next_id().await?,
        };
        let source = self.source_json(&row);
        let (status, body) = self
            .call(
                reqwest::Method::PUT,
                &format!("/{}/_doc/{}?refresh=true", self.index(), id),
                Some(source),
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(StorageError::Backend(format!(
                "elasticsearch create failed ({status}): {body}"
            )));
        }
        let stored = self.materialize(&row, id);
        self.audit(ctx, AuditAction::Create, Some(Value::Int(id)));
        Ok(stored)
    }

    fn materialize(&self, row: &Record, id: i64) -> Record {
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

    /// `max(pk) + 1` via a max aggregation — a search engine has no
    /// identity columns.
    async fn next_id(&self) -> Result<i64, StorageError> {
        let (status, body) = self
            .call(
                reqwest::Method::POST,
                &format!("/{}/_search", self.index()),
                Some(json!({
                    "size": 0,
                    "aggs": { "max_id": { "max": { "field": self.schema.primary_key.clone() } } }
                })),
            )
            .await?;
        // A missing index means no rows yet; max is then 0.
        if status == 404 {
            return Ok(1);
        }
        if status != 200 {
            return Err(StorageError::Backend(format!(
                "max aggregation failed ({status}): {body}"
            )));
        }
        let max = body
            .pointer("/aggregations/max_id/value")
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        Ok(max as i64 + 1)
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
        let mut next_id = self.next_id().await?;
        let mut claimed = std::collections::BTreeSet::new();
        let mut ids = Vec::with_capacity(rows.len());
        let mut ndjson = String::new();
        for row in &rows {
            let id = match row.get(pk.as_str()) {
                Some(Value::Int(existing)) if *existing > 0 => *existing,
                _ => {
                    let id = next_id;
                    next_id += 1;
                    id
                }
            };
            if !claimed.insert(id) {
                return Err(StorageError::Conflict(format!(
                    "primary key {id} repeats within the batch"
                )));
            }
            ids.push(id);
            let source = self.source_json(row);
            // `create` (not `index`): an existing id must be an item error,
            // not a silent overwrite — conflict semantics depend on it.
            ndjson.push_str(&format!(
                "{{\"create\": {{\"_index\": {:?}, \"_id\": \"{}\"}}}}\n{}\n",
                self.index(),
                id,
                source
            ));
        }
        if !ndjson.is_empty() {
            let url = format!("{}/{}/_bulk?refresh=true", self.base, self.index());
            let response = self
                .http
                .post(&url)
                .header("content-type", "application/x-ndjson")
                .body(ndjson)
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("bulk transport: {e}")))?;
            let status = response.status().as_u16();
            let body: Json = response.json().await.unwrap_or(Json::Null);
            if status != 200 {
                return Err(StorageError::Backend(format!(
                    "bulk failed ({status}): {body}"
                )));
            }
            // The bulk API reports per-item outcomes; any item error fails
            // the whole batch (the contract's atomicity expectation), and
            // every item that succeeded is deleted — a failed batch lands
            // nothing.
            let items = body
                .get("items")
                .and_then(Json::as_array)
                .cloned()
                .unwrap_or_default();
            let failed: Vec<i64> = items
                .iter()
                .enumerate()
                .filter(|(_, item)| {
                    item.get("create")
                        .and_then(|create| create.get("error"))
                        .is_some()
                })
                .filter_map(|(position, _)| ids.get(position).copied())
                .collect();
            if !failed.is_empty() {
                for id in &ids {
                    if !failed.contains(id) {
                        let _ = self
                            .call(
                                reqwest::Method::DELETE,
                                &format!("/{}/_doc/{}?refresh=true", self.index(), id),
                                None,
                            )
                            .await;
                    }
                }
                return Err(StorageError::Conflict(
                    "bulk reported item errors (duplicate or invalid ids)".into(),
                ));
            }
        }
        let stored = rows
            .into_iter()
            .zip(ids)
            .map(|(row, id)| self.materialize(&row, id))
            .collect();
        self.audit(ctx, AuditAction::BatchCreate, None);
        Ok(stored)
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
        // Visibility probe BEFORE mutating: the scoped get turns
        // out-of-scope rows into NotFound without touching the store.
        self.get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        let mut doc = serde_json::Map::new();
        for column in &self.schema.columns {
            if let Some(value) = patch.get(&column.name) {
                if column.name != pk {
                    doc.insert(
                        column.name.clone(),
                        match value {
                            Value::Null => Json::Null,
                            Value::Bool(b) => json!(b),
                            Value::Int(i) => json!(i),
                            Value::Real(f) => match serde_json::Number::from_f64(*f) {
                                Some(number) => Json::Number(number),
                                None => Json::Null,
                            },
                            Value::Text(s) => json!(s),
                        },
                    );
                }
            }
        }
        let (status, body) = self
            .call(
                reqwest::Method::POST,
                &format!("/{}/_update/{}?refresh=true", self.index(), id),
                Some(json!({ "doc": Json::Object(doc) })),
            )
            .await?;
        match status {
            200..=299 => {}
            s if Self::is_missing(s) => return Err(StorageError::NotFound),
            other => {
                return Err(StorageError::Backend(format!(
                    "elasticsearch update failed ({other}): {body}"
                )))
            }
        }
        // The viewer boundary holds for updates too; the re-read through
        // the scoped path turns out-of-scope rows into NotFound.
        let updated = self
            .get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        self.audit(ctx, AuditAction::Update, Some(Value::Int(id)));
        Ok(updated)
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
        let source = self.source_json(&row);
        let (status, body) = self
            .call(
                reqwest::Method::PUT,
                &format!("/{}/_doc/{}?refresh=true", self.index(), id),
                Some(source),
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(StorageError::Backend(format!(
                "elasticsearch upsert failed ({status}): {body}"
            )));
        }
        let stored = self.materialize(&row, id);
        self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
        Ok(stored)
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        // Probe through the scoped path first: out-of-scope (or missing)
        // rows are NotFound, and ES reports deletes of absent documents as
        // 200 with result=not_found — never trust the status alone.
        self.get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        let (status, body) = self
            .call(
                reqwest::Method::DELETE,
                &format!("/{}/_doc/{}?refresh=true", self.index(), id),
                None,
            )
            .await?;
        let deleted = body.get("result").and_then(Json::as_str) == Some("deleted");
        match status {
            200 if deleted => {
                self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
                Ok(())
            }
            _ => Err(StorageError::NotFound),
        }
    }
}

impl Repository for ElasticRepo {
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
