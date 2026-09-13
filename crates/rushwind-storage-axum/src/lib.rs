//! The axum endpoint layer for the [`Repository`] contract — the last mile
//! of the list-query story: HTTP in, rows out.
//!
//! [`CrudApi::new`] binds a repository; [`CrudApi::router`] mounts the CRUD
//! surface, ready to nest under any path:
//!
//! ```ignore
//! let app = Router::new().nest("/api/widgets", CrudApi::new(repo).router());
//! ```
//!
//! # Routes
//!
//! | method + path | behavior |
//! |:---|:---|
//! | `GET /` | list — query vocabulary below |
//! | `POST /` | create — JSON object body, schema-validated |
//! | `GET /{id}` | point read |
//! | `PATCH /{id}` | update by patch |
//! | `PUT /{id}` | upsert |
//! | `DELETE /{id}` | delete |
//!
//! # The list-query vocabulary
//!
//! `GET /?q={...}` takes a **protojson** `PagingRequest` document verbatim
//! (the same bytes a Go client sends). For ad-hoc callers the individual
//! parameters spell the same request:
//!
//! - `filter` — AIP text (`age >= 10 AND name LIKE "a%"`)
//! - `page`+`size`, or `offset`+`limit`, or `token`+`limit`
//! - `sort` — `name:desc,age:asc`
//! - `fields` — comma-separated field mask (`id,name`)
//!
//! # Tenancy
//!
//! Without configuration every request runs all-access (demos and
//! bootstrap). Production tenancy plugs in through
//! [`CrudApi::with_viewer`]: a function from request headers to a
//! [`Viewer`], applied on every call — the same scope enforcement the
//! conformance suite pins, now at the HTTP edge.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod json;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use rushwind_storage::{
    Auditor, FilterExpr, ListQuery, Paging, QueryCtx, Repository, Sort, SortDir, StorageError,
    Value, Viewer,
};
use rushwind_storage_proto::wire::list_query_from_json;

/// The per-request viewer policy: headers in, tenancy scope out.
pub type ViewerFn = Arc<dyn Fn(&HeaderMap) -> Viewer + Send + Sync>;

/// The CRUD endpoint surface bound to one repository.
#[derive(Clone)]
pub struct CrudApi {
    repo: Arc<dyn Repository>,
    viewer: Option<ViewerFn>,
    auditor: Option<Arc<dyn Auditor>>,
}

impl CrudApi {
    /// Binds the surface to a repository; every request runs all-access.
    pub fn new(repo: Arc<dyn Repository>) -> Self {
        Self {
            repo,
            viewer: None,
            auditor: None,
        }
    }

    /// Installs the tenancy policy — headers to viewer, enforced on every
    /// call by the engine underneath.
    pub fn with_viewer(mut self, viewer: ViewerFn) -> Self {
        self.viewer = Some(viewer);
        self
    }

    /// Attaches the audit sink mutations flow into (the contract's
    /// [`Auditor`] hook, fed by the engine on every write).
    pub fn with_auditor(mut self, auditor: Arc<dyn Auditor>) -> Self {
        self.auditor = Some(auditor);
        self
    }

    /// The mounted CRUD router; nest it under your API prefix.
    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(list).post(create))
            .route(
                "/{id}",
                get(get_one).patch(update).put(upsert).delete(delete_one),
            )
            .with_state(self)
    }

    fn ctx(&self, headers: &HeaderMap) -> QueryCtx {
        let viewer = self
            .viewer
            .as_ref()
            .map(|policy| policy(headers))
            .unwrap_or_else(Viewer::all);
        match &self.auditor {
            Some(auditor) => QueryCtx::new(viewer).audited(Arc::clone(auditor)),
            None => QueryCtx::new(viewer),
        }
    }
}

// ---- handlers --------------------------------------------------------------

async fn list(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let query = match build_list_query(&params) {
        Ok(query) => query,
        Err(error) => return error_response(error),
    };
    match api.repo.list(api.ctx(&headers), &query).await {
        Ok(page) => {
            let items: Vec<serde_json::Value> =
                page.items.iter().map(json::record_to_json).collect();
            (
                StatusCode::OK,
                Json(json!({
                    "items": items,
                    "total": page.total,
                    "nextToken": page.next_token,
                })),
            )
                .into_response()
        }
        Err(error) => error_response(error),
    }
}

async fn create(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(row) => match api.repo.create(api.ctx(&headers), row).await {
            Ok(stored) => {
                (StatusCode::CREATED, Json(json::record_to_json(&stored))).into_response()
            }
            Err(error) => error_response(error),
        },
        Err(error) => error_response(error),
    }
}

async fn get_one(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match api.repo.get(api.ctx(&headers), Value::Int(id)).await {
        Ok(Some(row)) => (StatusCode::OK, Json(json::record_to_json(&row))).into_response(),
        Ok(None) => error_response(StorageError::NotFound),
        Err(error) => error_response(error),
    }
}

async fn update(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(patch) => match api
            .repo
            .update(api.ctx(&headers), Value::Int(id), patch)
            .await
        {
            Ok(stored) => (StatusCode::OK, Json(json::record_to_json(&stored))).into_response(),
            Err(error) => error_response(error),
        },
        Err(error) => error_response(error),
    }
}

async fn upsert(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(mut row) => {
            row.insert(api.repo.schema().primary_key.as_str(), Value::Int(id));
            match api.repo.upsert(api.ctx(&headers), row).await {
                Ok(stored) => (StatusCode::OK, Json(json::record_to_json(&stored))).into_response(),
                Err(error) => error_response(error),
            }
        }
        Err(error) => error_response(error),
    }
}

async fn delete_one(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match api.repo.delete(api.ctx(&headers), Value::Int(id)).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(error),
    }
}

// ---- helpers ---------------------------------------------------------------

fn parse_id(raw: &str) -> Option<i64> {
    raw.parse::<i64>().ok()
}

fn error_response(error: StorageError) -> Response {
    let status = match &error {
        StorageError::NotFound => StatusCode::NOT_FOUND,
        StorageError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        StorageError::Conflict(_) => StatusCode::CONFLICT,
        StorageError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        StorageError::Timeout | StorageError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

/// Builds the list query: a `q` protojson document wins; otherwise the
/// individual parameters spell it — `filter` in AIP text, `sort` as
/// `field:dir` pairs, `fields` as the comma-separated mask.
fn build_list_query(params: &HashMap<String, String>) -> Result<ListQuery, StorageError> {
    if let Some(document) = params.get("q") {
        return list_query_from_json(document);
    }
    let mut query = match (
        params.get("token"),
        params.get("offset"),
        params.get("page"),
    ) {
        (Some(token), _, _) => ListQuery {
            paging: Paging::Token {
                token: token.clone(),
                limit: limit_of(params)?,
            },
            ..ListQuery::default()
        },
        (_, Some(offset), _) => ListQuery {
            paging: Paging::Offset {
                offset: offset
                    .parse()
                    .map_err(|_| StorageError::InvalidQuery("offset must be an integer".into()))?,
                limit: limit_of(params)?,
            },
            ..ListQuery::default()
        },
        (_, _, page) => ListQuery {
            paging: Paging::Page {
                page: match page {
                    Some(page) => page.parse().map_err(|_| {
                        StorageError::InvalidQuery("page must be an integer".into())
                    })?,
                    None => 1,
                },
                size: limit_of(params)?,
            },
            ..ListQuery::default()
        },
    };
    if let Some(filter) = params.get("filter") {
        query.filter = Some(FilterExpr::from_aip(filter)?);
    }
    if let Some(sort) = params.get("sort") {
        let mut fields = Vec::new();
        for term in sort.split(',') {
            let term = term.trim();
            let Some((field, dir)) = term.rsplit_once(':') else {
                return Err(StorageError::InvalidQuery(format!(
                    "sort term {term:?} must be field:asc or field:desc"
                )));
            };
            let dir = match dir.to_ascii_lowercase().as_str() {
                "asc" => SortDir::Asc,
                "desc" => SortDir::Desc,
                other => {
                    return Err(StorageError::InvalidQuery(format!(
                        "sort direction {other:?} must be asc or desc"
                    )))
                }
            };
            fields.push(rushwind_storage::SortField {
                field: field.trim().to_owned(),
                dir,
            });
        }
        query.sort = Sort { fields };
    }
    if let Some(fields) = params.get("fields") {
        query.mask = Some(rushwind_storage::FieldMask::of(
            fields.split(',').map(str::trim).filter(|f| !f.is_empty()),
        ));
    }
    Ok(query)
}

fn limit_of(params: &HashMap<String, String>) -> Result<u32, StorageError> {
    let raw = params
        .get("size")
        .or_else(|| params.get("limit"))
        .map(String::as_str)
        .unwrap_or("20");
    raw.parse::<u32>()
        .map_err(|_| StorageError::InvalidQuery("page size must be an integer".into()))
}
