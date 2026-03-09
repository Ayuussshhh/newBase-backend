//! Snapshot and Impact Analysis API Routes
//!
//! Routes for schema snapshots, diffs, and blast radius analysis.

use crate::auth::Claims;
use crate::error::AppError;
use crate::introspection::PostgresIntrospector;
use crate::snapshot::{BlastRadiusAnalyzer, DiffEngine, SchemaDiff};
use crate::state::SharedState;
use axum::{
    extract::{Extension, Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ==================== Request/Response Types ====================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSnapshotRequest {
    /// Optional label for the snapshot
    pub label: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    pub success: bool,
    pub message: String,
    pub snapshot: crate::introspection::SchemaSnapshot,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotListResponse {
    pub success: bool,
    pub snapshots: Vec<crate::snapshot::store::SnapshotMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffQuery {
    /// From version (defaults to previous)
    pub from_version: Option<u64>,
    /// To version (defaults to latest)
    pub to_version: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffResponse {
    pub success: bool,
    pub diff: SchemaDiff,
    pub rules_result: crate::snapshot::rules::RulesResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlastRadiusRequest {
    pub schema: String,
    pub table: String,
    pub column: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlastRadiusResponse {
    pub success: bool,
    pub blast_radius: crate::snapshot::BlastRadius,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesListResponse {
    pub success: bool,
    pub rules: Vec<crate::snapshot::Rule>,
}

// ==================== Handlers ====================

/// Create a new schema snapshot for a connection
#[axum::debug_handler]
pub async fn create_snapshot(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
    Json(_req): Json<CreateSnapshotRequest>,
) -> Result<Json<SnapshotResponse>, AppError> {
    // Get the connection
    let pool = state.connections.get_pool(connection_id).await?;
    
    // Introspect current schema
    let snapshot = PostgresIntrospector::introspect(&pool, connection_id).await?;
    
    // Save the snapshot (auto-increments version)
    let snapshot = state.snapshots.save(snapshot).await?;
    
    tracing::info!(
        "User {} created snapshot v{} for connection {}",
        claims.sub,
        snapshot.version,
        connection_id
    );
    
    Ok(Json(SnapshotResponse {
        success: true,
        message: format!("Snapshot v{} created successfully", snapshot.version),
        snapshot,
    }))
}

/// List all snapshots for a connection
pub async fn list_snapshots(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
) -> Result<Json<SnapshotListResponse>, AppError> {
    let snapshots = state.snapshots.list(connection_id).await;
    
    Ok(Json(SnapshotListResponse {
        success: true,
        snapshots,
    }))
}

/// Get the latest snapshot for a connection
pub async fn get_latest_snapshot(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
) -> Result<Json<SnapshotResponse>, AppError> {
    let snapshot = state.snapshots.get_latest(connection_id).await
        .ok_or_else(|| AppError::NotFound("No snapshots found for this connection".to_string()))?;
    
    Ok(Json(SnapshotResponse {
        success: true,
        message: format!("Latest snapshot v{}", snapshot.version),
        snapshot,
    }))
}

/// Get a specific snapshot version
pub async fn get_snapshot_version(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path((connection_id, version)): Path<(Uuid, u64)>,
) -> Result<Json<SnapshotResponse>, AppError> {
    let snapshot = state.snapshots.get_version(connection_id, version).await
        .ok_or_else(|| AppError::NotFound(format!("Snapshot v{} not found", version)))?;
    
    Ok(Json(SnapshotResponse {
        success: true,
        message: format!("Snapshot v{}", version),
        snapshot,
    }))
}

/// Compare two schema snapshots and show diff + rules violations
pub async fn diff_snapshots(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
    Query(query): Query<DiffQuery>,
) -> Result<Json<DiffResponse>, AppError> {
    // Get latest version
    let latest = state.snapshots.get_latest(connection_id).await
        .ok_or_else(|| AppError::NotFound("No snapshots found".to_string()))?;
    
    let to_version = query.to_version.unwrap_or(latest.version);
    let from_version = query.from_version.unwrap_or(to_version.saturating_sub(1));
    
    if from_version == 0 {
        return Err(AppError::BadRequest("Need at least 2 snapshots to compare".to_string()));
    }
    
    // Get both snapshots
    let (from_snapshot, to_snapshot) = state.snapshots
        .compare_versions(connection_id, from_version, to_version)
        .await?;
    
    // Compute diff
    let diff = DiffEngine::diff(&from_snapshot, &to_snapshot);
    
    // Evaluate rules against the diff
    let rules_result = state.rules.evaluate(&diff, &to_snapshot);
    
    Ok(Json(DiffResponse {
        success: true,
        diff,
        rules_result,
    }))
}

/// Analyze blast radius for a table or column
pub async fn analyze_blast_radius(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
    Json(req): Json<BlastRadiusRequest>,
) -> Result<Json<BlastRadiusResponse>, AppError> {
    // Get the latest snapshot
    let snapshot = state.snapshots.get_latest(connection_id).await
        .ok_or_else(|| AppError::NotFound("No snapshots found. Create a snapshot first.".to_string()))?;
    
    // Analyze blast radius
    let blast_radius = if let Some(column) = req.column {
        BlastRadiusAnalyzer::analyze_column(&snapshot, &req.schema, &req.table, &column)
    } else {
        BlastRadiusAnalyzer::analyze_table(&snapshot, &req.schema, &req.table)
    };
    
    Ok(Json(BlastRadiusResponse {
        success: true,
        blast_radius,
    }))
}

/// Set baseline snapshot (mark as "production state")
pub async fn set_baseline(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path((connection_id, snapshot_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Only admins can set baselines
    if !claims.role.can_approve() {
        return Err(AppError::Forbidden("Only admins can set baselines".to_string()));
    }
    
    state.snapshots.set_baseline(connection_id, snapshot_id).await?;
    
    Ok(Json(serde_json::json!({
        "success": true,
        "message": "Baseline set successfully"
    })))
}

/// List all governance rules
pub async fn list_rules(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
) -> Result<Json<RulesListResponse>, AppError> {
    let rules = state.rules.list_rules().to_vec();
    
    Ok(Json(RulesListResponse {
        success: true,
        rules,
    }))
}

/// Compare current live schema against baseline
pub async fn check_drift(
    State(state): State<SharedState>,
    Extension(_claims): Extension<Claims>,
    Path(connection_id): Path<Uuid>,
) -> Result<Json<DiffResponse>, AppError> {
    // Get baseline
    let baseline = state.snapshots.get_baseline(connection_id).await
        .ok_or_else(|| AppError::NotFound("No baseline set. Set a baseline first.".to_string()))?;
    
    // Get current live schema
    let pool = state.connections.get_pool(connection_id).await?;
    let current = PostgresIntrospector::introspect(&pool, connection_id).await?;
    
    // Compute drift
    let diff = DiffEngine::diff(&baseline, &current);
    let rules_result = state.rules.evaluate(&diff, &current);
    
    Ok(Json(DiffResponse {
        success: true,
        diff,
        rules_result,
    }))
}
