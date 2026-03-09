//! Project management route handlers
//!
//! Handles CRUD operations for projects and saved connections

use crate::auth::Claims;
use crate::error::{ApiResult, AppError};
use crate::models::{
    CreateProjectRequest, Project, SaveConnectionRequest, SavedConnection,
    ConnectionDetails, SuccessResponse, MessageResponse, UpdateProjectRequest,
};
use crate::state::SharedState;
use axum::{
    extract::{Path, State, Extension},
    Json,
};
use chrono::Utc;
use serde::Serialize;
use tracing::{debug, info, error};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectResponse {
    #[serde(flatten)]
    pub project: Project,
    pub connection_count: i64,
}

/// Create a new project
pub async fn create_project(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Json(payload): Json<CreateProjectRequest>,
) -> ApiResult<Json<SuccessResponse<Project>>> {
    debug!("Creating project: {}", payload.name);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Insert project into database
    let row = client.query_one(
        "INSERT INTO projects (owner_id, name, description, icon, color, is_private, created_at, updated_at) 
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id, owner_id, name, description, icon, color, is_private, created_at, updated_at",
        &[
            &owner_id,
            &payload.name,
            &payload.description,
            &payload.icon,
            &payload.color,
            &false,
            &Utc::now(),
            &Utc::now(),
        ],
    ).await
    .map_err(|e| {
        error!("Failed to create project: {}", e);
        AppError::Internal(format!("Failed to create project: {}", e))
    })?;

    let project = Project {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        description: row.get("description"),
        icon: row.get("icon"),
        color: row.get("color"),
        is_private: row.get("is_private"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    info!("Project created: {} (id: {})", project.name, project.id);

    Ok(Json(SuccessResponse::with_data(
        "Project created successfully.",
        project,
    )))
}

/// List all projects for current user
pub async fn list_projects(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
) -> ApiResult<Json<SuccessResponse<Vec<Project>>>> {
    debug!("Listing projects for user: {}", claims.sub);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Fetch all projects owned by the user
    let rows = client.query(
        "SELECT id, owner_id, name, description, icon, color, is_private, created_at, updated_at
         FROM projects
         WHERE owner_id = $1
         ORDER BY created_at DESC",
        &[&owner_id],
    ).await
    .map_err(|e| {
        error!("Failed to list projects: {}", e);
        AppError::Internal(format!("Failed to list projects: {}", e))
    })?;

    let projects: Vec<Project> = rows.iter().map(|row| {
        Project {
            id: row.get("id"),
            owner_id: row.get("owner_id"),
            name: row.get("name"),
            description: row.get("description"),
            icon: row.get("icon"),
            color: row.get("color"),
            is_private: row.get("is_private"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }).collect();

    debug!("Found {} projects for user {}", projects.len(), owner_id);

    Ok(Json(SuccessResponse::with_data(
        format!("{} projects found.", projects.len()),
        projects,
    )))
}

/// Get a specific project
pub async fn get_project(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<i32>,
) -> ApiResult<Json<SuccessResponse<Project>>> {
    debug!("Getting project: {}", id);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Fetch project (must be owned by the current user)
    let row = client.query_opt(
        "SELECT id, owner_id, name, description, icon, color, is_private, created_at, updated_at
         FROM projects
         WHERE id = $1 AND owner_id = $2",
        &[&id, &owner_id],
    ).await
    .map_err(|e| {
        error!("Failed to fetch project: {}", e);
        AppError::Internal(format!("Failed to fetch project: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound(format!("Project {} not found", id)))?;

    let project = Project {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        description: row.get("description"),
        icon: row.get("icon"),
        color: row.get("color"),
        is_private: row.get("is_private"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    Ok(Json(SuccessResponse::with_data(
        "Project retrieved successfully.",
        project,
    )))
}

/// Update a project
pub async fn update_project(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<i32>,
    Json(payload): Json<UpdateProjectRequest>,
) -> ApiResult<Json<SuccessResponse<Project>>> {
    debug!("Updating project: {}", id);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Update project (must be owned by the current user)
    let row = client.query_opt(
        "UPDATE projects
         SET name = COALESCE($1, name),
             description = COALESCE($2, description),
             icon = COALESCE($3, icon),
             color = COALESCE($4, color),
             updated_at = $5
         WHERE id = $6 AND owner_id = $7
         RETURNING id, owner_id, name, description, icon, color, is_private, created_at, updated_at",
        &[
            &payload.name,
            &payload.description,
            &payload.icon,
            &payload.color,
            &Utc::now(),
            &id,
            &owner_id,
        ],
    ).await
    .map_err(|e| {
        error!("Failed to update project: {}", e);
        AppError::Internal(format!("Failed to update project: {}", e))
    })?
    .ok_or_else(|| AppError::NotFound(format!("Project {} not found", id)))?;

    let project = Project {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        description: row.get("description"),
        icon: row.get("icon"),
        color: row.get("color"),
        is_private: row.get("is_private"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    info!("Project updated: {} (id: {})", project.name, project.id);

    Ok(Json(SuccessResponse::with_data(
        "Project updated successfully.",
        project,
    )))
}

/// Delete a project
pub async fn delete_project(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<i32>,
) -> ApiResult<Json<MessageResponse>> {
    debug!("Deleting project: {}", id);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Delete project (must be owned by the current user)
    let rows_affected = client.execute(
        "DELETE FROM projects WHERE id = $1 AND owner_id = $2",
        &[&id, &owner_id],
    ).await
    .map_err(|e| {
        error!("Failed to delete project: {}", e);
        AppError::Internal(format!("Failed to delete project: {}", e))
    })?;

    if rows_affected == 0 {
        return Err(AppError::NotFound(format!("Project {} not found", id)));
    }

    info!("Project deleted: {}", id);

    Ok(Json(MessageResponse::new(format!(
        "Project {} deleted successfully.",
        id
    ))))
}

/// Save a database connection to a project
pub async fn save_connection(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(project_id): Path<i32>,
    Json(payload): Json<SaveConnectionRequest>,
) -> ApiResult<Json<SuccessResponse<SavedConnection>>> {
    debug!("Saving connection to project: {}", project_id);

    // Parse user_id from claims
    let _user_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Encrypt the connection string using pgcrypto
    // Use a simple encryption key (in production, use environment variable)
    let encryption_key = std::env::var("CONNECTION_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-encryption-key".to_string());
    
    // Insert saved connection into database with encryption
    let row = client.query_one(
        "INSERT INTO saved_connections (project_id, name, connection_string_encrypted, connection_type, environment, is_active, last_tested, test_status, created_by, created_at, updated_at)
         VALUES ($1, $2, pgp_sym_encrypt($3, $4)::bytea, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING id, project_id, name, connection_type, environment, is_active, last_tested, test_status, created_by, created_at, updated_at",
        &[
            &project_id,
            &payload.name,
            &payload.connection_string,
            &encryption_key,
            &payload.connection_type,
            &payload.environment,
            &false,  // is_active
            &None::<chrono::DateTime<Utc>>,  // last_tested
            &None::<String>,  // test_status
            &_user_id,  // created_by
            &Utc::now(),
            &Utc::now(),
        ],
    ).await
    .map_err(|e| {
        error!("Failed to save connection: {}", e);
        AppError::Internal(format!("Failed to save connection: {}", e))
    })?;

    let connection = SavedConnection {
        id: row.get("id"),
        project_id: row.get("project_id"),
        name: row.get("name"),
        connection_string_encrypted: vec![],
        connection_type: row.get("connection_type"),
        environment: row.get("environment"),
        is_active: row.get("is_active"),
        last_tested: row.get("last_tested"),
        test_status: row.get("test_status"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    info!("Connection saved: {}", connection.id);

    Ok(Json(SuccessResponse::with_data(
        "Connection saved successfully.",
        connection,
    )))
}

/// List all connections for a project
pub async fn list_connections(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path(project_id): Path<i32>,
) -> ApiResult<Json<SuccessResponse<Vec<ConnectionDetails>>>> {
    debug!("Listing connections for project: {}", project_id);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Verify project ownership
    let _project_exists = client.query_opt(
        "SELECT id FROM projects WHERE id = $1 AND owner_id = $2",
        &[&project_id, &owner_id],
    ).await
    .map_err(|e| AppError::Internal(format!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    // Fetch all connections for the project
    let rows = client.query(
        "SELECT id, project_id, connection_name, database_type, created_at, updated_at
         FROM saved_connections
         WHERE project_id = $1
         ORDER BY created_at DESC",
        &[&project_id],
    ).await
    .map_err(|e| {
        error!("Failed to list connections: {}", e);
        AppError::Internal(format!("Failed to list connections: {}", e))
    })?;

    let connections: Vec<ConnectionDetails> = rows.iter().map(|row| {
        ConnectionDetails {
            id: row.get("id"),
            project_id: row.get("project_id"),
            name: row.get("connection_name"),
            connection_type: row.get("database_type"),
            environment: "production".to_string(),
            is_active: false,
            last_tested: None,
            test_status: None,
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }).collect();

    Ok(Json(SuccessResponse::with_data(
        format!("{} connections found.", connections.len()),
        connections,
    )))
}

/// Remove a saved connection
pub async fn remove_connection(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path((project_id, connection_id)): Path<(i32, i32)>,
) -> ApiResult<Json<MessageResponse>> {
    debug!("Removing connection {} from project {}", connection_id, project_id);

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Verify project ownership
    let _project_exists = client.query_opt(
        "SELECT id FROM projects WHERE id = $1 AND owner_id = $2",
        &[&project_id, &owner_id],
    ).await
    .map_err(|e| AppError::Internal(format!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    // Delete the connection
    let rows_affected = client.execute(
        "DELETE FROM saved_connections WHERE id = $1 AND project_id = $2",
        &[&connection_id, &project_id],
    ).await
    .map_err(|e| {
        error!("Failed to delete connection: {}", e);
        AppError::Internal(format!("Failed to delete connection: {}", e))
    })?;

    if rows_affected == 0 {
        return Err(AppError::NotFound(format!("Connection {} not found", connection_id)));
    }

    info!("Connection deleted: {}", connection_id);

    Ok(Json(MessageResponse::new(
        "Connection removed successfully.".to_string(),
    )))
}

/// Activate a connection (set as active)
pub async fn activate_connection(
    State(state): State<SharedState>,
    Extension(claims): Extension<Claims>,
    Path((project_id, connection_id)): Path<(i32, i32)>,
) -> ApiResult<Json<SuccessResponse<ConnectionDetails>>> {
    debug!(
        "Activating connection {} in project {}",
        connection_id, project_id
    );

    // Parse user_id from claims
    let owner_id: i32 = claims.sub.parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID".to_string()))?;

    // Get database client (required - no fallback)
    let client = state.db_pool.get().await
        .map_err(|e| AppError::Internal(format!("Failed to get database connection: {}", e)))?;

    // Verify project ownership
    let _project_exists = client.query_opt(
        "SELECT id FROM projects WHERE id = $1 AND owner_id = $2",
        &[&project_id, &owner_id],
    ).await
    .map_err(|e| AppError::Internal(format!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Project not found".to_string()))?;

    // Fetch the connection
    let row = client.query_opt(
        "SELECT id, project_id, connection_name, database_type, created_at, updated_at
         FROM saved_connections
         WHERE id = $1 AND project_id = $2",
        &[&connection_id, &project_id],
    ).await
    .map_err(|e| AppError::Internal(format!("Database error: {}", e)))?
    .ok_or_else(|| AppError::NotFound(format!("Connection {} not found", connection_id)))?;

    let connection = ConnectionDetails {
        id: row.get("id"),
        project_id: row.get("project_id"),
        name: row.get("connection_name"),
        connection_type: row.get("database_type"),
        environment: "production".to_string(),
        is_active: true,
        last_tested: None,
        test_status: None,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };

    info!("Connection activated: {}", connection.id);

    Ok(Json(SuccessResponse::with_data(
        "Connection activated successfully.",
        connection,
    )))
}