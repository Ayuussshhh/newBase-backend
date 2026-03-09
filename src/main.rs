//! SchemaFlow API - Database Governance Platform
//!
//! A "GitHub PR for Databases" - propose, review, and safely execute schema changes.
//! 
//! NEW ARCHITECTURE: The server now supports dynamic database connections.
//! You no longer need to configure a database in .env - users can connect
//! to any database by providing a connection string via the API.
//!
//! GOVERNANCE PIPELINE: The server includes a full governance workflow:
//! - Stage 1 (Mirror): Schema introspection with semantic mapping
//! - Stage 2 (Proposal): Schema change proposals with review workflow
//! - Stage 3 (Simulate): Risk analysis, dry-run validation, impact assessment
//! - Stage 4 (Execute): Safe execution with rollback capability

mod auth;
mod config;
mod connection;
mod db;
mod error;
mod introspection;
mod models;
mod pipeline;
mod proposal;
mod routes;
mod simulation;
mod snapshot;
mod state;
mod users;

use crate::config::Settings;
use crate::routes::create_router;
use crate::state::AppState;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn, error};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for structured logging
    init_tracing();

    info!("🚀 Starting SchemaFlow - Database Governance Platform...");

    // Load configuration
    let settings = Settings::load()?;
    info!("📋 Configuration loaded successfully");
    
    // Get JWT secret from environment or generate a default (for dev only)
    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| {
            warn!("⚠️  JWT_SECRET not set, using default (INSECURE - set in production!)");
            "schemaflow-dev-secret-change-in-production".to_string()
        });

    // Initialize database pool - REQUIRED (no fallback to in-memory)
    let state = match init_database_pool().await {
        Ok(pool) => {
            info!("✅ Database pool created successfully");
            
            // Create tables if they don't exist
            if let Err(e) = create_database_tables(&pool).await {
                warn!("⚠️  Warning creating tables: {}", e);
            }
            
            Arc::new(AppState::new(pool, jwt_secret))
        }
        Err(e) => {
            error!("❌ FATAL: Failed to initialize database pool: {}", e);
            error!("DATABASE_URL must be set in .env and database must be accessible");
            panic!("Cannot start server without database connection");
        }
    };

    // Build the router
    let app = create_router(state, &settings);

    // Create socket address
    let addr = SocketAddr::from((settings.server.host, settings.server.port));
    
    info!("🌐 Server listening on http://{}", addr);
    info!("");
    info!("📚 API Endpoints:");
    info!("   ─── Authentication ───");
    info!("   POST /api/auth/login           - Login with email/password");
    info!("   POST /api/auth/register        - Register new account");
    info!("   POST /api/auth/refresh         - Refresh access token");
    info!("   GET  /api/auth/me              - Get current user");
    info!("");
    info!("   ─── Connection Management ───");
    info!("   POST /api/connections          - Connect to a database");
    info!("   GET  /api/connections          - List all connections");
    info!("   POST /api/connections/test     - Test a connection");
    info!("   GET  /api/schema               - Get schema for active connection");
    info!("");
    info!("   ─── Governance Pipeline ───");
    info!("   POST /api/proposals            - Create new proposal");
    info!("   GET  /api/proposals            - List all proposals");
    info!("   POST /api/proposals/:id/submit - Submit for review");
    info!("   POST /api/proposals/:id/approve - Approve (Admin only)");
    info!("   POST /api/proposals/:id/analyze - Risk analysis");
    info!("   POST /api/proposals/:id/execute - Execute migration");
    info!("");
    info!("   ─── Impact Analysis (Core Feature) ───");
    info!("   POST /api/connections/:id/snapshots    - Create schema snapshot");
    info!("   GET  /api/connections/:id/snapshots    - List all snapshots");
    info!("   GET  /api/connections/:id/snapshots/diff - Compare snapshots");
    info!("   POST /api/connections/:id/blast-radius - Analyze impact of changes");
    info!("   GET  /api/connections/:id/schema-drift - Check drift from baseline");
    info!("   GET  /api/rules                        - List governance rules");
    info!("");

    // Create TCP listener and serve
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("👋 Server shutdown complete");
    Ok(())
}

/// Initialize tracing with structured logging
fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,interactive_db_api=debug,tower_http=debug"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .compact(),
        )
        .init();
}

/// Initialize database pool from DATABASE_URL
async fn init_database_pool() -> anyhow::Result<deadpool_postgres::Pool> {
    // Load .env file first
    let _ = dotenvy::dotenv();
    
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL not set in environment or .env file"))?;

    // Parse the DATABASE_URL using tokio_postgres::Config
    let config = database_url.parse::<tokio_postgres::Config>()
        .map_err(|e| anyhow::anyhow!("Failed to parse DATABASE_URL: {}", e))?;

    // Extract connection parameters from parsed config
    let hosts = config.get_hosts();
    let host_str = if !hosts.is_empty() {
        match &hosts[0] {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
        }
    } else {
        return Err(anyhow::anyhow!("No host in DATABASE_URL"));
    };
    
    let ports = config.get_ports();
    let port = if !ports.is_empty() { ports[0] } else { 5432 };
    
    let user = config.get_user()
        .map(|u| u.to_string())
        .ok_or_else(|| anyhow::anyhow!("No user in DATABASE_URL"))?;
    
    let password = config.get_password()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .unwrap_or_default();
    
    let database = config.get_dbname()
        .map(|db| db.to_string())
        .ok_or_else(|| anyhow::anyhow!("No database name in DATABASE_URL"))?;

    // Determine if TLS is needed (Neon requires it)
    let use_tls = host_str.contains("neon.tech") || database_url.contains("sslmode=require");

    // Create deadpool config
    use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};
    
    let mut cfg = Config::new();
    cfg.host = Some(host_str.clone());
    cfg.port = Some(port);
    cfg.user = Some(user);
    cfg.password = Some(password);
    cfg.dbname = Some(database);
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    // Create pool with TLS support if needed
    let pool = if use_tls {
        // Create TLS connector for Neon
        let certs = rustls_native_certs::load_native_certs();
        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs.certs {
            root_store.add(cert).ok();
        }

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        
        cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), tls)
            .map_err(|e| anyhow::anyhow!("Failed to create TLS pool: {}", e))?
    } else {
        cfg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
            .map_err(|e| anyhow::anyhow!("Failed to create pool: {}", e))?
    };

    // Test the connection
    let client = pool.get().await
        .map_err(|e| anyhow::anyhow!("Failed to get pool connection: {}", e))?;
    
    // Simple test query to verify connection works
    let _row = client.query_one("SELECT 1 as ok", &[])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to verify database connection: {}", e))?;

    info!("✅ Database connection successful (TLS: {})", use_tls);
    Ok(pool)
}

/// Create database tables if they don't exist
async fn create_database_tables(pool: &deadpool_postgres::Pool) -> anyhow::Result<()> {
    let client = pool.get().await?;

    // Create roles table (for managing user roles)
    client.execute(
        "CREATE TABLE IF NOT EXISTS roles (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL,
            description TEXT,
            permissions TEXT[],
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        &[],
    ).await?;

    // Create users table
    client.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            avatar_url TEXT,
            role_id INTEGER DEFAULT 1 REFERENCES roles(id),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        &[],
    ).await?;

    // Create projects table
    client.execute(
        "CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            icon VARCHAR(50),
            color VARCHAR(7),
            is_private BOOLEAN DEFAULT true,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
        )",
        &[],
    ).await?;

    // Create project_members table
    client.execute(
        "CREATE TABLE IF NOT EXISTS project_members (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'editor',
            joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            UNIQUE(project_id, user_id)
        )",
        &[],
    ).await?;

    // Create saved_connections table
    client.execute(
        "CREATE TABLE IF NOT EXISTS saved_connections (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            connection_string VARCHAR(1024) NOT NULL,
            encrypted_password TEXT,
            database_type VARCHAR(50),
            connection_name VARCHAR(255),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
        )",
        &[],
    ).await?;

    // Insert default roles if they don't exist
    let _ = client.execute(
        "INSERT INTO roles (name, description, permissions) VALUES 
         ('admin', 'Administrator with full access', '{}'),
         ('editor', 'Can edit and manage content', '{}'),
         ('viewer', 'Read-only access', '{}')
         ON CONFLICT (name) DO NOTHING",
        &[],
    ).await;

    // Create indexes for performance
    let _ = client.execute(
        "CREATE INDEX IF NOT EXISTS idx_projects_owner_id ON projects(owner_id)",
        &[],
    ).await;
    let _ = client.execute(
        "CREATE INDEX IF NOT EXISTS idx_project_members_project_id ON project_members(project_id)",
        &[],
    ).await;
    let _ = client.execute(
        "CREATE INDEX IF NOT EXISTS idx_saved_connections_project_id ON saved_connections(project_id)",
        &[],
    ).await;

    info!("✅ Database tables initialized");
    Ok(())
}

/// Graceful shutdown signal handler
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("📴 Received Ctrl+C signal, initiating graceful shutdown...");
        },
        _ = terminate => {
            info!("📴 Received terminate signal, initiating graceful shutdown...");
        },
    }
}
