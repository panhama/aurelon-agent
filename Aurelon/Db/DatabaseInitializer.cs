using Dapper;
using Npgsql;

namespace Aurelon.Db;

public class DatabaseInitializer(IConfiguration config)
{
    private readonly string _connectionString = config.GetConnectionString("DefaultConnection")!;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        
        var sql = """
            CREATE TABLE IF NOT EXISTS uploads (
                id UUID PRIMARY KEY,
                user_id TEXT NOT NULL,
                kind INT NOT NULL,
                original_file_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                content_type TEXT NOT NULL,
                storage_key TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS document_jobs (
                id UUID PRIMARY KEY,
                upload_id UUID NOT NULL,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INT NOT NULL,
                max_attempts INT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL,
                started_at_utc TIMESTAMPTZ,
                completed_at_utc TIMESTAMPTZ,
                leased_until_utc TIMESTAMPTZ,
                heartbeat_at_utc TIMESTAMPTZ,
                last_error TEXT
            );
            
            CREATE TABLE IF NOT EXISTS document_chunks (
                id UUID PRIMARY KEY,
                upload_id UUID NOT NULL,
                job_id UUID NOT NULL,
                user_id TEXT NOT NULL,
                chunk_index INT NOT NULL,
                page_number INT NOT NULL,
                title TEXT NOT NULL,
                storage_key TEXT NOT NULL,
                excerpt TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS datasets (
                id UUID PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dataset_versions (
                id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                version_number INT NOT NULL,
                upload_id UUID NOT NULL,
                status TEXT NOT NULL,
                row_count INT,
                clickhouse_table_name TEXT,
                manifest_json JSONB,
                summary_json JSONB,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dataset_jobs (
                id UUID PRIMARY KEY,
                upload_id UUID NOT NULL,
                dataset_id UUID NOT NULL,
                dataset_version_id UUID NOT NULL,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INT NOT NULL,
                max_attempts INT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL,
                started_at_utc TIMESTAMPTZ,
                completed_at_utc TIMESTAMPTZ,
                leased_until_utc TIMESTAMPTZ,
                heartbeat_at_utc TIMESTAMPTZ,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS training_jobs (
                id UUID PRIMARY KEY,
                user_id TEXT NOT NULL,
                dataset_version_id UUID NOT NULL,
                model_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                label_column TEXT NOT NULL,
                feature_columns_json JSONB NOT NULL,
                max_training_time_seconds INT,
                plan_json JSONB NOT NULL,
                feature_snapshot_id UUID,
                lease_token UUID,
                next_retry_at_utc TIMESTAMPTZ,
                priority INT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INT NOT NULL,
                max_attempts INT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                updated_at_utc TIMESTAMPTZ NOT NULL,
                started_at_utc TIMESTAMPTZ,
                completed_at_utc TIMESTAMPTZ,
                leased_until_utc TIMESTAMPTZ,
                heartbeat_at_utc TIMESTAMPTZ,
                last_error TEXT
            );
            
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id UUID PRIMARY KEY,
                dataset_version_id UUID NOT NULL,
                source_table_name TEXT NOT NULL,
                label_column TEXT NOT NULL,
                feature_columns_json JSONB NOT NULL,
                schema_json JSONB NOT NULL,
                time_column TEXT,
                group_column TEXT
            );

            CREATE TABLE IF NOT EXISTS model_registry (
                id UUID PRIMARY KEY,
                dataset_version_id UUID NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                artifact_key TEXT NOT NULL,
                artifact_hash TEXT NOT NULL,
                manifest_json JSONB NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                published_at_utc TIMESTAMPTZ
            );
            
            CREATE TABLE IF NOT EXISTS outbox_events (
                id UUID PRIMARY KEY,
                event_type TEXT NOT NULL,
                aggregate_id UUID NOT NULL,
                job_id UUID,
                payload_json JSONB NOT NULL,
                occurred_at_utc TIMESTAMPTZ NOT NULL,
                processed_at_utc TIMESTAMPTZ
            );
            """;
            
        await conn.ExecuteAsync(new CommandDefinition(sql, cancellationToken: cancellationToken));
    }
}
