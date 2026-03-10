using Aurelon.Db;
using Aurelon.Models;
using Dapper;

namespace Aurelon.Repositories;

public sealed class DatasetWorkRepository(IAppDbConnectionFactory connectionFactory)
{
    private const string LeaseSql = """
        with next_job as (
            select dj.id
            from dataset_jobs dj
            where dj.status in ('Pending', 'RetryableFailed')
               or (dj.status = 'Processing' and (dj.leased_until_utc is null or dj.leased_until_utc < now() at time zone 'utc'))
            order by dj.created_at_utc
            for update skip locked
            limit 1
        )
        update dataset_jobs dj
        set status = 'Processing',
            started_at_utc = coalesce(dj.started_at_utc, now() at time zone 'utc'),
            heartbeat_at_utc = now() at time zone 'utc',
            leased_until_utc = now() at time zone 'utc' + interval '15 minutes',
            updated_at_utc = now() at time zone 'utc'
        from next_job nj, uploads u
        where dj.id = nj.id and u.id = dj.upload_id
        returning dj.id as JobId,
                  dj.upload_id as UploadId,
                  dj.dataset_id as DatasetId,
                  dj.dataset_version_id as DatasetVersionId,
                  dj.user_id as UserId,
                  u.storage_key as StorageKey,
                  u.original_file_name as OriginalFileName;
        """;

    public async Task<DatasetLease?> TryLeaseNextAsync(CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var lease = await connection.QuerySingleOrDefaultAsync<DatasetLease>(new CommandDefinition(LeaseSql, transaction: transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return lease;
    }

    public async Task MarkCompletedAsync(DatasetLease lease, string tableName, int rowCount, string manifestJson, string summaryJson, CancellationToken cancellationToken)
    {
        const string completeVersionSql = """
            update dataset_versions
            set row_count = @RowCount,
                clickhouse_table_name = @TableName,
                manifest_json = cast(@ManifestJson as jsonb),
                summary_json = cast(@SummaryJson as jsonb),
                status = 'Completed'
            where id = @DatasetVersionId;
            """;
        const string completeJobSql = """
            update dataset_jobs
            set status = 'Completed', completed_at_utc = now() at time zone 'utc', updated_at_utc = now() at time zone 'utc', leased_until_utc = null
            where id = @JobId;
            """;
        const string completeUploadSql = """
            update uploads
            set status = 'Completed', updated_at_utc = now() at time zone 'utc'
            where id = @UploadId;
            """;

        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(completeVersionSql, new { lease.DatasetVersionId, RowCount = rowCount, TableName = tableName, ManifestJson = manifestJson, SummaryJson = summaryJson }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(completeJobSql, new { lease.JobId }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(completeUploadSql, new { lease.UploadId }, transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(DatasetLease lease, string errorMessage, CancellationToken cancellationToken)
    {
        const string failJobSql = """
            update dataset_jobs
            set status = 'RetryableFailed', last_error = @ErrorMessage, updated_at_utc = now() at time zone 'utc', leased_until_utc = null
            where id = @JobId;
            """;
        const string failUploadSql = """
            update uploads
            set status = 'Failed', updated_at_utc = now() at time zone 'utc'
            where id = @UploadId;
            """;
        const string failVersionSql = """
            update dataset_versions
            set status = 'RetryableFailed'
            where id = @DatasetVersionId;
            """;

        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(failJobSql, new { lease.JobId, ErrorMessage = errorMessage }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(failUploadSql, new { lease.UploadId }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(failVersionSql, new { lease.DatasetVersionId }, transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }
}
