using Aurelon.Contracts.Jobs;
using Aurelon.Infrastructure.Db;
using Aurelon.Workers.Documents.Models;
using Dapper;

namespace Aurelon.Workers.Documents.Repositories;

public sealed class DocumentWorkRepository(IAppDbConnectionFactory connectionFactory)
{
    private const string LeaseSql = """
        with next_job as (
            select dj.id
            from document_jobs dj
            where dj.status in ('Pending', 'RetryableFailed')
            order by dj.created_at_utc
            for update skip locked
            limit 1
        )
        update document_jobs dj
        set status = 'Processing',
            started_at_utc = coalesce(dj.started_at_utc, now() at time zone 'utc'),
            heartbeat_at_utc = now() at time zone 'utc',
            leased_until_utc = now() at time zone 'utc' + interval '15 minutes',
            updated_at_utc = now() at time zone 'utc'
        from next_job nj, uploads u
        where dj.id = nj.id and u.id = dj.upload_id
        returning dj.id as JobId,
                  dj.upload_id as UploadId,
                  dj.user_id as UserId,
                  u.storage_key as StorageKey,
                  u.display_name as DisplayName,
                  u.original_file_name as OriginalFileName,
                  u.content_type as ContentType;
        """;

    public async Task<DocumentLease?> TryLeaseNextAsync(CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var lease = await connection.QuerySingleOrDefaultAsync<DocumentLease>(
            new CommandDefinition(LeaseSql, transaction: transaction, cancellationToken: cancellationToken));
        if (lease is null)
        {
            await transaction.CommitAsync(cancellationToken);
            return null;
        }

        await transaction.CommitAsync(cancellationToken);
        return lease;
    }

    public async Task ReplaceChunksAsync(DocumentLease lease, IReadOnlyList<DocumentChunkRecord> chunks, CancellationToken cancellationToken)
    {
        const string deleteSql = "delete from document_chunks where upload_id = @UploadId;";
        const string insertSql = """
            insert into document_chunks (
                id, upload_id, job_id, user_id, chunk_index, page_number, title, storage_key, excerpt, content, created_at_utc)
            values (
                @Id, @UploadId, @JobId, @UserId, @ChunkIndex, @PageNumber, @Title, @StorageKey, @Excerpt, @Content, @CreatedAtUtc);
            """;
        const string completeJobSql = """
            update document_jobs
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
        await connection.ExecuteAsync(new CommandDefinition(deleteSql, new { lease.UploadId }, transaction, cancellationToken: cancellationToken));
        if (chunks.Count > 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(insertSql, chunks, transaction, cancellationToken: cancellationToken));
        }
        await connection.ExecuteAsync(new CommandDefinition(completeJobSql, new { lease.JobId }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(completeUploadSql, new { lease.UploadId }, transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(Guid jobId, Guid uploadId, string errorMessage, CancellationToken cancellationToken)
    {
        const string failJobSql = """
            update document_jobs
            set status = 'RetryableFailed', last_error = @ErrorMessage, updated_at_utc = now() at time zone 'utc', leased_until_utc = null
            where id = @JobId;
            """;
        const string failUploadSql = """
            update uploads
            set status = 'Failed', updated_at_utc = now() at time zone 'utc'
            where id = @UploadId;
            """;

        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(failJobSql, new { JobId = jobId, ErrorMessage = errorMessage }, transaction, cancellationToken: cancellationToken));
        await connection.ExecuteAsync(new CommandDefinition(failUploadSql, new { UploadId = uploadId }, transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }
}
