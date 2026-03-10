using Dapper;
using Aurelon.Db;
using Aurelon.Models;

namespace Aurelon.Repositories;

public interface IDocumentLibraryRepository
{
    Task<IReadOnlyList<DocumentLibraryItemDto>> GetDocumentsAsync(string userId, CancellationToken cancellationToken = default);
    Task<DocumentSourceRecord?> GetSourceAsync(string userId, string storageKey, CancellationToken cancellationToken = default);
    Task<bool> DeleteDocumentAsync(Guid uploadId, string userId, CancellationToken cancellationToken = default);
    Task<bool> DocumentExistsAsync(string userId, string storageKey, CancellationToken cancellationToken = default);
}

public sealed class DocumentSourceRecord
{
    public Guid UploadId { get; init; }
    public string StorageKey { get; init; } = string.Empty;
    public string FileName { get; init; } = string.Empty;
    public string ContentType { get; init; } = "application/pdf";
}

public sealed class DocumentLibraryRepository(IAppDbConnectionFactory connectionFactory) : IDocumentLibraryRepository
{
    private const string ListSql = """
        select u.id as UploadId,
               dj.id as JobId,
               u.original_file_name as FileName,
               u.storage_key as StorageKey,
               u.content_type as ContentType,
               u.file_size as FileSize,
               coalesce(dj.status, u.status) as Status,
               u.created_at_utc as UploadedAtUtc,
               dj.last_error as LastError
        from uploads u
        left join document_jobs dj on dj.upload_id = u.id
        where u.user_id = @UserId and u.kind = 'Document'
        order by u.created_at_utc desc;
        """;

    public async Task<IReadOnlyList<DocumentLibraryItemDto>> GetDocumentsAsync(string userId, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<DocumentRow>(new CommandDefinition(ListSql, new { UserId = userId }, cancellationToken: cancellationToken));
        return rows.Select(row => new DocumentLibraryItemDto(row.UploadId, row.JobId, row.FileName, row.StorageKey, row.ContentType, row.FileSize, Enum.Parse<JobStatus>(row.Status, true), new DateTimeOffset(row.UploadedAtUtc, TimeSpan.Zero), row.LastError)).ToList();
    }

    public async Task<DocumentSourceRecord?> GetSourceAsync(string userId, string storageKey, CancellationToken cancellationToken = default)
    {
        const string sql = """
            select id as UploadId,
                   storage_key as StorageKey,
                   original_file_name as FileName,
                   content_type as ContentType
            from uploads
            where user_id = @UserId and kind = 'Document' and storage_key = @StorageKey
            limit 1;
            """;
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<DocumentSourceRecord>(new CommandDefinition(sql, new { UserId = userId, StorageKey = storageKey }, cancellationToken: cancellationToken));
    }

    public async Task<bool> DeleteDocumentAsync(Guid uploadId, string userId, CancellationToken cancellationToken = default)
    {
        const string sql = "delete from uploads where id = @UploadId and user_id = @UserId and kind = 'Document';";
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        var changed = await connection.ExecuteAsync(new CommandDefinition(sql, new { UploadId = uploadId, UserId = userId }, cancellationToken: cancellationToken));
        return changed > 0;
    }

    public async Task<bool> DocumentExistsAsync(string userId, string storageKey, CancellationToken cancellationToken = default)
    {
        const string sql = "select exists(select 1 from uploads where user_id = @UserId and kind = 'Document' and storage_key = @StorageKey);";
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteScalarAsync<bool>(new CommandDefinition(sql, new { UserId = userId, StorageKey = storageKey }, cancellationToken: cancellationToken));
    }

    private sealed class DocumentRow
    {
        public Guid UploadId { get; init; }
        public Guid JobId { get; init; }
        public string FileName { get; init; } = string.Empty;
        public string StorageKey { get; init; } = string.Empty;
        public string ContentType { get; init; } = "application/pdf";
        public long FileSize { get; init; }
        public string Status { get; init; } = JobStatus.Pending.ToString();
        public DateTime UploadedAtUtc { get; init; }
        public string? LastError { get; init; }
    }
}
