using Dapper;
using Npgsql;
using Aurelon.Db;
using Aurelon.Models;

namespace Aurelon.Repositories;

public interface IJobReadRepository
{
    Task<JobStatusResponse?> GetByIdAsync(Guid jobId, CancellationToken cancellationToken = default);
}

public sealed class JobReadRepository(IAppDbConnectionFactory connectionFactory) : IJobReadRepository
{
    private const string UploadJobQuerySql = """
        select id, status, 'document' as phase, upload_id, null::uuid as dataset_version_id, attempt_count, created_at_utc, started_at_utc, completed_at_utc, last_error, 'Document' as upload_kind
        from document_jobs where id = @JobId
        union all
        select id, status, 'dataset' as phase, upload_id, dataset_version_id, attempt_count, created_at_utc, started_at_utc, completed_at_utc, last_error, 'Dataset' as upload_kind
        from dataset_jobs where id = @JobId
        limit 1;
        """;

    private const string TrainingJobQuerySql = """
        select id, status, 'training' as phase, null::uuid as upload_id, dataset_version_id, attempt_count, created_at_utc, started_at_utc, completed_at_utc, last_error, null::text as upload_kind
        from training_jobs where id = @JobId
        limit 1;
        """;

    public async Task<JobStatusResponse?> GetByIdAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<JobStatusRow>(
            new CommandDefinition(UploadJobQuerySql, new { JobId = jobId }, cancellationToken: cancellationToken));
        row ??= await connection.QuerySingleOrDefaultAsync<JobStatusRow>(
            new CommandDefinition(TrainingJobQuerySql, new { JobId = jobId }, cancellationToken: cancellationToken));
        if (row is null)
        {
            return null;
        }

        var status = Enum.Parse<JobStatus>(row.Status, ignoreCase: true);
        var uploadKind = string.IsNullOrWhiteSpace(row.UploadKind)
            ? (UploadKind?)null
            : Enum.Parse<UploadKind>(row.UploadKind, ignoreCase: true);

        return new JobStatusResponse(
            row.Id,
            status,
            row.Phase,
            row.UploadId,
            uploadKind,
            row.DatasetVersionId,
            row.AttemptCount,
            new DateTimeOffset(row.CreatedAtUtc, TimeSpan.Zero),
            row.StartedAtUtc is null ? null : new DateTimeOffset(row.StartedAtUtc.Value, TimeSpan.Zero),
            row.CompletedAtUtc is null ? null : new DateTimeOffset(row.CompletedAtUtc.Value, TimeSpan.Zero),
            row.LastError);
    }
}

file sealed class JobStatusRow
{
    public Guid Id { get; init; }
    public string Status { get; init; } = string.Empty;
    public string Phase { get; init; } = string.Empty;
    public Guid? UploadId { get; init; }
    public Guid? DatasetVersionId { get; init; }
    public int AttemptCount { get; init; }
    public DateTime CreatedAtUtc { get; init; }
    public DateTime? StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
    public string? LastError { get; init; }
    public string? UploadKind { get; init; }
}
