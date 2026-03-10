using Aurelon.Jobs;
using Dapper;
using Npgsql;

namespace Aurelon.Infrastructure.Repositories.Documents;

public interface IDocumentJobRepository
{
    Task InsertAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DocumentJob job, CancellationToken cancellationToken = default);
}

public sealed class DocumentJobRepository : IDocumentJobRepository
{
    private const string InsertSql = """
        insert into document_jobs (
            id, upload_id, user_id, status, attempt_count, max_attempts, created_at_utc, updated_at_utc)
        values (
            @Id, @UploadId, @UserId, @Status, @AttemptCount, @MaxAttempts, @CreatedAtUtc, @UpdatedAtUtc);
        """;

    public Task InsertAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DocumentJob job, CancellationToken cancellationToken = default)
    {
        var parameters = new
        {
            job.Id,
            job.UploadId,
            job.UserId,
            Status = job.Status.ToString(),
            job.AttemptCount,
            job.MaxAttempts,
            CreatedAtUtc = job.CreatedAtUtc.UtcDateTime,
            UpdatedAtUtc = job.UpdatedAtUtc.UtcDateTime,
        };

        return connection.ExecuteAsync(new CommandDefinition(InsertSql, parameters, transaction, cancellationToken: cancellationToken));
    }
}
