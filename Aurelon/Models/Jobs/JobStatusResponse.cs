
namespace Aurelon.Models;

public sealed record JobStatusResponse(
    Guid JobId,
    JobStatus Status,
    string Phase,
    Guid? UploadId,
    UploadKind? UploadKind,
    Guid? DatasetVersionId,
    int AttemptCount,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? StartedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    string? LastError);
