
namespace Aurelon.Models;

public sealed record QueuedUploadResponse(
    Guid UploadId,
    Guid JobId,
    UploadKind Kind,
    JobStatus Status,
    string StorageKey,
    Guid? DatasetVersionId = null);
