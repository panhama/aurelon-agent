using Aurelon.Uploads;
using Aurelon.Jobs;
using Aurelon.Uploads;

namespace Aurelon.Uploads;

public sealed record QueuedUploadResponse(
    Guid UploadId,
    Guid JobId,
    UploadKind Kind,
    JobStatus Status,
    string StorageKey,
    Guid? DatasetVersionId = null);
