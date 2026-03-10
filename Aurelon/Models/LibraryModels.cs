namespace Aurelon.Models;

public record DocumentLibraryItemDto(
    Guid UploadId,
    Guid JobId,
    string FileName,
    string StorageKey,
    string ContentType,
    long FileSize,
    JobStatus Status,
    DateTimeOffset UploadedAtUtc,
    string? LastError
);
