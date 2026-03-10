namespace Aurelon.Models;

public sealed record DocumentChunkRecord(
    Guid Id,
    Guid UploadId,
    Guid JobId,
    string UserId,
    int ChunkIndex,
    int? PageNumber,
    string Title,
    string StorageKey,
    string Excerpt,
    string Content,
    DateTimeOffset CreatedAtUtc);

public sealed record DocumentLease(
    Guid JobId,
    Guid UploadId,
    string UserId,
    string StorageKey,
    string DisplayName,
    string OriginalFileName,
    string ContentType);

public sealed record ExtractedDocumentChunk(int ChunkIndex, int? PageNumber, string Text);
