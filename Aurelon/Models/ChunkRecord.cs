namespace Aurelon.Models;

public sealed record ChunkRecord(
    string ChunkId,
    string DocumentId,
    int Order,
    int PageFrom,
    int PageTo,
    string SectionPath,
    string Kind,
    string SearchText,
    string DisplayText,
    string? ParentChunkId = null
);
