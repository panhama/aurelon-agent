using Microsoft.Extensions.VectorData;

namespace Aurelon.Workers.Documents.Models;

public sealed record DocumentChunkVectorRecord
{
    private const int VectorDimensions = 1536;

    [VectorStoreKey]
    public required Guid Key { get; init; }

    [VectorStoreData(IsIndexed = true)]
    public required string UserId { get; init; }

    [VectorStoreData(IsIndexed = true)]
    public required string UploadId { get; init; }

    [VectorStoreData]
    public required string StorageKey { get; init; }

    [VectorStoreData]
    public required string Title { get; init; }

    [VectorStoreData]
    public int PageNumber { get; init; }

    [VectorStoreData]
    public required string Text { get; init; }

    [VectorStoreVector(VectorDimensions, DistanceFunction = DistanceFunction.CosineSimilarity)]
    public ReadOnlyMemory<float>? Vector { get; set; }
}
