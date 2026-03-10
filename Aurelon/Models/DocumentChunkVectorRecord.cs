using Microsoft.Extensions.VectorData;

namespace Aurelon.Models;

public sealed record DocumentChunkVectorRecord
{
    private const int VectorDimensions = 1024;

    [VectorStoreKey]
    public required string Id { get; init; }

    [VectorStoreData(IsIndexed = true)]
    public required string UserId { get; init; }

    [VectorStoreData(IsIndexed = true)]
    public required string DocumentId { get; init; }

    [VectorStoreData(IsIndexed = true)]
    public required string DocumentType { get; init; }

    [VectorStoreData]
    public required string FileName { get; init; }

    [VectorStoreData]
    public required string SectionPath { get; init; }

    [VectorStoreData]
    public int PageFrom { get; init; }

    [VectorStoreData]
    public int PageTo { get; init; }

    [VectorStoreData]
    public required string ChunkKind { get; init; }

    [VectorStoreData]
    public required string SearchText { get; init; }

    [VectorStoreData]
    public required string DisplayText { get; init; }

    [VectorStoreData]
    public string? ParentChunkId { get; init; }

    [VectorStoreVector(VectorDimensions, DistanceFunction = DistanceFunction.CosineSimilarity)]
    public ReadOnlyMemory<float>? Vector { get; set; }
}
