using Aurelon.Models;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using Microsoft.Extensions.Logging;

namespace Aurelon.Services;

public sealed class DocumentIndexingService(
    LayoutAwareChunker chunker,
    VoyageEmbeddingClient embeddingClient,
    QdrantVectorStore vectorStore,
    ITokenEstimator tokenEstimator,
    ILogger<DocumentIndexingService> logger)
{
    public async Task IndexAsync(DocumentLease lease, StructuredDocument doc, CancellationToken ct)
    {
        var collection = vectorStore.GetCollection<string, DocumentChunkVectorRecord>("document_chunks");
        
        await collection.EnsureCollectionExistsAsync(ct);
        
        var chunks = chunker.CreateSmallChunks(doc);
        logger.LogInformation("Generated {ChunkCount} chunks for document {DocumentId}", chunks.Count, doc.DocumentId);

        // Adaptive token-budget batching
        var batches = Batching.ByTokenBudget(
            chunks.Select(c => c.SearchText),
            text => tokenEstimator.Count(text),
            maxItems: 256,
            maxTokens: 200_000);

        var chunkIdx = 0;
        foreach (var batch in batches)
        {
            var embeddings = await embeddingClient.EmbedDocumentsAsync(batch, ct);
            
            for (int i = 0; i < batch.Count; i++)
            {
                var chunk = chunks[chunkIdx++];
                var record = new DocumentChunkVectorRecord
                {
                    Id = chunk.ChunkId,
                    UserId = lease.UserId,
                    DocumentId = chunk.DocumentId,
                    DocumentType = doc.DocumentType,
                    FileName = doc.FileName,
                    SectionPath = chunk.SectionPath,
                    PageFrom = chunk.PageFrom,
                    PageTo = chunk.PageTo,
                    ChunkKind = chunk.Kind,
                    SearchText = chunk.SearchText,
                    DisplayText = chunk.DisplayText,
                    ParentChunkId = chunk.ParentChunkId,
                    Vector = embeddings[i]
                };
                
                await collection.UpsertAsync(record, cancellationToken: ct);
            }
        }
    }
}

public static class Batching
{
    public static IEnumerable<IReadOnlyList<string>> ByTokenBudget(
        IEnumerable<string> inputs,
        Func<string, int> estimateTokens,
        int maxItems = 256,
        int maxTokens = 200_000)
    {
        var current = new List<string>();
        var used = 0;

        foreach (var input in inputs)
        {
            var cost = estimateTokens(input);
            if (current.Count > 0 && (current.Count >= maxItems || used + cost > maxTokens))
            {
                yield return current;
                current = new List<string>();
                used = 0;
            }
            current.Add(input);
            used += cost;
        }

        if (current.Count > 0)
            yield return current;
    }
}
