using Aurelon.Models;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using Microsoft.Extensions.VectorData;

namespace Aurelon.Services;

public sealed class RetrievalService(
    QdrantVectorStore vectorStore,
    VoyageEmbeddingClient embeddingClient)
{
    public async Task<IReadOnlyList<SearchHit>> SearchAsync(string query, string userId, int topK = 10, CancellationToken ct = default)
    {
        var collection = vectorStore.GetCollection<string, DocumentChunkVectorRecord>("document_chunks");
        
        var queryEmbedding = await embeddingClient.EmbedQueryAsync(query, ct);
        
        // Dense search
        var searchOptions = new VectorSearchOptions<DocumentChunkVectorRecord>
        {
            Filter = r => r.UserId == userId
        };
        
        var searchResults = collection.SearchAsync(queryEmbedding, 40, searchOptions, ct);
        var denseHits = new List<SearchHit>();
        
        await foreach (var result in searchResults.WithCancellation(ct))
        {
            var record = result.Record;
            denseHits.Add(new SearchHit(
                record.Id,
                result.Score ?? 0,
                record.SearchText,
                record.DisplayText,
                record.ParentChunkId,
                record.PageFrom,
                record.PageTo,
                record.SectionPath,
                record.FileName
            ));
        }

        // Rerank
        if (denseHits.Count > 0)
        {
            var documentTexts = denseHits.Select(h => h.SearchText).ToList();
            var rerankScores = await embeddingClient.RerankAsync(query, documentTexts, ct);
            
            var rerankedHits = new List<SearchHit>();
            for (int i = 0; i < denseHits.Count; i++)
            {
                rerankedHits.Add(denseHits[i] with { Score = rerankScores[i] });
            }
            
            return rerankedHits.OrderByDescending(h => h.Score).Take(topK).ToList();
        }

        return denseHits;
    }
}
