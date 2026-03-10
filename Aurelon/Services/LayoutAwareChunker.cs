using Aurelon.Models;

namespace Aurelon.Services;

public sealed class LayoutAwareChunker
{
    private readonly ITokenEstimator _tokens;
    private const int TargetTokens = 500;
    private const int MaxTokens = 600;
    private const int MinTokens = 140;
    private const int OverlapTokens = 60;

    public LayoutAwareChunker(ITokenEstimator tokens) => _tokens = tokens;

    public IReadOnlyList<ChunkRecord> CreateSmallChunks(StructuredDocument doc)
    {
        var chunks = new List<ChunkRecord>();
        var buffer = new List<ExtractedBlock>();
        var order = 0;

        foreach (var block in doc.Blocks)
        {
            var blockText = RetrievalTextNormalizer.ToEmbeddingText(block);
            var blockTokens = _tokens.Count(blockText);

            var forceBoundary =
                block.Kind == "heading" && buffer.Count > 0;

            var wouldOverflow =
                buffer.Count > 0 && CurrentTokens(buffer) + blockTokens > MaxTokens;

            var oversizedAtomicBlock =
                block.Kind == "table" && blockTokens > MaxTokens;

            if (forceBoundary || wouldOverflow)
            {
                EmitChunk();
                buffer = SeedOverlap(buffer);
            }

            if (oversizedAtomicBlock)
            {
                chunks.Add(ToChunk(doc.DocumentId, order++, new List<ExtractedBlock> { block }));
                continue;
            }

            buffer.Add(block);
        }

        if (buffer.Count > 0)
            EmitChunk();

        MergeSmallTailChunks(chunks);

        return chunks;

        void EmitChunk()
        {
            if (buffer.Count == 0) return;
            chunks.Add(ToChunk(doc.DocumentId, order++, buffer));
            buffer = new List<ExtractedBlock>();
        }
    }

    private int CurrentTokens(IEnumerable<ExtractedBlock> blocks) =>
        blocks.Sum(b => _tokens.Count(RetrievalTextNormalizer.ToEmbeddingText(b)));

    private List<ExtractedBlock> SeedOverlap(IReadOnlyList<ExtractedBlock> previous)
    {
        var overlap = new List<ExtractedBlock>();
        var tokenCount = 0;

        for (var i = previous.Count - 1; i >= 0; i--)
        {
            var b = previous[i];

            if (b.Kind is "heading" or "table")
                break;

            overlap.Insert(0, b);
            tokenCount += _tokens.Count(RetrievalTextNormalizer.ToEmbeddingText(b));

            if (tokenCount >= OverlapTokens)
                break;
        }

        return overlap;
    }

    private static ChunkRecord ToChunk(string documentId, int order, IReadOnlyList<ExtractedBlock> blocks)
    {
        var sectionPath = blocks.LastOrDefault(b => !string.IsNullOrWhiteSpace(b.SectionPath))?.SectionPath ?? "";
        var pageFrom = blocks.Min(b => b.PageNumber);
        var pageTo = blocks.Max(b => b.PageNumber);
        var kind = blocks.Any(b => b.Kind == "table") ? "table" : "text";

        var searchText = string.Join("\n\n", blocks.Select(RetrievalTextNormalizer.ToEmbeddingText));
        var displayText = string.Join("\n\n", blocks.Select(b => b.Text));

        return new ChunkRecord(
            ChunkId: $"{documentId}:{order}",
            DocumentId: documentId,
            Order: order,
            PageFrom: pageFrom,
            PageTo: pageTo,
            SectionPath: sectionPath,
            Kind: kind,
            SearchText: searchText,
            DisplayText: displayText
        );
    }

    private void MergeSmallTailChunks(List<ChunkRecord> chunks)
    {
        if (chunks.Count < 2) return;

        var last = chunks[^1];
        var approxLastTokens = _tokens.Count(last.SearchText);

        if (approxLastTokens >= MinTokens) return;

        var prev = chunks[^2];
        chunks[^2] = prev with
        {
            PageTo = Math.Max(prev.PageTo, last.PageTo),
            SearchText = prev.SearchText + "\n\n" + last.SearchText,
            DisplayText = prev.DisplayText + "\n\n" + last.DisplayText
        };
        chunks.RemoveAt(chunks.Count - 1);
    }
}
