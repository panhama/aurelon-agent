namespace Aurelon.Models;

public sealed record SearchHit(
    string ChunkId,
    double Score,
    string SearchText,
    string DisplayText,
    string? ParentChunkId,
    int PageFrom,
    int PageTo,
    string SectionPath,
    string FileName);

public static class RankFusion
{
    public static IReadOnlyList<SearchHit> Rrf(params IEnumerable<SearchHit>[] rankings)
    {
        const double k = 60.0;
        var map = new Dictionary<string, (SearchHit Hit, double Score)>();

        foreach (var ranking in rankings)
        {
            var rank = 0;
            foreach (var hit in ranking)
            {
                if (!map.TryGetValue(hit.ChunkId, out var existing))
                    existing = (hit, 0);

                existing.Score += 1.0 / (k + rank + 1);
                map[hit.ChunkId] = existing;
                rank++;
            }
        }

        return map.Values
            .OrderByDescending(x => x.Score)
            .Select(x => x.Hit with { Score = x.Score })
            .ToList();
    }
}
