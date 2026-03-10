using Aurelon.Models;

namespace Aurelon.Services;

public sealed class ClickHouseDatasetLoader
{
    public async Task<string> LoadAsync(Guid datasetVersionId, ParsedDataset dataset, CancellationToken ct)
    {
        // Simplified implementation as ClickHouse logic is not the primary focus of RAG fix
        return $"table_{datasetVersionId:n}";
    }
}
