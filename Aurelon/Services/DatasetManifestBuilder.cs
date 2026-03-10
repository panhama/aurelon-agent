using Aurelon.Models;
using System.Text.Json;

namespace Aurelon.Services;

public sealed class DatasetManifestBuilder
{
    public (string ManifestJson, string SummaryJson) Build(
        Guid datasetId,
        Guid datasetVersionId,
        string fileName,
        string tableName,
        ParsedDataset dataset)
    {
        var manifest = new
        {
            DatasetId = datasetId,
            VersionId = datasetVersionId,
            FileName = fileName,
            TableName = tableName,
            Columns = dataset.Columns
        };

        var summary = new
        {
            RowCount = dataset.RowCount,
            ColumnCount = dataset.Columns.Count
        };

        return (JsonSerializer.Serialize(manifest), JsonSerializer.Serialize(summary));
    }
}
