using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Aurelon.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Aurelon.Services;

public sealed class ClickHouseDatasetLoader(
    IHttpClientFactory httpClientFactory, 
    IOptions<ClickHouseOptions> options,
    ILogger<ClickHouseDatasetLoader> logger)
{
    private readonly ClickHouseOptions _options = options.Value;
    private static readonly Regex IdentifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    public async Task<string> LoadAsync(Guid datasetVersionId, ParsedDataset dataset, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_options.Endpoint) || string.IsNullOrWhiteSpace(_options.Database))
        {
            logger.LogWarning("ClickHouse is not configured. Mocking table name for dataset version {DatasetVersionId}.", datasetVersionId);
            return $"table_{datasetVersionId:n}";
        }

        if (!Uri.TryCreate(_options.Endpoint, UriKind.Absolute, out var uri) || 
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            throw new InvalidOperationException("Invalid ClickHouse endpoint URL. Must be an absolute HTTP/HTTPS URL.");
        }

        var tableName = $"dataset_{datasetVersionId:n}";
        var client = httpClientFactory.CreateClient();
        client.BaseAddress = uri;

        if (!string.IsNullOrEmpty(_options.Username))
        {
            var authHeader = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password}"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authHeader);
        }

        var columnsSqlList = new List<string>();
        foreach (var c in dataset.Columns)
        {
            if (!IdentifierPattern.IsMatch(c.StorageName) || !IdentifierPattern.IsMatch(c.ClickHouseType))
            {
                throw new InvalidOperationException($"Invalid characters in column name '{c.StorageName}' or type '{c.ClickHouseType}'.");
            }
            columnsSqlList.Add($"\"{c.StorageName}\" Nullable({c.ClickHouseType})");
        }

        var createTableSql = $"CREATE TABLE IF NOT EXISTS {_options.Database}.\"{tableName}\" ({string.Join(", ", columnsSqlList)}) ENGINE = MergeTree ORDER BY tuple()";

        try
        {
            logger.LogInformation("Creating ClickHouse table {TableName} for dataset version {DatasetVersionId}.", tableName, datasetVersionId);
            var createResponse = await client.PostAsync($"/?database={Uri.EscapeDataString(_options.Database)}", new StringContent(createTableSql, Encoding.UTF8, "text/plain"), ct);
            createResponse.EnsureSuccessStatusCode();

            logger.LogInformation("Inserting {RowCount} rows into ClickHouse table {TableName}.", dataset.Rows.Count, tableName);
            using var content = new JsonEachRowContent(dataset.Rows);
            var insertResponse = await client.PostAsync($"/?database={Uri.EscapeDataString(_options.Database)}", content, ct);
            insertResponse.EnsureSuccessStatusCode();

            logger.LogInformation("Successfully loaded dataset into ClickHouse table {TableName}.", tableName);
            return tableName;
        }
        catch (HttpRequestException ex)
        {
            logger.LogError(ex, "Failed to communicate with ClickHouse for dataset version {DatasetVersionId}.", datasetVersionId);
            throw new InvalidOperationException("Failed to load dataset into ClickHouse due to a network or server error.", ex);
        }
    }

    private sealed class JsonEachRowContent(IReadOnlyList<IReadOnlyDictionary<string, string?>> rows) : HttpContent
    {
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            foreach (var row in rows)
            {
                await JsonSerializer.SerializeAsync(stream, row);
                stream.WriteByte((byte)'\n');
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
