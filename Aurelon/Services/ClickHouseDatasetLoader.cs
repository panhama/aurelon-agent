using System.Text;
using System.Text.Json;
using Aurelon.Models;
using Microsoft.Extensions.Options;

namespace Aurelon.Services;

public sealed class ClickHouseDatasetLoader(IHttpClientFactory httpClientFactory, IOptions<ClickHouseOptions> options)
{
    private readonly ClickHouseOptions _options = options.Value;

    public async Task<string> LoadAsync(Guid datasetVersionId, ParsedDataset dataset, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_options.Endpoint)) return $"table_{datasetVersionId:n}"; // Mock if not configured
        
        var tableName = $"dataset_{datasetVersionId:n}";
        var client = httpClientFactory.CreateClient();
        
        var endpoint = _options.Endpoint.TrimEnd('/');
        if (!endpoint.StartsWith("http")) endpoint = $"http://{endpoint}";
        
        client.BaseAddress = new Uri(endpoint);

        if (!string.IsNullOrEmpty(_options.Username))
        {
            var authHeader = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password}"));
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", authHeader);
        }
        
        var columnsSql = string.Join(", ", dataset.Columns.Select(c => $"\"{c.StorageName}\" Nullable({c.ClickHouseType})"));
        var createTableSql = $"CREATE TABLE IF NOT EXISTS {_options.Database}.\"{tableName}\" ({columnsSql}) ENGINE = MergeTree ORDER BY tuple()";
        
        var createResponse = await client.PostAsync($"/?database={Uri.EscapeDataString(_options.Database)}", new StringContent(createTableSql, Encoding.UTF8, "text/plain"), ct);
        createResponse.EnsureSuccessStatusCode();

        var sb = new StringBuilder();
        foreach (var row in dataset.Rows)
        {
            sb.AppendLine(JsonSerializer.Serialize(row));
        }
        
        var insertSql = $"INSERT INTO {_options.Database}.\"{tableName}\" FORMAT JSONEachRow\n{sb}";
        var insertResponse = await client.PostAsync($"/?database={Uri.EscapeDataString(_options.Database)}", new StringContent(insertSql, Encoding.UTF8, "text/plain"), ct);
        insertResponse.EnsureSuccessStatusCode();

        return tableName;
    }
}
