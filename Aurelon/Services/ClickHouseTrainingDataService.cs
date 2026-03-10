using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using Aurelon.Models;
using Aurelon.Cores;
using Microsoft.Extensions.Options;

namespace Aurelon.Services;

public sealed class ClickHouseTrainingDataService(IHttpClientFactory httpClientFactory, IOptions<ClickHouseOptions> options)
{
    private static readonly Regex IdentifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);
    private readonly ClickHouseOptions _options = options.Value;

    public Task ExportSplitAsync(
        FeatureSnapshotDefinition snapshot,
        TrainingPlanDto plan,
        string trainDestinationPath,
        string testDestinationPath,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(plan);

        var orderedColumns = snapshot.Schema.Select(static column => column.Name).ToList();
        var trainQuery = BuildSplitQuery(snapshot, plan, orderedColumns, exportTrainRows: true);
        var testQuery = BuildSplitQuery(snapshot, plan, orderedColumns, exportTrainRows: false);

        return Task.WhenAll(
            ExportQueryToCsvAsync(trainQuery, trainDestinationPath, cancellationToken),
            ExportQueryToCsvAsync(testQuery, testDestinationPath, cancellationToken));
    }

    private async Task ExportQueryToCsvAsync(string query, string destinationPath, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.Endpoint) || string.IsNullOrWhiteSpace(_options.Database))
        {
            throw new InvalidOperationException("ClickHouse configuration is incomplete.");
        }

        var encodedQuery = Uri.EscapeDataString(query);
        using var request = new HttpRequestMessage(HttpMethod.Post, BuildUrl(encodedQuery));
        if (!string.IsNullOrWhiteSpace(_options.Username))
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_options.Username}:{_options.Password}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);
        }

        var client = httpClientFactory.CreateClient();
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await EnsureSuccessAsync(response, query, cancellationToken);
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var fileStream = new FileStream(destinationPath, FileMode.Create, FileAccess.Write, FileShare.None);
        await stream.CopyToAsync(fileStream, cancellationToken);
    }

    private string BuildSplitQuery(
        FeatureSnapshotDefinition snapshot,
        TrainingPlanDto plan,
        IReadOnlyList<string> orderedColumns,
        bool exportTrainRows)
    {
        var selectColumns = string.Join(", ", orderedColumns.Select(QuoteIdentifier));
        var source = $"{QuoteIdentifier(_options.Database)}.{QuoteIdentifier(snapshot.SourceTableName)}";
        var hashExpression = BuildRowHashExpression(orderedColumns, plan.Seed);
        var labelColumn = QuoteIdentifier(plan.LabelColumn);

        return plan.SplitStrategy switch
        {
            "StratifiedHoldout" => BuildStratifiedQuery(source, selectColumns, labelColumn, hashExpression, exportTrainRows),
            "ChronologicalHoldout" => BuildChronologicalQuery(source, selectColumns, snapshot.TimeColumn, hashExpression, exportTrainRows),
            "GroupedHoldout" => BuildGroupedQuery(source, selectColumns, snapshot.GroupColumn, plan.Seed, exportTrainRows),
            _ => BuildRandomQuery(source, selectColumns, hashExpression, exportTrainRows),
        };
    }

    private static string BuildRandomQuery(string source, string selectColumns, string hashExpression, bool exportTrainRows)
    {
        var predicate = exportTrainRows ? "split_bucket < 8" : "split_bucket >= 8";
        return $"""
            select {selectColumns}
            from (
                select {selectColumns},
                       {hashExpression} % 10 as split_bucket
                from {source}
            )
            where {predicate}
            format CSVWithNames
            """;
    }

    private static string BuildStratifiedQuery(string source, string selectColumns, string labelColumn, string hashExpression, bool exportTrainRows)
    {
        const string testCountExpression = "if(split_partition_count > 1, least(split_partition_count - 1, greatest(1, toInt64(floor(split_partition_count * 0.2)))), 0)";
        var predicate = exportTrainRows
            ? $"split_row_number > {testCountExpression}"
            : $"split_row_number <= {testCountExpression}";

        return $"""
            select {selectColumns}
            from (
                select {selectColumns},
                       row_number() over (partition by {labelColumn} order by {hashExpression}) as split_row_number,
                       count() over (partition by {labelColumn}) as split_partition_count
                from {source}
            )
            where {predicate}
            format CSVWithNames
            """;
    }

    private static string BuildChronologicalQuery(string source, string selectColumns, string? timeColumn, string hashExpression, bool exportTrainRows)
    {
        if (string.IsNullOrWhiteSpace(timeColumn))
        {
            throw new InvalidOperationException("Chronological split requires a time column in the feature snapshot.");
        }

        var orderedTimeColumn = QuoteIdentifier(timeColumn);
        const string testCountExpression = "if(split_total_count > 1, least(split_total_count - 1, greatest(1, toInt64(floor(split_total_count * 0.2)))), 0)";
        var predicate = exportTrainRows
            ? $"split_row_number <= split_total_count - {testCountExpression}"
            : $"split_row_number > split_total_count - {testCountExpression}";

        return $"""
            select {selectColumns}
            from (
                select {selectColumns},
                       row_number() over (order by {orderedTimeColumn} asc, {hashExpression} asc) as split_row_number,
                       count() over () as split_total_count
                from {source}
            )
            where {predicate}
            format CSVWithNames
            """;
    }

    private static string BuildGroupedQuery(string source, string selectColumns, string? groupColumn, int seed, bool exportTrainRows)
    {
        if (string.IsNullOrWhiteSpace(groupColumn))
        {
            throw new InvalidOperationException("Grouped split requires a group column in the feature snapshot.");
        }

        var groupBucket = $"cityHash64(toString({seed}), toString({QuoteIdentifier(groupColumn)})) % 10";
        var predicate = exportTrainRows ? "split_bucket < 8" : "split_bucket >= 8";

        return $"""
            select {selectColumns}
            from (
                select {selectColumns},
                       {groupBucket} as split_bucket
                from {source}
            )
            where {predicate}
            format CSVWithNames
            """;
    }

    private static string BuildRowHashExpression(IReadOnlyList<string> orderedColumns, int seed)
    {
        var arguments = orderedColumns
            .Select(column => $"toString({QuoteIdentifier(column)})")
            .Prepend($"toString({seed})");

        return $"cityHash64({string.Join(", ", arguments)})";
    }

    private Uri BuildUrl(string encodedQuery)
    {
        var endpoint = NormalizeEndpoint(_options.Endpoint);
        return new Uri($"{endpoint.TrimEnd('/')}/?database={Uri.EscapeDataString(_options.Database)}&query={encodedQuery}");
    }

    private static string NormalizeEndpoint(string endpoint)
    {
        var trimmed = endpoint.Trim();
        if (trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return trimmed;
        }

        return $"https://{trimmed}";
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (!IdentifierPattern.IsMatch(identifier))
        {
            throw new InvalidOperationException($"Unsafe ClickHouse identifier '{identifier}'.");
        }

        return $"\"{identifier}\"";
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, string query, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var body = response.Content is null
            ? string.Empty
            : (await response.Content.ReadAsStringAsync(cancellationToken)).Trim();

        throw new InvalidOperationException(
            $"ClickHouse export failed with status {(int)response.StatusCode} ({response.ReasonPhrase}). Query: {query}. Response: {body}");
    }
}
