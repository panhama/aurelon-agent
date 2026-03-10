namespace Aurelon.Services;

using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

public sealed class VoyageOptions
{
    public string ApiKey { get; init; } = "";
    public string Model { get; init; } = "voyage-4-lite";
    public int OutputDimension { get; init; } = 1024;
}

public sealed class VoyageEmbeddingClient
{
    private readonly HttpClient _http;
    private readonly VoyageOptions _options;

    public VoyageEmbeddingClient(HttpClient http, VoyageOptions options)
    {
        _http = http;
        _options = options;

        _http.BaseAddress = new Uri("https://api.voyageai.com/");
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", _options.ApiKey);
    }

    public async Task<float[][]> EmbedDocumentsAsync(IReadOnlyList<string> texts, CancellationToken ct)
        => await EmbedAsync(texts, "document", ct);

    public async Task<float[]> EmbedQueryAsync(string text, CancellationToken ct)
        => (await EmbedAsync(new[] { text }, "query", ct))[0];

    private async Task<float[][]> EmbedAsync(IReadOnlyList<string> inputs, string inputType, CancellationToken ct)
    {
        var body = new
        {
            input = inputs,
            model = _options.Model,
            input_type = inputType,
            output_dimension = _options.OutputDimension,
            output_dtype = "float",
            truncation = false
        };

        using var req = new HttpRequestMessage(HttpMethod.Post, "v1/embeddings")
        {
            Content = new StringContent(
                JsonSerializer.Serialize(body),
                Encoding.UTF8,
                "application/json")
        };

        using var resp = await _http.SendAsync(req, ct);
        resp.EnsureSuccessStatusCode();

        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        var payload = await JsonSerializer.DeserializeAsync<VoyageEmbeddingResponse>(stream, cancellationToken: ct)
                      ?? throw new InvalidOperationException("Voyage response was empty.");

        return payload.Data
            .OrderBy(x => x.Index)
            .Select(x => x.Embedding)
            .ToArray();
    }

    private sealed class VoyageEmbeddingResponse
    {
        [JsonPropertyName("data")]
        public List<Item> Data { get; init; } = new();

        public sealed class Item
        {
            [JsonPropertyName("index")]
            public int Index { get; init; }

            [JsonPropertyName("embedding")]
            public float[] Embedding { get; init; } = Array.Empty<float>();
        }
    }
}