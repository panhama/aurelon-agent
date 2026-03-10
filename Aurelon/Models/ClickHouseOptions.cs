namespace Aurelon.Options;

public sealed class ClickHouseOptions
{
    public string Endpoint { get; init; } = string.Empty;
    public string Database { get; init; } = string.Empty;
    public string? Username { get; init; }
    public string? Password { get; init; }
}
