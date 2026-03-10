using Aurelon.Jobs;

namespace Aurelon.Datasets;

public sealed class DatasetVersion
{
    public Guid Id { get; init; } = Guid.CreateVersion7();
    public Guid DatasetId { get; init; }
    public Guid UploadId { get; init; }
    public string SourceFileKey { get; init; } = string.Empty;
    public string SourceFileName { get; init; } = string.Empty;
    public string SchemaHash { get; init; } = string.Empty;
    public int? RowCount { get; init; }
    public string? ClickHouseTableName { get; init; }
    public JobStatus Status { get; init; } = JobStatus.Pending;
    public DateTimeOffset ImportedAtUtc { get; init; } = DateTimeOffset.UtcNow;
}
