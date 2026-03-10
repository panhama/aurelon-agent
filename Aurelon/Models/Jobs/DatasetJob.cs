namespace Aurelon.Models;

public sealed class DatasetJob : JobRecord
{
    public Guid UploadId { get; init; }
    public Guid DatasetId { get; init; }
    public Guid DatasetVersionId { get; init; }
}
