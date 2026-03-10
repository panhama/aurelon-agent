namespace Aurelon.Jobs;

public sealed class DocumentJob : JobRecord
{
    public Guid UploadId { get; init; }
}
