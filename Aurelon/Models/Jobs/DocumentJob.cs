namespace Aurelon.Models;

public sealed class DocumentJob : JobRecord
{
    public Guid UploadId { get; init; }
}
