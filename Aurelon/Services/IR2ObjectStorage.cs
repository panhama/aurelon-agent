namespace Aurelon.Services;

public interface IR2ObjectStorage
{
    Task<Stream?> OpenReadAsync(string storageKey, CancellationToken ct);
    Task<string> UploadAsync(Stream stream, string storageKey, string contentType, CancellationToken ct);
}
