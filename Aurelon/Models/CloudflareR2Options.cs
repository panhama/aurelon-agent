namespace Aurelon.Models;

public sealed class CloudflareR2Options
{
    public string AccessKey { get; init; } = string.Empty;
    public string SecretKey { get; init; } = string.Empty;
    public string ServiceUrl { get; init; } = string.Empty;
    public string BucketName { get; init; } = string.Empty;
    public string? ModelBucketName { get; init; } = "training-model";
}
