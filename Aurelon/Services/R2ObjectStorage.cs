using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using Aurelon.Models;

namespace Aurelon.Services;

public sealed class R2ObjectStorage(IAmazonS3 s3Client, IOptions<CloudflareR2Options> options) : IR2ObjectStorage
{
    private readonly string _bucketName = options.Value.BucketName;

    public async Task<Stream?> OpenReadAsync(string storageKey, CancellationToken ct)
    {
        try
        {
            var response = await s3Client.GetObjectAsync(_bucketName, storageKey, ct);
            return response.ResponseStream;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<string> UploadAsync(Stream stream, string storageKey, string contentType, CancellationToken ct)
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = storageKey,
            InputStream = stream,
            ContentType = contentType,
            DisablePayloadSigning = true
        };

        await s3Client.PutObjectAsync(request, ct);
        return storageKey;
    }
}
