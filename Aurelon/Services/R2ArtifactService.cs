using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using System.Net;

namespace Aurelon.Services;

public sealed class R2ArtifactService
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;

    public R2ArtifactService(IAmazonS3 s3Client, IConfiguration config)
    {
        _s3Client = s3Client;
        _bucketName = config["CloudflareR2:BucketName"]
            ?? throw new ArgumentNullException("CloudflareR2:BucketName missing");
    }

    public async Task UploadModelAsync(string localFilePath, string cloudKey, CancellationToken cancellationToken)
    {
        await using var fileStream = new FileStream(localFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = cloudKey,
            InputStream = fileStream,
            ContentType = "application/zip",
            DisablePayloadSigning = true,
        };

        var response = await _s3Client.PutObjectAsync(request, cancellationToken);
        if (response.HttpStatusCode is not HttpStatusCode.OK and not HttpStatusCode.Created)
        {
            throw new InvalidOperationException($"Failed to upload model artifact. Status: {response.HttpStatusCode}");
        }
    }
}
