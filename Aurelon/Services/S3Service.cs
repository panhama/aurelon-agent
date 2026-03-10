using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Net;

namespace Aurelon.Services;

public class S3Service
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _serviceUrl;
        private readonly TimeSpan _presignExpiry;

    public S3Service(IAmazonS3 s3Client, IConfiguration config)
    {
        _s3Client = s3Client;

        // Unified config naming to match DataAgent
        _bucketName = config["CloudflareR2:BucketName"]
            ?? throw new ArgumentNullException("CloudflareR2:BucketName missing");

        // The base URL where your files can be downloaded (e.g., https://pub-xyz.r2.dev)
        // Support both PublicEndpoint and ServiceUrl keys for compatibility with templates.
        _serviceUrl = config["CloudflareR2:PublicEndpoint"]
            ?? config["CloudflareR2:ServiceUrl"]
            ?? string.Empty;

        // Presigned URL expiry (in minutes). Default to 60 minutes if not provided or invalid.
        if (int.TryParse(config["CloudflareR2:PresignExpiryMinutes"], out var minutes) && minutes > 0)
        {
            _presignExpiry = TimeSpan.FromMinutes(minutes);
        }
        else
        {
            _presignExpiry = TimeSpan.FromHours(1);
        }
    }

    public async Task<string> UploadModelAsync(string localFilePath, string cloudKey)
    {
        await using var fileStream = new FileStream(localFilePath, FileMode.Open, FileAccess.Read);

        var request = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = cloudKey,
            InputStream = fileStream,
            ContentType = "application/zip",
            DisablePayloadSigning = true
        };

        var response = await _s3Client.PutObjectAsync(request);

        if (response.HttpStatusCode != HttpStatusCode.OK && response.HttpStatusCode != HttpStatusCode.Created)
        {
            throw new InvalidOperationException($"Failed to upload to R2. Status: {response.HttpStatusCode}");
        }

        // Generate a presigned URL for secure, time-limited access to the uploaded object.
        var presignRequest = new Amazon.S3.Model.GetPreSignedUrlRequest
        {
            BucketName = _bucketName,
            Key = cloudKey,
            Expires = DateTime.UtcNow.Add(_presignExpiry)
        };

        var presignedUrl = _s3Client.GetPreSignedURL(presignRequest);

        return presignedUrl;
    }

    public async Task DownloadFileAsync(string cloudKey, string localDestinationPath)
    {
        var request = new GetObjectRequest
        {
            BucketName = _bucketName,
            Key = cloudKey
        };

        using var response = await _s3Client.GetObjectAsync(request);
        
        if (response.HttpStatusCode != HttpStatusCode.OK)
        {
            throw new InvalidOperationException($"Failed to download from R2. Status: {response.HttpStatusCode}");
        }

        await response.WriteResponseStreamToFileAsync(localDestinationPath, false, default);
    }
}
