using Aurelon.Models;
using Aurelon.Services;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace Aurelon.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DatasetsController(IR2ObjectStorage storage, IConfiguration configuration) : ControllerBase
{
    private readonly string _connectionString = configuration.GetConnectionString("DefaultConnection")!;

    [HttpPost("upload")]
    public async Task<IActionResult> UploadDataset(IFormFile file, [FromForm] string userId)
    {
        if (file.Length == 0) return BadRequest("File is empty.");

        var uploadId = Guid.CreateVersion7();
        var jobId = Guid.CreateVersion7();
        var datasetId = Guid.CreateVersion7();
        var datasetVersionId = Guid.CreateVersion7();
        
        var extension = Path.GetExtension(file.FileName).ToLowerInvariant();
        var storageKey = $"uploads/datasets/{userId}/{uploadId}{extension}";

        await using var stream = file.OpenReadStream();
        await storage.UploadAsync(stream, storageKey, file.ContentType, HttpContext.RequestAborted);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(HttpContext.RequestAborted);
        
        const string insertUploadSql = """
            INSERT INTO uploads (id, user_id, kind, original_file_name, display_name, content_type, storage_key, status, created_at_utc, updated_at_utc)
            VALUES (@Id, @UserId, @Kind, @OriginalFileName, @DisplayName, @ContentType, @StorageKey, 'Pending', @CreatedAt, @UpdatedAt)
            """;

        const string insertDatasetSql = """
            INSERT INTO datasets (id, user_id, name, created_at_utc, updated_at_utc)
            VALUES (@Id, @UserId, @Name, @CreatedAt, @UpdatedAt)
            """;

        const string insertVersionSql = """
            INSERT INTO dataset_versions (id, dataset_id, version_number, upload_id, status, created_at_utc, updated_at_utc)
            VALUES (@Id, @DatasetId, 1, @UploadId, 'Pending', @CreatedAt, @UpdatedAt)
            """;
            
        const string insertJobSql = """
            INSERT INTO dataset_jobs (id, upload_id, dataset_id, dataset_version_id, user_id, status, attempt_count, max_attempts, created_at_utc, updated_at_utc)
            VALUES (@Id, @UploadId, @DatasetId, @DatasetVersionId, @UserId, 'Pending', 0, 5, @CreatedAt, @UpdatedAt)
            """;

        var now = DateTimeOffset.UtcNow;
        var tx = await conn.BeginTransactionAsync(HttpContext.RequestAborted);

        await conn.ExecuteAsync(insertUploadSql, new {
            Id = uploadId, UserId = userId, Kind = (int)UploadKind.Dataset,
            OriginalFileName = file.FileName, DisplayName = Path.GetFileNameWithoutExtension(file.FileName),
            ContentType = file.ContentType, StorageKey = storageKey, CreatedAt = now, UpdatedAt = now
        }, tx);

        await conn.ExecuteAsync(insertDatasetSql, new {
            Id = datasetId, UserId = userId, Name = Path.GetFileNameWithoutExtension(file.FileName),
            CreatedAt = now, UpdatedAt = now
        }, tx);

        await conn.ExecuteAsync(insertVersionSql, new {
            Id = datasetVersionId, DatasetId = datasetId, UploadId = uploadId,
            CreatedAt = now, UpdatedAt = now
        }, tx);

        await conn.ExecuteAsync(insertJobSql, new {
            Id = jobId, UploadId = uploadId, DatasetId = datasetId, DatasetVersionId = datasetVersionId,
            UserId = userId, CreatedAt = now, UpdatedAt = now
        }, tx);

        await tx.CommitAsync(HttpContext.RequestAborted);

        return Ok(new QueuedUploadResponse(uploadId, jobId, UploadKind.Dataset, JobStatus.Pending, storageKey, datasetVersionId));
    }
}
