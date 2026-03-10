using Aurelon.Models;
using Aurelon.Services;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace Aurelon.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DocumentsController(IR2ObjectStorage storage, IConfiguration configuration) : ControllerBase
{
    private readonly string _connectionString = configuration.GetConnectionString("DefaultConnection")!;

    [HttpPost("upload")]
    public async Task<IActionResult> UploadDocument(IFormFile file, [FromForm] string userId)
    {
        if (file.Length == 0) return BadRequest("File is empty.");

        var uploadId = Guid.CreateVersion7();
        var jobId = Guid.CreateVersion7();
        var extension = Path.GetExtension(file.FileName).ToLowerInvariant();
        var storageKey = $"uploads/documents/{userId}/{uploadId}{extension}";

        await using var stream = file.OpenReadStream();
        await storage.UploadAsync(stream, storageKey, file.ContentType, HttpContext.RequestAborted);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(HttpContext.RequestAborted);
        
        const string insertUploadSql = """
            INSERT INTO uploads (id, user_id, kind, original_file_name, display_name, content_type, storage_key, status, created_at_utc, updated_at_utc)
            VALUES (@Id, @UserId, @Kind, @OriginalFileName, @DisplayName, @ContentType, @StorageKey, 'Pending', @CreatedAt, @UpdatedAt)
            """;
        
        const string insertJobSql = """
            INSERT INTO document_jobs (id, upload_id, user_id, status, attempt_count, max_attempts, created_at_utc, updated_at_utc)
            VALUES (@Id, @UploadId, @UserId, 'Pending', 0, 5, @CreatedAt, @UpdatedAt)
            """;

        var now = DateTimeOffset.UtcNow;
        var tx = await conn.BeginTransactionAsync(HttpContext.RequestAborted);

        await conn.ExecuteAsync(insertUploadSql, new {
            Id = uploadId,
            UserId = userId,
            Kind = (int)UploadKind.Document,
            OriginalFileName = file.FileName,
            DisplayName = Path.GetFileNameWithoutExtension(file.FileName),
            ContentType = file.ContentType,
            StorageKey = storageKey,
            CreatedAt = now,
            UpdatedAt = now
        }, tx);

        await conn.ExecuteAsync(insertJobSql, new {
            Id = jobId,
            UploadId = uploadId,
            UserId = userId,
            CreatedAt = now,
            UpdatedAt = now
        }, tx);

        await tx.CommitAsync(HttpContext.RequestAborted);

        return Ok(new QueuedUploadResponse(uploadId, jobId, UploadKind.Document, JobStatus.Pending, storageKey));
    }
}
