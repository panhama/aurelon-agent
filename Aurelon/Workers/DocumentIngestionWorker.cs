using Aurelon.Infrastructure.Extensions;
using Aurelon.Infrastructure.Storage;
using Aurelon.Workers.Documents.Models;
using Aurelon.Workers.Documents.Options;
using Aurelon.Workers.Documents.Repositories;
using Aurelon.Workers.Documents.Services;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.VectorData;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using OpenAI;
using Qdrant.Client;
using System.ClientModel;

namespace Aurelon.Workers.Documents.Workers;

public sealed class DocumentIngestionWorker(
    DocumentWorkRepository repository,
    DocumentExtractionService extractionService,
    DocumentIndexingService indexingService,
    IR2ObjectStorage storage,
    ILogger<DocumentIngestionWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Document worker started.");
        while (!stoppingToken.IsCancellationRequested)
        {
            var lease = await repository.TryLeaseNextAsync(stoppingToken);
            if (lease is null)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                continue;
            }

            try
            {
                await using var stream = await storage.OpenReadAsync(lease.StorageKey, stoppingToken)
                    ?? throw new InvalidOperationException($"R2 object '{lease.StorageKey}' was not found.");

                var extracted = await extractionService.ExtractAsync(stream, lease.OriginalFileName, stoppingToken);
                var now = DateTimeOffset.UtcNow;
                var chunks = extracted.Select(chunk => new DocumentChunkRecord(
                    CreateDeterministicGuid(lease.UploadId, chunk.PageNumber ?? 0, chunk.ChunkIndex),
                    lease.UploadId,
                    lease.JobId,
                    lease.UserId,
                    chunk.ChunkIndex,
                    chunk.PageNumber,
                    lease.DisplayName,
                    lease.StorageKey,
                    chunk.Text.Length > 240 ? chunk.Text[..240] + "..." : chunk.Text,
                    chunk.Text,
                    now)).ToList();

                await repository.ReplaceChunksAsync(lease, chunks, stoppingToken);
                await indexingService.IndexAsync(lease, chunks, stoppingToken);
                logger.LogInformation("Completed document ingestion for upload {UploadId}", lease.UploadId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Document ingestion failed for upload {UploadId}", lease.UploadId);
                await repository.MarkFailedAsync(lease.JobId, lease.UploadId, ex.Message, stoppingToken);
            }
        }
    }

    private static Guid CreateDeterministicGuid(Guid uploadId, int pageNumber, int chunkIndex)
    {
        Span<byte> source = stackalloc byte[24];
        uploadId.TryWriteBytes(source);
        BitConverter.TryWriteBytes(source[16..20], pageNumber);
        BitConverter.TryWriteBytes(source[20..24], chunkIndex);
        var hash = System.Security.Cryptography.SHA256.HashData(source.ToArray());
        var guidBytes = hash[..16].ToArray();
        return new Guid(guidBytes);
    }
}
