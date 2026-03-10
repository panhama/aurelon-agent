using Aurelon.Models;
using Aurelon.Services;
using Aurelon.Repositories;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Aurelon.Workers;

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
                    ?? throw new InvalidOperationException("R2 object '" + lease.StorageKey + "' was not found.");

                var structuredDoc = await extractionService.ExtractAsync(stream, lease.OriginalFileName, stoppingToken);
                
                await indexingService.IndexAsync(lease, structuredDoc, stoppingToken);
                
                logger.LogInformation("Completed document ingestion for upload {UploadId}", lease.UploadId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Document ingestion failed for upload {UploadId}", lease.UploadId);
                await repository.MarkFailedAsync(lease.JobId, lease.UploadId, ex.Message, stoppingToken);
            }
        }
    }
}
