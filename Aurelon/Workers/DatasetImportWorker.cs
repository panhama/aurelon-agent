using Aurelon.Infrastructure.Storage;
using Aurelon.Workers.Datasets.Repositories;
using Aurelon.Workers.Datasets.Services;

namespace Aurelon.Workers.Datasets.Workers;

public sealed class DatasetImportWorker(
    DatasetWorkRepository repository,
    DatasetParsingService parsingService,
    ClickHouseDatasetLoader clickHouseLoader,
    DatasetManifestBuilder manifestBuilder,
    IR2ObjectStorage storage,
    ILogger<DatasetImportWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Dataset worker started.");
        while (!stoppingToken.IsCancellationRequested)
        {
            var lease = await repository.TryLeaseNextAsync(stoppingToken);
            if (lease is null)
            {
                logger.LogDebug("No pending dataset jobs found.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                continue;
            }

            logger.LogInformation(
                "Leased dataset job {JobId} for version {DatasetVersionId} and upload {UploadId}. File: {FileName}",
                lease.JobId,
                lease.DatasetVersionId,
                lease.UploadId,
                lease.OriginalFileName);

            try
            {
                logger.LogInformation("Opening R2 object {StorageKey}", lease.StorageKey);
                await using var stream = await storage.OpenReadAsync(lease.StorageKey, stoppingToken)
                    ?? throw new InvalidOperationException($"R2 object '{lease.StorageKey}' was not found.");

                logger.LogInformation("Parsing dataset file {FileName}", lease.OriginalFileName);
                var dataset = await parsingService.ParseAsync(stream, lease.OriginalFileName, stoppingToken);

                logger.LogInformation(
                    "Parsed dataset version {DatasetVersionId} with {RowCount} rows and {ColumnCount} columns.",
                    lease.DatasetVersionId,
                    dataset.RowCount,
                    dataset.Columns.Count);

                var tableName = await clickHouseLoader.LoadAsync(lease.DatasetVersionId, dataset, stoppingToken);
                logger.LogInformation("Loaded dataset version {DatasetVersionId} into ClickHouse table {TableName}", lease.DatasetVersionId, tableName);

                var (manifestJson, summaryJson) = manifestBuilder.Build(
                    lease.DatasetId,
                    lease.DatasetVersionId,
                    Path.GetFileNameWithoutExtension(lease.OriginalFileName),
                    tableName,
                    dataset);

                await repository.MarkCompletedAsync(lease, tableName, dataset.RowCount, manifestJson, summaryJson, stoppingToken);
                logger.LogInformation("Completed dataset import for version {DatasetVersionId}", lease.DatasetVersionId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Dataset import failed for version {DatasetVersionId}", lease.DatasetVersionId);
                await repository.MarkFailedAsync(lease, ex.Message, stoppingToken);
            }
        }
    }
}
