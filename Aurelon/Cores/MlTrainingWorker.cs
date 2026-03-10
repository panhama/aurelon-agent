using System.Text.Json;
using Aurelon.Models;
using Aurelon.Services;
using Dapper;
using Npgsql;

namespace Aurelon.Cores;

public sealed class MlTrainingWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ILogger<MlTrainingWorker> _logger;
    private readonly string _dbConnString;
    private readonly ITaskTrainer _taskTrainer;
    private readonly ClickHouseTrainingDataService _clickHouseTrainingDataService;

    public MlTrainingWorker(
        ILogger<MlTrainingWorker> logger,
        IConfiguration config,
        ITaskTrainer taskTrainer,
        ClickHouseTrainingDataService clickHouseTrainingDataService)
    {
        _logger = logger;
        _dbConnString = config.GetConnectionString("DefaultConnection")
                        ?? throw new InvalidOperationException("DefaultConnection not found in configuration.");
        _taskTrainer = taskTrainer;
        _clickHouseTrainingDataService = clickHouseTrainingDataService;

        var dbBuilder = new NpgsqlConnectionStringBuilder(_dbConnString);
        _logger.LogInformation("ML worker configured for Postgres host {Host} database {Database}.", dbBuilder.Host, dbBuilder.Database);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ML worker started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await TryProcessNextJobAsync(stoppingToken);
                if (!processed)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception in ML worker loop.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task<bool> TryProcessNextJobAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_dbConnString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        var leaseToken = Guid.CreateVersion7();
        const string leaseSql = """
            with next_job as (
                select id
                from training_jobs
                where status = 'Pending'
                   or (
                        status = 'RetryableFailed'
                        and coalesce(next_retry_at_utc, now() at time zone 'utc') <= now() at time zone 'utc'
                   )
                   or (
                        status = 'Processing'
                        and coalesce(leased_until_utc, now() at time zone 'utc' - interval '1 second') <= now() at time zone 'utc'
                   )
                order by priority desc, created_at_utc asc, id asc
                for update skip locked
                limit 1
            )
            update training_jobs tj
            set status = 'Processing',
                started_at_utc = coalesce(tj.started_at_utc, now() at time zone 'utc'),
                heartbeat_at_utc = now() at time zone 'utc',
                leased_until_utc = now() at time zone 'utc' + interval '30 minutes',
                updated_at_utc = now() at time zone 'utc',
                last_error = null,
                next_retry_at_utc = null,
                lease_token = @LeaseToken
            from next_job nj
            where tj.id = nj.id
            returning
                tj.id as Id,
                tj.dataset_version_id as DatasetVersionId,
                tj.model_name as ModelName,
                tj.plan_json::text as PlanJson,
                tj.feature_snapshot_id as FeatureSnapshotId,
                tj.lease_token as LeaseToken,
                tj.attempt_count as AttemptCount,
                tj.max_attempts as MaxAttempts;
            """;

        var leased = await conn.QuerySingleOrDefaultAsync<LeasedTrainingJob>(
            new CommandDefinition(leaseSql, new { LeaseToken = leaseToken }, tx, cancellationToken: cancellationToken));
        if (leased is null)
        {
            await tx.CommitAsync(cancellationToken);
            return false;
        }

        if (leased.FeatureSnapshotId is null || string.IsNullOrWhiteSpace(leased.PlanJson))
        {
            await tx.CommitAsync(cancellationToken);
            await MarkFailedAsync(leased, "Training job is missing persisted plan or feature snapshot metadata.", cancellationToken);
            return true;
        }

        const string snapshotSql = """
            select
                fs.id as Id,
                fs.dataset_version_id as DatasetVersionId,
                fs.source_table_name as SourceTableName,
                fs.label_column as LabelColumn,
                fs.feature_columns_json::text as FeatureColumnsJson,
                fs.schema_json::text as SchemaJson,
                fs.time_column as TimeColumn,
                fs.group_column as GroupColumn
            from feature_snapshots fs
            where fs.id = @FeatureSnapshotId
            limit 1;
            """;

        var snapshotRow = await conn.QuerySingleOrDefaultAsync<FeatureSnapshotRow>(
            new CommandDefinition(snapshotSql, new { FeatureSnapshotId = leased.FeatureSnapshotId }, tx, cancellationToken: cancellationToken));
        await tx.CommitAsync(cancellationToken);

        if (snapshotRow is null)
        {
            await MarkFailedAsync(leased, "Feature snapshot was not found for the training job.", cancellationToken);
            return true;
        }

        var trainingPlan = DeserializePlan(leased.PlanJson);
        var featureSnapshot = new FeatureSnapshotDefinition
        {
            Id = snapshotRow.Id,
            DatasetVersionId = snapshotRow.DatasetVersionId,
            SourceTableName = snapshotRow.SourceTableName,
            LabelColumn = snapshotRow.LabelColumn,
            FeatureColumns = DeserializeStringList(snapshotRow.FeatureColumnsJson),
            Schema = DeserializeSchema(snapshotRow.SchemaJson),
            TimeColumn = snapshotRow.TimeColumn,
            GroupColumn = snapshotRow.GroupColumn,
        };

        var tempTrainCsvPath = Path.Combine(Path.GetTempPath(), $"{leased.Id}-train.csv");
        var tempTestCsvPath = Path.Combine(Path.GetTempPath(), $"{leased.Id}-test.csv");
        CancellationTokenSource? heartbeatCts = null;
        Task? heartbeatTask = null;

        try
        {
            _logger.LogInformation(
                "Training job {JobId} exporting feature snapshot {FeatureSnapshotId} with split {SplitStrategy}.",
                leased.Id,
                featureSnapshot.Id,
                trainingPlan.SplitStrategy);

            await _clickHouseTrainingDataService.ExportSplitAsync(featureSnapshot, trainingPlan, tempTrainCsvPath, tempTestCsvPath, cancellationToken);

            var trainingJob = new TrainingExecutionJob
            {
                Id = leased.Id,
                DatasetVersionId = leased.DatasetVersionId,
                ModelName = leased.ModelName,
                TrainDatasetPath = tempTrainCsvPath,
                TestDatasetPath = tempTestCsvPath,
                Plan = trainingPlan,
                FeatureSnapshot = featureSnapshot,
            };

            _logger.LogInformation(
                "Training job {JobId} is starting ML.NET training. Task={TaskType}, Label={LabelColumn}, Features={FeatureCount}, Split={SplitStrategy}, Metric={MetricName}, Seed={Seed}.",
                leased.Id,
                trainingPlan.TaskFamily,
                trainingPlan.LabelColumn,
                trainingPlan.FeatureColumns.Count,
                trainingPlan.SplitStrategy,
                trainingPlan.MetricName,
                trainingPlan.Seed);

            heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            heartbeatTask = RunHeartbeatAsync(leased.Id, leased.LeaseToken, heartbeatCts.Token);
            var result = await _taskTrainer.TrainModelAsync(trainingJob, cancellationToken);
            heartbeatCts.Cancel();
            await IgnoreCancellationAsync(heartbeatTask);

            _logger.LogInformation("Training job {JobId} finished ML.NET training in {Duration}.", leased.Id, result.TrainingDuration);
            await PersistSuccessAsync(leased, result, cancellationToken);
            _logger.LogInformation("Training job {JobId} persisted successfully with artifact {ArtifactKey}.", leased.Id, result.ArtifactKey);
            return true;
        }
        catch (LeaseLostException ex)
        {
            _logger.LogWarning(ex, "Training job {JobId} lost its lease before persisting results.", leased.Id);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Training job {JobId} failed.", leased.Id);
            if (heartbeatCts is not null)
            {
                heartbeatCts.Cancel();
            }

            if (heartbeatTask is not null)
            {
                await IgnoreCancellationAsync(heartbeatTask);
            }

            await MarkFailedAsync(leased, ex.Message, cancellationToken);
            return true;
        }
        finally
        {
            heartbeatCts?.Dispose();
            DeleteIfExists(tempTrainCsvPath);
            DeleteIfExists(tempTestCsvPath);
        }
    }

    private async Task RunHeartbeatAsync(Guid jobId, Guid leaseToken, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            await UpdateHeartbeatAsync(jobId, leaseToken, cancellationToken);
            _logger.LogInformation("Training job {JobId} heartbeat updated.", jobId);
        }
    }

    private async Task UpdateHeartbeatAsync(Guid jobId, Guid leaseToken, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_dbConnString);
        await conn.OpenAsync(cancellationToken);
        const string sql = """
            update training_jobs
            set heartbeat_at_utc = now() at time zone 'utc',
                leased_until_utc = now() at time zone 'utc' + interval '30 minutes',
                updated_at_utc = now() at time zone 'utc'
            where id = @JobId and status = 'Processing' and lease_token = @LeaseToken;
            """;
        await conn.ExecuteAsync(new CommandDefinition(sql, new { JobId = jobId, LeaseToken = leaseToken }, cancellationToken: cancellationToken));
    }

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task PersistSuccessAsync(LeasedTrainingJob leased, TrainingResult result, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_dbConnString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        const string completeJobSql = """
            update training_jobs
            set status = 'Completed',
                completed_at_utc = now() at time zone 'utc',
                updated_at_utc = now() at time zone 'utc',
                heartbeat_at_utc = null,
                leased_until_utc = null,
                lease_token = null,
                next_retry_at_utc = null,
                last_error = null
            where id = @JobId and lease_token = @LeaseToken;
            """;
        const string modelInsertSql = """
            insert into model_registry (id, dataset_version_id, task_type, status, artifact_key, artifact_hash, manifest_json, created_at_utc, published_at_utc)
            values (@Id, @DatasetVersionId, @TaskType, @Status, @ArtifactKey, @ArtifactHash, cast(@ManifestJson as jsonb), @CreatedAtUtc, @PublishedAtUtc);
            """;
        const string outboxSql = """
            insert into outbox_events (id, event_type, aggregate_id, job_id, payload_json, occurred_at_utc, processed_at_utc)
            values (@Id, @EventType, @AggregateId, @JobId, cast(@PayloadJson as jsonb), @OccurredAtUtc, null);
            """;

        var affected = await conn.ExecuteAsync(new CommandDefinition(
            completeJobSql,
            new { JobId = leased.Id, LeaseToken = leased.LeaseToken },
            tx,
            cancellationToken: cancellationToken));

        if (affected == 0)
        {
            await tx.RollbackAsync(cancellationToken);
            throw new LeaseLostException(leased.Id);
        }

        await conn.ExecuteAsync(new CommandDefinition(modelInsertSql, new
        {
            Id = result.Manifest!.ModelId,
            DatasetVersionId = leased.DatasetVersionId,
            TaskType = result.Manifest.TaskType,
            Status = "Draft",
            ArtifactKey = result.ArtifactKey!,
            ArtifactHash = result.ArtifactHash!,
            ManifestJson = JsonSerializer.Serialize(result.Manifest, JsonOptions),
            CreatedAtUtc = result.CompletedAtUtc.UtcDateTime,
            PublishedAtUtc = (DateTime?)null,
        }, tx, cancellationToken: cancellationToken));
        await conn.ExecuteAsync(new CommandDefinition(outboxSql, new
        {
            Id = Guid.CreateVersion7(),
            EventType = "TrainingCompleted",
            AggregateId = result.Manifest.ModelId,
            JobId = leased.Id,
            PayloadJson = JsonSerializer.Serialize(new
            {
                JobId = leased.Id,
                ModelId = result.Manifest.ModelId,
                DatasetVersionId = leased.DatasetVersionId,
                result.ArtifactKey,
                Metrics = result.Metrics?.ToDictionary(),
                FeatureSnapshotId = result.Manifest.FeatureSnapshotId,
                SplitStrategy = result.Manifest.SplitStrategy,
                MetricName = result.Manifest.MetricName,
            }),
            OccurredAtUtc = result.CompletedAtUtc.UtcDateTime,
        }, tx, cancellationToken: cancellationToken));

        await tx.CommitAsync(cancellationToken);
    }

    private async Task MarkFailedAsync(LeasedTrainingJob leased, string errorMessage, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_dbConnString);
        await conn.OpenAsync(cancellationToken);
        const string sql = """
            update training_jobs
            set attempt_count = attempt_count + 1,
                status = case when attempt_count + 1 >= max_attempts then 'DeadLetter' else 'RetryableFailed' end,
                last_error = @ErrorMessage,
                updated_at_utc = now() at time zone 'utc',
                heartbeat_at_utc = null,
                leased_until_utc = null,
                lease_token = null,
                next_retry_at_utc = case when attempt_count + 1 >= max_attempts then null else @NextRetryAtUtc end
            where id = @JobId and lease_token = @LeaseToken;
            """;

        var nextRetryAtUtc = DateTimeOffset.UtcNow.Add(ComputeRetryDelay(leased.AttemptCount + 1));
        await conn.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                JobId = leased.Id,
                LeaseToken = leased.LeaseToken,
                ErrorMessage = errorMessage,
                NextRetryAtUtc = nextRetryAtUtc.UtcDateTime,
            },
            cancellationToken: cancellationToken));
    }

    private static TimeSpan ComputeRetryDelay(int nextAttempt)
    {
        var cappedAttempt = Math.Min(nextAttempt, 6);
        var baseDelayMinutes = Math.Pow(2, cappedAttempt - 1);
        var jitterSeconds = Math.Min(30, nextAttempt * 3);
        return TimeSpan.FromMinutes(baseDelayMinutes) + TimeSpan.FromSeconds(jitterSeconds);
    }

    private static TrainingPlanDto DeserializePlan(string planJson)
        => JsonSerializer.Deserialize<TrainingPlanDto>(planJson, JsonOptions)
           ?? throw new InvalidOperationException("Training job plan could not be deserialized.");

    private static IReadOnlyList<ModelSchemaColumnDto> DeserializeSchema(string schemaJson)
        => JsonSerializer.Deserialize<List<ModelSchemaColumnDto>>(schemaJson, JsonOptions)
           ?? throw new InvalidOperationException("Feature snapshot schema could not be deserialized.");

    private static IReadOnlyList<string> DeserializeStringList(string json)
        => JsonSerializer.Deserialize<List<string>>(json, JsonOptions) ?? [];

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class LeasedTrainingJob
    {
        public Guid Id { get; init; }
        public Guid DatasetVersionId { get; init; }
        public string ModelName { get; init; } = string.Empty;
        public string PlanJson { get; init; } = "{}";
        public Guid? FeatureSnapshotId { get; init; }
        public Guid LeaseToken { get; init; }
        public int AttemptCount { get; init; }
        public int MaxAttempts { get; init; }
    }

    private sealed class FeatureSnapshotRow
    {
        public Guid Id { get; init; }
        public Guid DatasetVersionId { get; init; }
        public string SourceTableName { get; init; } = string.Empty;
        public string LabelColumn { get; init; } = string.Empty;
        public string FeatureColumnsJson { get; init; } = "[]";
        public string SchemaJson { get; init; } = "[]";
        public string? TimeColumn { get; init; }
        public string? GroupColumn { get; init; }
    }

    private sealed class LeaseLostException(Guid jobId) : Exception($"Training job {jobId} no longer owns its lease.");
}
