using System.Text.Json;
using Aurelon.Models;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace Aurelon.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TrainingController(IConfiguration configuration) : ControllerBase
{
    private readonly string _connectionString = configuration.GetConnectionString("DefaultConnection")!;

    [HttpPost("trigger")]
    public async Task<IActionResult> TriggerTraining([FromBody] TriggerTrainingRequest request)
    {
        var jobId = Guid.CreateVersion7();
        
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(HttpContext.RequestAborted);
        
        const string insertJobSql = """
            INSERT INTO training_jobs (
                id, user_id, dataset_version_id, model_name, task_type, label_column, feature_columns_json, 
                max_attempts, attempt_count, priority, plan_json, feature_snapshot_id, status, created_at_utc, updated_at_utc)
            VALUES (
                @Id, @UserId, @DatasetVersionId, @ModelName, @TaskType, @LabelColumn, cast(@FeatureColumnsJson as jsonb),
                5, 0, @Priority, cast(@PlanJson as jsonb), @FeatureSnapshotId, 'Pending', @CreatedAt, @UpdatedAt)
            """;

        var now = DateTimeOffset.UtcNow;
        var planJson = JsonSerializer.Serialize(request.Plan);

        await conn.ExecuteAsync(insertJobSql, new {
            Id = jobId,
            UserId = request.UserId,
            DatasetVersionId = request.DatasetVersionId,
            ModelName = request.ModelName,
            TaskType = request.Plan.TaskFamily,
            LabelColumn = request.Plan.LabelColumn,
            FeatureColumnsJson = JsonSerializer.Serialize(request.Plan.FeatureColumns),
            PlanJson = planJson,
            FeatureSnapshotId = request.FeatureSnapshotId,
            Priority = 0,
            CreatedAt = now,
            UpdatedAt = now
        });

        return Ok(new { JobId = jobId, Status = "Pending" });
    }
}

public record TriggerTrainingRequest(
    string UserId,
    Guid DatasetVersionId,
    string ModelName,
    Guid FeatureSnapshotId,
    TrainingPlanDto Plan
);
