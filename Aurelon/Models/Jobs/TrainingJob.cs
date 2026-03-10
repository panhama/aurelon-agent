namespace Aurelon.Jobs;

public sealed class TrainingJob : JobRecord
{
    public Guid DatasetVersionId { get; init; }
    public string ModelName { get; init; } = string.Empty;
    public string TaskType { get; init; } = string.Empty;
    public string LabelColumn { get; init; } = string.Empty;
    public string FeatureColumnsJson { get; init; } = "[]";
    public int MaxTrainingTimeSeconds { get; init; } = 1800;
    public string PlanJson { get; init; } = "{}";
    public Guid? FeatureSnapshotId { get; init; }
    public Guid? LeaseToken { get; init; }
    public DateTimeOffset? NextRetryAtUtc { get; init; }
    public int Priority { get; init; }
}
