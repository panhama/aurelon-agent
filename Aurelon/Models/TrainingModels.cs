namespace Aurelon.Models;

public record TrainingPlanDto(
    string TaskFamily,
    string LabelColumn,
    IReadOnlyList<string> FeatureColumns,
    string SplitStrategy,
    string MetricName,
    IReadOnlyList<string> Trainers,
    string? TimeColumn,
    string? GroupColumn,
    int Seed,
    int TrainingBudgetSeconds
);

public record ModelManifestDto(
    Guid ModelId,
    Guid DatasetVersionId,
    string ModelName,
    string TaskType,
    string ArtifactKey,
    string ArtifactHash,
    IReadOnlyList<FeatureContractDto> FeatureContracts,
    IReadOnlyDictionary<string, double> Metrics,
    DateTimeOffset CreatedAtUtc,
    string LabelColumn,
    Guid FeatureSnapshotId,
    IReadOnlyList<ModelSchemaColumnDto> InputSchema,
    IReadOnlyList<ModelSchemaColumnDto> OutputSchema,
    string SplitStrategy,
    string MetricName,
    string SelectedTrainer,
    int Seed,
    ModelRowCountsDto RowCounts,
    string CodeVersion,
    DateTimeOffset CompletedAtUtc,
    int TrainingDurationSeconds
);

public record ModelSchemaColumnDto(
    string Name,
    string DataType,
    bool IsNullable,
    string? SourceColumn = null
);

public record ModelRowCountsDto(
    long Total,
    long Train,
    long Test
);

public record FeatureContractDto(
    string Name,
    string DataType,
    bool IsRequired,
    string SourceColumn
);
