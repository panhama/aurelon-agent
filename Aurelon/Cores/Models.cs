using Aurelon.Models;

namespace Aurelon.MlWorker.Cores;

public sealed class TrainingExecutionJob
{
    public Guid Id { get; init; }
    public Guid DatasetVersionId { get; init; }
    public string ModelName { get; init; } = string.Empty;
    public string TrainDatasetPath { get; set; } = string.Empty;
    public string TestDatasetPath { get; set; } = string.Empty;
    public TrainingPlanDto Plan { get; init; } = new(
        "Regression",
        string.Empty,
        [],
        "RandomHoldout",
        "RSquared",
        [],
        null,
        null,
        42,
        300);
    public FeatureSnapshotDefinition FeatureSnapshot { get; init; } = new();
}

public sealed class TrainingResult
{
    public Guid JobId { get; init; }
    public bool Success { get; init; }
    public string? ArtifactKey { get; init; }
    public string? ArtifactHash { get; init; }
    public ModelManifestDto? Manifest { get; init; }
    public ModelMetrics? Metrics { get; init; }
    public TimeSpan TrainingDuration { get; init; }
    public DateTimeOffset CompletedAtUtc { get; init; }
    public string? ErrorMessage { get; init; }
}

public sealed class FeatureSnapshotDefinition
{
    public Guid Id { get; init; }
    public Guid DatasetVersionId { get; init; }
    public string SourceTableName { get; init; } = string.Empty;
    public string LabelColumn { get; init; } = string.Empty;
    public IReadOnlyList<string> FeatureColumns { get; init; } = [];
    public IReadOnlyList<ModelSchemaColumnDto> Schema { get; init; } = [];
    public string? TimeColumn { get; init; }
    public string? GroupColumn { get; init; }
}

public sealed class ModelMetrics
{
    public double? Accuracy { get; init; }
    public double? AUC { get; init; }
    public double? AUPRC { get; init; }
    public double? F1Score { get; init; }
    public double? MicroAccuracy { get; init; }
    public double? LogLoss { get; init; }
    public double? RSquared { get; init; }
    public double? RMSE { get; init; }
    public double? MAE { get; init; }

    public IReadOnlyDictionary<string, double> ToDictionary()
    {
        var values = new Dictionary<string, double>();
        if (Accuracy.HasValue) values["accuracy"] = Accuracy.Value;
        if (AUC.HasValue) values["auc"] = AUC.Value;
        if (AUPRC.HasValue) values["auprc"] = AUPRC.Value;
        if (F1Score.HasValue) values["f1"] = F1Score.Value;
        if (MicroAccuracy.HasValue) values["micro_accuracy"] = MicroAccuracy.Value;
        if (LogLoss.HasValue) values["log_loss"] = LogLoss.Value;
        if (RSquared.HasValue) values["r_squared"] = RSquared.Value;
        if (RMSE.HasValue) values["rmse"] = RMSE.Value;
        if (MAE.HasValue) values["mae"] = MAE.Value;
        return values;
    }
}

public enum MLTaskType
{
    BinaryClassification,
    MulticlassClassification,
    Regression,
    Forecasting,
    AnomalyDetection,
}
