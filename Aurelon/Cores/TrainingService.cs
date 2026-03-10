using Aurelon.Models;
using Aurelon.Services;
using Microsoft.ML;
using Microsoft.ML.AutoML;
using Microsoft.ML.Data;
using System.Reflection;
using System.Security.Cryptography;

namespace Aurelon.Cores;

public interface ITaskTrainer
{
    Task<TrainingResult> TrainModelAsync(TrainingExecutionJob job, CancellationToken cancellationToken);
}

public sealed class TabularAutoMlTrainer(R2ArtifactService artifactService) : ITaskTrainer
{
    private readonly MLContext _mlContext = new(seed: 42);

    public async Task<TrainingResult> TrainModelAsync(TrainingExecutionJob job, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);

        var startedAt = DateTimeOffset.UtcNow;
        var loaderOptions = BuildLoaderOptions(job.FeatureSnapshot.Schema);
        var textLoader = _mlContext.Data.CreateTextLoader(loaderOptions);
        var trainData = textLoader.Load(job.TrainDatasetPath);
        var testData = textLoader.Load(job.TestDatasetPath);
        var taskType = ParseTaskType(job.Plan.TaskFamily);

        var columnInformation = BuildColumnInformation(job.Plan.LabelColumn);
        var (model, selectedTrainer) = taskType switch
        {
            MLTaskType.Regression => ExecuteRegression(trainData, columnInformation, job.Plan),
            MLTaskType.BinaryClassification => ExecuteBinaryClassification(trainData, columnInformation, job.Plan),
            MLTaskType.MulticlassClassification => ExecuteMulticlassClassification(trainData, columnInformation, job.Plan),
            _ => throw new NotSupportedException($"Task {job.Plan.TaskFamily} is not supported by the worker."),
        };

        var metrics = Evaluate(model, testData, taskType, job.Plan.LabelColumn);
        var tempZipPath = Path.Combine(Path.GetTempPath(), $"{job.Id}.zip");
        _mlContext.Model.Save(model, trainData.Schema, tempZipPath);

        var artifactBytes = await File.ReadAllBytesAsync(tempZipPath, cancellationToken);
        var artifactHash = Convert.ToHexString(SHA256.HashData(artifactBytes)).ToLowerInvariant();
        var artifactKey = $"models/{job.DatasetVersionId:N}/{job.Id:N}.zip";
        await artifactService.UploadModelAsync(tempZipPath, artifactKey, cancellationToken);
        File.Delete(tempZipPath);

        var completedAt = DateTimeOffset.UtcNow;
        var trainingDuration = completedAt - startedAt;
        var inputSchema = job.FeatureSnapshot.Schema
            .Where(column => !string.Equals(column.Name, job.Plan.LabelColumn, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var outputSchema = MapSchema(model.GetOutputSchema(trainData.Schema));
        var trainRowCount = CountRows(job.TrainDatasetPath);
        var testRowCount = CountRows(job.TestDatasetPath);
        var rowCounts = new ModelRowCountsDto(
            trainRowCount + testRowCount,
            trainRowCount,
            testRowCount);
        var featureContracts = inputSchema
            .Select(static column => new FeatureContractDto(
                column.Name,
                column.DataType,
                !column.IsNullable,
                column.SourceColumn ?? column.Name))
            .ToList();

        var manifest = new ModelManifestDto(
            Guid.CreateVersion7(),
            job.DatasetVersionId,
            job.ModelName,
            job.Plan.TaskFamily,
            artifactKey,
            artifactHash,
            featureContracts,
            metrics.ToDictionary(),
            completedAt,
            job.Plan.LabelColumn,
            job.FeatureSnapshot.Id,
            inputSchema,
            outputSchema,
            job.Plan.SplitStrategy,
            job.Plan.MetricName,
            selectedTrainer,
            job.Plan.Seed,
            rowCounts,
            GetCodeVersion(),
            completedAt,
            (int)Math.Ceiling(trainingDuration.TotalSeconds));

        return new TrainingResult
        {
            JobId = job.Id,
            Success = true,
            ArtifactKey = artifactKey,
            ArtifactHash = artifactHash,
            Manifest = manifest,
            Metrics = metrics,
            TrainingDuration = trainingDuration,
            CompletedAtUtc = completedAt,
        };
    }

    private (ITransformer Model, string SelectedTrainer) ExecuteRegression(IDataView trainData, ColumnInformation columnInformation, TrainingPlanDto plan)
    {
        var settings = new RegressionExperimentSettings
        {
            MaxExperimentTimeInSeconds = (uint)Math.Max(60, plan.TrainingBudgetSeconds),
            OptimizingMetric = plan.MetricName switch
            {
                "MeanAbsoluteError" => RegressionMetric.MeanAbsoluteError,
                "RootMeanSquaredError" => RegressionMetric.RootMeanSquaredError,
                _ => RegressionMetric.RSquared,
            },
        };

        var result = _mlContext.Auto()
            .CreateRegressionExperiment(settings)
            .Execute(trainData, columnInformation);

        return (result.BestRun.Model, result.BestRun.TrainerName ?? "Unknown");
    }

    private (ITransformer Model, string SelectedTrainer) ExecuteBinaryClassification(IDataView trainData, ColumnInformation columnInformation, TrainingPlanDto plan)
    {
        var settings = new BinaryExperimentSettings
        {
            MaxExperimentTimeInSeconds = (uint)Math.Max(60, plan.TrainingBudgetSeconds),
            OptimizingMetric = plan.MetricName switch
            {
                "AreaUnderRocCurve" => BinaryClassificationMetric.AreaUnderRocCurve,
                "F1Score" => BinaryClassificationMetric.F1Score,
                "Accuracy" => BinaryClassificationMetric.Accuracy,
                _ => BinaryClassificationMetric.AreaUnderPrecisionRecallCurve,
            },
        };

        var result = _mlContext.Auto()
            .CreateBinaryClassificationExperiment(settings)
            .Execute(trainData, columnInformation);

        return (result.BestRun.Model, result.BestRun.TrainerName ?? "Unknown");
    }

    private (ITransformer Model, string SelectedTrainer) ExecuteMulticlassClassification(IDataView trainData, ColumnInformation columnInformation, TrainingPlanDto plan)
    {
        var settings = new MulticlassExperimentSettings
        {
            MaxExperimentTimeInSeconds = (uint)Math.Max(60, plan.TrainingBudgetSeconds),
            OptimizingMetric = plan.MetricName switch
            {
                "MacroAccuracy" => MulticlassClassificationMetric.MacroAccuracy,
                "LogLoss" => MulticlassClassificationMetric.LogLoss,
                _ => MulticlassClassificationMetric.MicroAccuracy,
            },
        };

        var result = _mlContext.Auto()
            .CreateMulticlassClassificationExperiment(settings)
            .Execute(trainData, columnInformation);

        return (result.BestRun.Model, result.BestRun.TrainerName ?? "Unknown");
    }

    private ModelMetrics Evaluate(ITransformer model, IDataView testData, MLTaskType taskType, string labelColumn)
    {
        var predictions = model.Transform(testData);
        return taskType switch
        {
            MLTaskType.Regression => Map(_mlContext.Regression.Evaluate(predictions, labelColumnName: labelColumn)),
            MLTaskType.BinaryClassification => Map(_mlContext.BinaryClassification.Evaluate(predictions, labelColumnName: labelColumn)),
            MLTaskType.MulticlassClassification => Map(_mlContext.MulticlassClassification.Evaluate(predictions, labelColumnName: labelColumn)),
            _ => new ModelMetrics(),
        };
    }

    private static TextLoader.Options BuildLoaderOptions(IReadOnlyList<ModelSchemaColumnDto> schema)
    {
        var columns = new List<TextLoader.Column>(schema.Count);
        for (var index = 0; index < schema.Count; index++)
        {
            columns.Add(new TextLoader.Column(schema[index].Name, MapDataKind(schema[index].DataType), index));
        }

        return new TextLoader.Options
        {
            HasHeader = true,
            Separators = [','],
            AllowQuoting = true,
            TrimWhitespace = false,
            Columns = columns.ToArray(),
        };
    }

    private static ColumnInformation BuildColumnInformation(string labelColumn)
        => new()
        {
            LabelColumnName = labelColumn,
        };

    private static ModelMetrics Map(RegressionMetrics metrics) => new()
    {
        RSquared = metrics.RSquared,
        RMSE = metrics.RootMeanSquaredError,
        MAE = metrics.MeanAbsoluteError,
    };

    private static ModelMetrics Map(BinaryClassificationMetrics metrics) => new()
    {
        Accuracy = metrics.Accuracy,
        AUC = metrics.AreaUnderRocCurve,
        AUPRC = metrics.AreaUnderPrecisionRecallCurve,
        F1Score = metrics.F1Score,
    };

    private static ModelMetrics Map(MulticlassClassificationMetrics metrics) => new()
    {
        Accuracy = metrics.MicroAccuracy,
        MicroAccuracy = metrics.MicroAccuracy,
        LogLoss = metrics.LogLoss,
    };

    private static IReadOnlyList<ModelSchemaColumnDto> MapSchema(DataViewSchema schema)
        => schema.Select(column => new ModelSchemaColumnDto(
                column.Name,
                MapDataViewType(column.Type),
                column.Type is not NumberDataViewType and not BooleanDataViewType and not DateTimeDataViewType))
            .ToList();

    private static string MapDataViewType(DataViewType type) => type switch
    {
        BooleanDataViewType => "Bool",
        NumberDataViewType number when number.RawType == typeof(float) => "Float32",
        NumberDataViewType number when number.RawType == typeof(double) => "Float64",
        NumberDataViewType number when number.RawType == typeof(short) => "Int16",
        NumberDataViewType number when number.RawType == typeof(int) => "Int32",
        NumberDataViewType number when number.RawType == typeof(long) => "Int64",
        NumberDataViewType number when number.RawType == typeof(sbyte) => "Int8",
        NumberDataViewType number when number.RawType == typeof(ushort) => "UInt16",
        NumberDataViewType number when number.RawType == typeof(uint) => "UInt32",
        NumberDataViewType number when number.RawType == typeof(ulong) => "UInt64",
        NumberDataViewType number when number.RawType == typeof(byte) => "UInt8",
        DateTimeDataViewType => "DateTime",
        _ => "String",
    };

    private static DataKind MapDataKind(string dataType) => dataType switch
    {
        "Bool" => DataKind.Boolean,
        "Float32" => DataKind.Single,
        "Float64" => DataKind.Double,
        "Int8" => DataKind.SByte,
        "UInt8" => DataKind.Byte,
        "Int16" => DataKind.Int16,
        "UInt16" => DataKind.UInt16,
        "Int32" => DataKind.Int32,
        "UInt32" => DataKind.UInt32,
        "Int64" => DataKind.Int64,
        "UInt64" => DataKind.UInt64,
        "DateTime" => DataKind.DateTime,
        _ => DataKind.String,
    };

    private static MLTaskType ParseTaskType(string taskFamily) => taskFamily switch
    {
        "Regression" => MLTaskType.Regression,
        "BinaryClassification" => MLTaskType.BinaryClassification,
        "MulticlassClassification" => MLTaskType.MulticlassClassification,
        _ => throw new InvalidOperationException($"Unsupported ML task type '{taskFamily}'."),
    };

    private static int CountRows(string csvPath)
    {
        using var stream = File.OpenRead(csvPath);
        using var reader = new StreamReader(stream);
        _ = reader.ReadLine();

        var rows = 0;
        while (reader.ReadLine() is not null)
        {
            rows++;
        }

        return rows;
    }

    private static string GetCodeVersion()
        => Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "unknown";
}
