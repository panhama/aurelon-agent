namespace Aurelon.Workers.Datasets.Models;

public sealed record DatasetLease(
    Guid JobId,
    Guid UploadId,
    Guid DatasetId,
    Guid DatasetVersionId,
    string UserId,
    string StorageKey,
    string OriginalFileName);

public sealed record ParsedDataset(
    IReadOnlyList<ParsedColumn> Columns,
    IReadOnlyList<IReadOnlyDictionary<string, string?>> Rows,
    int RowCount);

public sealed record ParsedColumn(
    string DisplayName,
    string StorageName,
    string ClickHouseType,
    bool IsNullable);
