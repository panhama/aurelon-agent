using OfficeOpenXml;
using Aurelon.Models;

namespace Aurelon.Services;

public sealed class DatasetParsingService
{
    static DatasetParsingService()
    {
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
    }

    public async Task<ParsedDataset> ParseAsync(Stream stream, string fileName, CancellationToken ct)
    {
        var extension = Path.GetExtension(fileName).ToLowerInvariant();
        if (extension != ".xlsx")
        {
            throw new NotSupportedException("Only .xlsx files are supported for training data.");
        }

        using var package = new ExcelPackage(stream);
        var worksheet = package.Workbook.Worksheets[0]; // Assume first sheet
        
        if (worksheet.Dimension == null)
            return new ParsedDataset([], [], 0);

        var startRow = worksheet.Dimension.Start.Row;
        var endRow = worksheet.Dimension.End.Row;
        var startCol = worksheet.Dimension.Start.Column;
        var endCol = worksheet.Dimension.End.Column;

        var columns = new List<ParsedColumn>();
        for (int c = startCol; c <= endCol; c++)
        {
            var header = worksheet.Cells[startRow, c].Text?.Trim() ?? $"Column{c}";
            columns.Add(new ParsedColumn(
                DisplayName: header,
                StorageName: header.ToLowerInvariant().Replace(" ", "_"),
                ClickHouseType: "String", // Default to String for simplicity
                IsNullable: true
            ));
        }

        var rows = new List<IReadOnlyDictionary<string, string?>>();
        for (int r = startRow + 1; r <= endRow; r++)
        {
            var rowData = new Dictionary<string, string?>();
            for (int c = startCol; c <= endCol; c++)
            {
                rowData[columns[c - startCol].DisplayName] = worksheet.Cells[r, c].Text;
            }
            rows.Add(rowData);
        }

        return new ParsedDataset(columns, rows, rows.Count);
    }
}
