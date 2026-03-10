using OfficeOpenXml;
using Aurelon.Models;
using System.Text;

namespace Aurelon.Services;

public sealed class ExcelExtractor
{
    static ExcelExtractor()
    {
        ExcelPackage.License.SetNonCommercialPersonal("Aurelon");
    }

    public StructuredDocument Extract(string documentId, string fileName, string path)
    {
        using var package = new ExcelPackage(new FileInfo(path));
        var blocks = new List<ExtractedBlock>();
        var order = 0;

        foreach (var worksheet in package.Workbook.Worksheets)
        {
            var markdownTable = WorksheetToMarkdown(worksheet);
            if (!string.IsNullOrWhiteSpace(markdownTable))
            {
                blocks.Add(new ExtractedBlock(
                    PageNumber: 0,
                    Order: order++,
                    Kind: "table",
                    Text: markdownTable,
                    SectionPath: worksheet.Name
                ));
            }
        }

        return new StructuredDocument(documentId, fileName, "xlsx", blocks);
    }

    private static string WorksheetToMarkdown(ExcelWorksheet worksheet)
    {
        if (worksheet.Dimension == null) return string.Empty;

        var startRow = worksheet.Dimension.Start.Row;
        var endRow = worksheet.Dimension.End.Row;
        var startCol = worksheet.Dimension.Start.Column;
        var endCol = worksheet.Dimension.End.Column;

        var sb = new StringBuilder();
        var rows = new List<string[]>();

        for (int r = startRow; r <= endRow; r++)
        {
            var rowValues = new List<string>();
            var hasData = false;
            for (int c = startCol; c <= endCol; c++)
            {
                var val = worksheet.Cells[r, c].Text?.Trim() ?? "";
                if (!string.IsNullOrWhiteSpace(val)) hasData = true;
                rowValues.Add(NormalizeWhitespace(val));
            }
            if (hasData)
            {
                rows.Add(rowValues.ToArray());
            }
        }

        if (rows.Count == 0) return string.Empty;

        var width = rows.Max(r => r.Length);

        sb.AppendLine("| " + string.Join(" | ", rows[0]) + " |");
        sb.AppendLine("| " + string.Join(" | ", Enumerable.Repeat("---", width)) + " |");

        foreach (var row in rows.Skip(1))
        {
            sb.AppendLine("| " + string.Join(" | ", row) + " |");
        }

        return sb.ToString().Trim();
    }

    private static string NormalizeWhitespace(string input) =>
        System.Text.RegularExpressions.Regex.Replace(input.Replace('\u00A0', ' '), @"\s+", " ").Trim();
}
