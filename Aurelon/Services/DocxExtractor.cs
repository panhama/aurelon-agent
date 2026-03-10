using System.Text;
using System.Text.RegularExpressions;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
namespace Aurelon.Services;
public sealed class DocxExtractor
{
    public StructuredDocument Extract(string documentId, string fileName, string path)
    {
        using var doc = WordprocessingDocument.Open(path, false);
        var body = doc.MainDocumentPart?.Document.Body
                   ?? throw new InvalidOperationException("DOCX body not found.");

        var blocks = new List<ExtractedBlock>();
        var sectionStack = new List<string>();
        var order = 0;

        foreach (var element in body.Elements())
        {
            switch (element)
            {
                case Paragraph p:
                {
                    var text = NormalizeWhitespace(p.InnerText);
                    if (string.IsNullOrWhiteSpace(text)) break;

                    var style = p.ParagraphProperties?.ParagraphStyleId?.Val?.Value;
                    var isHeading = IsHeadingStyle(style);

                    if (isHeading)
                    {
                        var level = InferHeadingLevel(style);
                        UpdateSectionStack(sectionStack, level, text);

                        blocks.Add(new ExtractedBlock(
                            PageNumber: 0,
                            Order: order++,
                            Kind: "heading",
                            Text: text,
                            SectionPath: string.Join(" > ", sectionStack),
                            StyleName: style));
                    }
                    else
                    {
                        blocks.Add(new ExtractedBlock(
                            PageNumber: 0,
                            Order: order++,
                            Kind: "paragraph",
                            Text: text,
                            SectionPath: string.Join(" > ", sectionStack),
                            StyleName: style));
                    }

                    break;
                }

                case Table table:
                {
                    var markdown = TableToMarkdown(table);
                    if (!string.IsNullOrWhiteSpace(markdown))
                    {
                        blocks.Add(new ExtractedBlock(
                            PageNumber: 0,
                            Order: order++,
                            Kind: "table",
                            Text: markdown,
                            SectionPath: string.Join(" > ", sectionStack)));
                    }

                    break;
                }
            }
        }

        return new StructuredDocument(documentId, fileName, "docx", blocks);
    }

    private static bool IsHeadingStyle(string? style) =>
        !string.IsNullOrWhiteSpace(style) &&
        style.StartsWith("Heading", StringComparison.OrdinalIgnoreCase);

    private static int InferHeadingLevel(string? style)
    {
        if (style is null) return 1;
        var match = Regex.Match(style, @"(\d+)");
        return match.Success ? Math.Clamp(int.Parse(match.Groups[1].Value), 1, 6) : 1;
    }

    private static void UpdateSectionStack(List<string> stack, int level, string heading)
    {
        while (stack.Count >= level) stack.RemoveAt(stack.Count - 1);
        stack.Add(heading);
    }

    private static string NormalizeWhitespace(string input) =>
        Regex.Replace(input.Replace('\u00A0', ' '), @"\s+", " ").Trim();

    private static string TableToMarkdown(Table table)
    {
        var rows = table.Elements<TableRow>()
            .Select(r => r.Elements<TableCell>()
                .Select(c => NormalizeWhitespace(c.InnerText))
                .ToArray())
            .Where(r => r.Length > 0 && r.Any(cell => !string.IsNullOrWhiteSpace(cell)))
            .ToList();

        if (rows.Count == 0) return string.Empty;

        var width = rows.Max(r => r.Length);
        string[] Pad(string[] row) =>
            row.Concat(Enumerable.Repeat(string.Empty, width - row.Length)).ToArray();

        rows = rows.Select(Pad).ToList();

        var sb = new StringBuilder();
        sb.AppendLine("| " + string.Join(" | ", rows[0]) + " |");
        sb.AppendLine("| " + string.Join(" | ", Enumerable.Repeat("---", width)) + " |");

        foreach (var row in rows.Skip(1))
            sb.AppendLine("| " + string.Join(" | ", row) + " |");

        return sb.ToString().Trim();
    }
}