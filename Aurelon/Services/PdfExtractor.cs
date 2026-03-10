using UglyToad.PdfPig;
using UglyToad.PdfPig.Content;
using UglyToad.PdfPig.DocumentLayoutAnalysis.PageSegmenter;
namespace Aurelon.Services;
public sealed class PdfExtractor
{
    public StructuredDocument Extract(string documentId, string fileName, string path)
    {
        using var pdf = PdfDocument.Open(path);

        var blocks = new List<ExtractedBlock>();
        var order = 0;
        var currentSection = new List<string>();

        foreach (var page in pdf.GetPages())
        {
            var words = page.GetWords().ToList();
            if (words.Count == 0) continue;

            var segments = DocstrumBoundingBoxes.Instance.GetBlocks(words);
            var medianFont = GetMedianFontSize(words);

            foreach (var segment in segments)
            {
                var text = string.Join("\n", segment.TextLines.Select(l => l.Text)).Trim();
                if (string.IsNullOrWhiteSpace(text)) continue;

                var avgFont = segment.TextLines
                    .SelectMany(l => l.Words)
                    .SelectMany(w => w.Letters)
                    .Average(l => l.PointSize);

                var kind = IsLikelyHeading(text, avgFont, medianFont) ? "heading" : "paragraph";

                if (kind == "heading")
                {
                    currentSection = UpdatePdfSectionPath(currentSection, text);
                }

                blocks.Add(new ExtractedBlock(
                    PageNumber: page.Number,
                    Order: order++,
                    Kind: kind,
                    Text: text,
                    SectionPath: string.Join(" > ", currentSection),
                    FontSize: (float)avgFont
                ));
            }
        }

        return new StructuredDocument(documentId, fileName, "pdf", blocks);
    }

    private static double GetMedianFontSize(IEnumerable<Word> words)
    {
        var sizes = words.SelectMany(w => w.Letters).Select(l => l.PointSize).OrderBy(x => x).ToArray();
        if (sizes.Length == 0) return 10;
        return sizes[sizes.Length / 2];
    }

    private static bool IsLikelyHeading(string text, double avgFont, double medianFont)
    {
        var wordCount = text.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;

        return text.Length < 140
               && wordCount <= 16
               && !text.EndsWith(".")
               && avgFont >= medianFont * 1.15;
    }

    private static List<string> UpdatePdfSectionPath(List<string> current, string heading)
    {
        // Simple version; you can later infer heading level from font-size buckets.
        if (current.Count == 0) return new List<string> { heading };
        if (current.Count >= 3) current.RemoveAt(current.Count - 1);
        current.Add(heading);
        return current;
    }
}