using System.Text.RegularExpressions;
using Aurelon.Models;

namespace Aurelon.Services;

public sealed class MarkdownExtractor
{
    public StructuredDocument Extract(string documentId, string fileName, string path)
    {
        var content = File.ReadAllText(path);
        var lines = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        
        var blocks = new List<ExtractedBlock>();
        var sectionStack = new List<string>();
        var order = 0;

        foreach (var line in lines)
        {
            var trimmedLine = line.Trim();
            if (string.IsNullOrWhiteSpace(trimmedLine)) continue;

            var headerMatch = Regex.Match(trimmedLine, @"^(#{1,6})\s+(.*)$");
            if (headerMatch.Success)
            {
                var level = headerMatch.Groups[1].Value.Length;
                var text = headerMatch.Groups[2].Value.Trim();
                
                UpdateSectionStack(sectionStack, level, text);
                
                blocks.Add(new ExtractedBlock(
                    PageNumber: 0,
                    Order: order++,
                    Kind: "heading",
                    Text: text,
                    SectionPath: string.Join(" > ", sectionStack)
                ));
            }
            else
            {
                blocks.Add(new ExtractedBlock(
                    PageNumber: 0,
                    Order: order++,
                    Kind: "paragraph",
                    Text: trimmedLine,
                    SectionPath: string.Join(" > ", sectionStack)
                ));
            }
        }

        return new StructuredDocument(documentId, fileName, "md", blocks);
    }

    private static void UpdateSectionStack(List<string> stack, int level, string heading)
    {
        while (stack.Count >= level) stack.RemoveAt(stack.Count - 1);
        stack.Add(heading);
    }
}
