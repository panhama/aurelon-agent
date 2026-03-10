namespace Aurelon.Services;
using System.Text.RegularExpressions;

public static class RetrievalTextNormalizer
{
    public static string ToSearchText(ExtractedBlock block)
    {
        var text = block.Text.Replace("\r\n", "\n").Replace('\u00A0', ' ');
        text = Regex.Replace(text, @"[ \t]+", " ");
        text = Regex.Replace(text, @"\n{3,}", "\n\n");
        text = text.Trim();

        return block.Kind switch
        {
            "heading"   => $"# {text}",
            "table"     => $"Table\n{text}",
            "list_item" => $"- {text}",
            _           => text
        };
    }

    public static string ToEmbeddingText(ExtractedBlock block)
    {
        var core = ToSearchText(block);

        if (string.IsNullOrWhiteSpace(block.SectionPath))
            return core;

        return $"Section: {block.SectionPath}\n\n{core}";
    }
}