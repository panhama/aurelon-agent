using Aurelon.Models;

namespace Aurelon.Services;

public sealed class DocumentExtractionService(
    PdfExtractor pdfExtractor,
    DocxExtractor docxExtractor,
    MarkdownExtractor markdownExtractor,
    ExcelExtractor excelExtractor)
{
    public async Task<StructuredDocument> ExtractAsync(Stream stream, string fileName, CancellationToken ct)
    {
        var extension = Path.GetExtension(fileName).ToLowerInvariant();
        var documentId = Guid.NewGuid().ToString();
        
        var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}{extension}");
        try
        {
            await using (var fileStream = new FileStream(tempPath, FileMode.Create))
            {
                await stream.CopyToAsync(fileStream, ct);
            }

            return extension switch
            {
                ".pdf" => pdfExtractor.Extract(documentId, fileName, tempPath),
                ".docx" => docxExtractor.Extract(documentId, fileName, tempPath),
                ".md" => markdownExtractor.Extract(documentId, fileName, tempPath),
                ".xlsx" => excelExtractor.Extract(documentId, fileName, tempPath),
                _ => throw new NotSupportedException($"Unsupported file type: {extension}")
            };
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }
}
