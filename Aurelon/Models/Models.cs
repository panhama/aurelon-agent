namespace Aurelon.Services;
public sealed record ExtractedBlock(
    int PageNumber,
    int Order,
    string Kind,           // heading | paragraph | table | list_item | footer | header
    string Text,
    string? SectionPath,
    string? StyleName = null,
    float? FontSize = null,
    string? BoundingBox = null // optional: x1,y1,x2,y2 serialized
);

public sealed record StructuredDocument(
    string DocumentId,
    string FileName,
    string DocumentType,   // pdf | docx
    IReadOnlyList<ExtractedBlock> Blocks
);