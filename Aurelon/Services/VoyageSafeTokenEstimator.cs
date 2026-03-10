namespace Aurelon.Services;

// Temporary safe estimator for Voyage until you add exact model tokenizer support.
public sealed class VoyageSafeTokenEstimator : ITokenEstimator
{
    private readonly Microsoft.ML.Tokenizers.TiktokenTokenizer _fallback =
        Microsoft.ML.Tokenizers.TiktokenTokenizer.CreateForModel("text-embedding-3-small");

    public int Count(string text)
    {
        var openAiCount = _fallback.CountTokens(text);
        return (int)Math.Ceiling(openAiCount * 1.2); // safe-side estimate
    }
}