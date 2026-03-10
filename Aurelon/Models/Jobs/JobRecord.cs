
namespace Aurelon.Models;

public abstract class JobRecord
{
    public Guid Id { get; init; } = Guid.CreateVersion7();
    public string UserId { get; init; } = string.Empty;
    public JobStatus Status { get; init; } = JobStatus.Pending;
    public int AttemptCount { get; init; }
    public int MaxAttempts { get; init; } = 5;
    public DateTimeOffset CreatedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? StartedAtUtc { get; init; }
    public DateTimeOffset? CompletedAtUtc { get; init; }
    public DateTimeOffset? LeasedUntilUtc { get; init; }
    public DateTimeOffset? HeartbeatAtUtc { get; init; }
    public bool CancelRequested { get; init; }
    public string? LastError { get; init; }
}
