namespace Aurelon.Models;

public enum JobStatus
{
    Pending = 1,
    Leased = 2,
    Processing = 3,
    RetryableFailed = 4,
    Completed = 5,
    Cancelled = 6,
    DeadLetter = 7,
}
