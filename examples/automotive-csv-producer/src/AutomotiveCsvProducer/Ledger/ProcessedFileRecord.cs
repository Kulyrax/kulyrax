namespace AutomotiveCsvProducer.Ledger;

public enum FileStatus
{
    InProgress,
    Succeeded,
    Failed,
}

public sealed record ProcessedFileRecord(
    string FileName,
    string ContentHash,
    FileStatus Status,
    string? FailureReason);

internal static class FileStatusExtensions
{
    public static string ToDbString(this FileStatus status) => status switch
    {
        FileStatus.InProgress => "in_progress",
        FileStatus.Succeeded => "succeeded",
        FileStatus.Failed => "failed",
        _ => throw new ArgumentOutOfRangeException(nameof(status)),
    };

    public static FileStatus ParseFileStatus(this string status) => status switch
    {
        "in_progress" => FileStatus.InProgress,
        "succeeded" => FileStatus.Succeeded,
        "failed" => FileStatus.Failed,
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown file status"),
    };
}
