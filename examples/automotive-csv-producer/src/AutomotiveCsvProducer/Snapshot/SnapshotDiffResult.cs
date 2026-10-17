namespace AutomotiveCsvProducer.Snapshot;

/// <summary>
/// The result of comparing two snapshots. Rejected rather than Accepted when
/// the drop in vehicle count looks like a truncated or otherwise incomplete
/// feed rather than a real mass sale — in that case no changes are returned
/// at all (not even the non-Sold ones), so the caller never generates a
/// single event or advances the baseline off a snapshot it doesn't trust.
/// </summary>
public sealed record SnapshotDiffResult(
    bool IsAccepted,
    IReadOnlyList<VehicleChange> Changes,
    string? RejectionReason)
{
    public static SnapshotDiffResult Accepted(IReadOnlyList<VehicleChange> changes) =>
        new(true, changes, null);

    public static SnapshotDiffResult Rejected(string reason) =>
        new(false, Array.Empty<VehicleChange>(), reason);
}
