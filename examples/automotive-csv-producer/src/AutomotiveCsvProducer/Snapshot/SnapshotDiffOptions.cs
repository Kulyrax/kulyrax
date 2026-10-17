namespace AutomotiveCsvProducer.Snapshot;

/// <summary>
/// The configurable safeguard against treating a truncated feed as a mass
/// sale. If more than <see cref="MaxDropFraction"/> of the previous
/// snapshot's vehicles are missing from the current one, the drop is
/// treated as suspicious rather than real.
/// </summary>
public sealed record SnapshotDiffOptions(double MaxDropFraction = 0.5);
