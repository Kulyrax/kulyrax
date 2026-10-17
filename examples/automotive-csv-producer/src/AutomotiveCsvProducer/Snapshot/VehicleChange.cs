using AutomotiveCsvProducer.Csv;

namespace AutomotiveCsvProducer.Snapshot;

/// <summary>
/// One detected change between two snapshots. <see cref="Previous"/> is
/// null only for <see cref="ChangeKind.Created"/>; <see cref="Current"/> is
/// null only for <see cref="ChangeKind.Sold"/> — every other kind carries both,
/// since a price or field change needs the old value to describe the delta.
/// </summary>
public sealed record VehicleChange(
    ChangeKind Kind,
    string ProviderVehicleId,
    VehicleRecord? Previous,
    VehicleRecord? Current);
