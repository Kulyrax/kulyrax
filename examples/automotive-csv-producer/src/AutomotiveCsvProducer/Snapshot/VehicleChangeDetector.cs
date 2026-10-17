using AutomotiveCsvProducer.Csv;

namespace AutomotiveCsvProducer.Snapshot;

/// <summary>
/// Compares the committed baseline snapshot against a freshly validated one
/// and produces the minimal set of changes per §5 of the task spec:
/// <list type="bullet">
/// <item>first appearance / new vehicle / reappearance → <see cref="ChangeKind.Created"/></item>
/// <item>price only → <see cref="ChangeKind.PriceChanged"/></item>
/// <item>price + other fields → <see cref="ChangeKind.PriceChanged"/> plus a <see cref="ChangeKind.Updated"/></item>
/// <item>other fields only → <see cref="ChangeKind.Updated"/></item>
/// <item>disappearance from a validated full snapshot → <see cref="ChangeKind.Sold"/></item>
/// </list>
/// Only ever compares the last committed baseline to the current snapshot —
/// never an intermediate one — so an intermediate value the producer never
/// committed to never surfaces as a change; this is automatic from the
/// inputs, not a special case.
/// </summary>
public static class VehicleChangeDetector
{
    public static SnapshotDiffResult Detect(
        IReadOnlyDictionary<string, VehicleRecord> previous,
        IReadOnlyDictionary<string, VehicleRecord> current,
        SnapshotDiffOptions options)
    {
        var missing = previous.Keys.Where(id => !current.ContainsKey(id)).ToList();
        if (previous.Count > 0 && missing.Count > 0)
        {
            var dropFraction = (double)missing.Count / previous.Count;
            if (dropFraction > options.MaxDropFraction)
            {
                return SnapshotDiffResult.Rejected(
                    $"{missing.Count} of {previous.Count} vehicles ({dropFraction:P0}) are missing from " +
                    $"this snapshot, exceeding the {options.MaxDropFraction:P0} drop safeguard — treating " +
                    "this as a possibly truncated or incomplete feed, not a mass sale. No events were " +
                    "generated and the baseline was not advanced.");
            }
        }

        var changes = new List<VehicleChange>();

        foreach (var (id, curr) in current)
        {
            if (!previous.TryGetValue(id, out var prev))
            {
                changes.Add(new VehicleChange(ChangeKind.Created, id, null, curr));
                continue;
            }

            if (prev.HasSameBusinessFieldsAs(curr))
            {
                continue;
            }

            var priceChanged = prev.Price != curr.Price;
            var otherChanged = !prev.HasSameNonPriceFieldsAs(curr);

            if (priceChanged)
            {
                changes.Add(new VehicleChange(ChangeKind.PriceChanged, id, prev, curr));
            }
            if (otherChanged)
            {
                changes.Add(new VehicleChange(ChangeKind.Updated, id, prev, curr));
            }
        }

        foreach (var id in missing)
        {
            changes.Add(new VehicleChange(ChangeKind.Sold, id, previous[id], null));
        }

        return SnapshotDiffResult.Accepted(changes);
    }
}
