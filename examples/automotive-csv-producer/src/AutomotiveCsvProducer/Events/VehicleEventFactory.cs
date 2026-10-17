using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AutomotiveCsvProducer.Csv;
using AutomotiveCsvProducer.Snapshot;

namespace AutomotiveCsvProducer.Events;

/// <summary>
/// Builds the persisted, submission-ready form of each <see cref="VehicleChange"/>.
/// Every payload carries the *full* current (or, for a sale, last-known)
/// vehicle record — producer-contract.md §6 ("fat events, not deltas":
/// Kulyrax has no read-modify-write path) — plus a smaller, kind-specific
/// field the task spec asks for on top: `previous_price` for a price change,
/// `changed_fields` for a non-price update.
/// </summary>
public static class VehicleEventFactory
{
    /// <summary>
    /// Build the <see cref="ProducerEvent"/>s for one <see cref="VehicleChange"/>,
    /// consuming (and returning, incremented) this vehicle's durable sequence
    /// counter. A combined price+other change yields two events, in the
    /// order the task spec requires (price change first), each with its own
    /// sequence value — never the same one twice.
    /// </summary>
    public static (IReadOnlyList<ProducerEvent> Events, long NextSequence) Build(
        VehicleChange change,
        string fileName,
        string snapshotContentHash,
        long currentSequence,
        int positionIndexStart,
        out int positionIndexEnd)
    {
        var events = new List<ProducerEvent>();
        var seq = currentSequence;
        var posIndex = positionIndexStart;

        switch (change.Kind)
        {
            case ChangeKind.Created:
                seq++;
                events.Add(BuildEvent(
                    VehicleEventType.Created,
                    change,
                    fileName,
                    snapshotContentHash,
                    seq,
                    posIndex++,
                    extra: null));
                break;

            case ChangeKind.PriceChanged:
                seq++;
                events.Add(BuildEvent(
                    VehicleEventType.PriceChanged,
                    change,
                    fileName,
                    snapshotContentHash,
                    seq,
                    posIndex++,
                    extra: new Dictionary<string, object?>
                    {
                        ["previous_price"] = change.Previous!.Price,
                    }));
                break;

            case ChangeKind.Updated:
                seq++;
                events.Add(BuildEvent(
                    VehicleEventType.Updated,
                    change,
                    fileName,
                    snapshotContentHash,
                    seq,
                    posIndex++,
                    extra: new Dictionary<string, object?>
                    {
                        ["changed_fields"] = ChangedNonPriceFields(change.Previous!, change.Current!),
                    }));
                break;

            case ChangeKind.Sold:
                seq++;
                events.Add(BuildEvent(
                    VehicleEventType.Sold,
                    change,
                    fileName,
                    snapshotContentHash,
                    seq,
                    posIndex++,
                    extra: null));
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(change));
        }

        positionIndexEnd = posIndex;
        return (events, seq);
    }

    /// <summary>
    /// A combined price + other-field change produces exactly these two
    /// events, price first — called once per change instead of routing both
    /// kinds through <see cref="Build"/> separately so the shared sequence
    /// counter and position index thread through correctly across both.
    /// </summary>
    public static (IReadOnlyList<ProducerEvent> Events, long NextSequence) BuildCombined(
        VehicleChange priceChange,
        VehicleChange updateChange,
        string fileName,
        string snapshotContentHash,
        long currentSequence,
        int positionIndexStart,
        out int positionIndexEnd)
    {
        var (priceEvents, afterPrice) = Build(
            priceChange, fileName, snapshotContentHash, currentSequence, positionIndexStart, out var posAfterPrice);
        var (updateEvents, afterUpdate) = Build(
            updateChange, fileName, snapshotContentHash, afterPrice, posAfterPrice, out positionIndexEnd);

        return (priceEvents.Concat(updateEvents).ToList(), afterUpdate);
    }

    private static Dictionary<string, object?> ChangedNonPriceFields(VehicleRecord prev, VehicleRecord curr)
    {
        var changed = new Dictionary<string, object?>();
        if (prev.Vin != curr.Vin) changed["vin"] = curr.Vin;
        if (prev.Make != curr.Make) changed["make"] = curr.Make;
        if (prev.Model != curr.Model) changed["model"] = curr.Model;
        if (prev.Year != curr.Year) changed["year"] = curr.Year;
        if (prev.Mileage != curr.Mileage) changed["mileage"] = curr.Mileage;
        if (prev.Condition != curr.Condition) changed["condition"] = curr.Condition;
        if (prev.Location != curr.Location) changed["location"] = curr.Location;
        if (prev.ListingStatus != curr.ListingStatus) changed["listing_status"] = curr.ListingStatus;
        return changed;
    }

    private static ProducerEvent BuildEvent(
        string eventType,
        VehicleChange change,
        string fileName,
        string snapshotContentHash,
        long sequence,
        int positionIndex,
        Dictionary<string, object?>? extra)
    {
        // Sold has no Current — the payload carries the last-known record
        // instead, flagged sold: true, so the full-record / fat-event
        // contract still holds for the one event kind without a "current".
        var record = change.Current ?? change.Previous!;
        var body = new Dictionary<string, object?>
        {
            ["provider_vehicle_id"] = change.ProviderVehicleId,
            ["vin"] = record.Vin,
            ["make"] = record.Make,
            ["model"] = record.Model,
            ["year"] = record.Year,
            ["price"] = record.Price,
            ["mileage"] = record.Mileage,
            ["condition"] = record.Condition,
            ["location"] = record.Location,
            ["listing_status"] = record.ListingStatus,
            ["sold"] = eventType == VehicleEventType.Sold,
        };
        if (extra is not null)
        {
            foreach (var (k, v) in extra)
            {
                body[k] = v;
            }
        }

        var payload = JsonSerializer.Serialize(body);
        var eventId = DeterministicEventId(snapshotContentHash, change.ProviderVehicleId, eventType, body);
        var position = $"{fileName}:{positionIndex:D6}";

        return new ProducerEvent(
            eventId,
            fileName,
            change.ProviderVehicleId,
            eventType,
            sequence,
            position,
            payload);
    }

    /// <summary>
    /// SHA-256 over the snapshot's content hash, the vehicle id, the event
    /// type, and the resolved payload (the "relevant change identity" the
    /// task spec asks for) — the same inputs reprocessing the same file
    /// would reproduce byte-for-byte, so a retry (or a resumed run after a
    /// restart) derives the identical id rather than minting a new one.
    /// </summary>
    private static string DeterministicEventId(
        string snapshotContentHash,
        string providerVehicleId,
        string eventType,
        Dictionary<string, object?> body)
    {
        // Keys sorted so JSON key order (which .NET does not guarantee
        // across runs for a Dictionary) can never change the hash.
        var canonical = string.Join(
            '|',
            body.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"{kv.Key}={Convert.ToString(kv.Value, CultureInfo.InvariantCulture)}"));
        var input = $"{snapshotContentHash}|{providerVehicleId}|{eventType}|{canonical}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
