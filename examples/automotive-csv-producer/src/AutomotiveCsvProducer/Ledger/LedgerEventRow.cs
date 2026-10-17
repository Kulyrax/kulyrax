using AutomotiveCsvProducer.Events;

namespace AutomotiveCsvProducer.Ledger;

public enum EventStatus
{
    Pending,
    Acked,
    Failed,
}

internal static class EventStatusExtensions
{
    public static string ToDbString(this EventStatus status) => status switch
    {
        EventStatus.Pending => "pending",
        EventStatus.Acked => "acked",
        EventStatus.Failed => "failed",
        _ => throw new ArgumentOutOfRangeException(nameof(status)),
    };

    public static EventStatus ParseEventStatus(this string status) => status switch
    {
        "pending" => EventStatus.Pending,
        "acked" => EventStatus.Acked,
        "failed" => EventStatus.Failed,
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown event status"),
    };
}

public sealed record LedgerEventRow(ProducerEvent Event, EventStatus Status);
