namespace AutomotiveCsvProducer.Grpc;

public enum SendOutcome
{
    /// <summary>Acked with <c>dlq_events == 0</c> — verified projected.</summary>
    Confirmed,

    /// <summary>
    /// Acked, but <c>dlq_events &gt; 0</c> — Kulyrax dead-lettered the event
    /// after exhausting its own retry budget. A permanent payload rejection
    /// from the producer's point of view: resending the identical payload
    /// would not help.
    /// </summary>
    DeadLettered,

    /// <summary>The retry budget was exhausted without ever getting an ack.</summary>
    RetryExhausted,
}

public sealed record SendResult(SendOutcome Outcome, string? Detail = null);
