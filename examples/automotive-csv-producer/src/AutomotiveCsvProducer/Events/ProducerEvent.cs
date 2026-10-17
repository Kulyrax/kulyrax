namespace AutomotiveCsvProducer.Events;

/// <summary>
/// A fully-built, persisted event — everything <c>EventEnvelope</c> needs,
/// plus the ledger bookkeeping fields. Built once, persisted before the
/// first send attempt, and never rebuilt: a retry reuses exactly this
/// <see cref="EventId"/> and <see cref="Payload"/>, per the task's "persist
/// before submitting, retries reuse the same persisted id and payload" rule.
/// </summary>
public sealed record ProducerEvent(
    string EventId,
    string FileName,
    string ProviderVehicleId,
    string EventType,
    long Sequence,
    string Position,
    string Payload);
