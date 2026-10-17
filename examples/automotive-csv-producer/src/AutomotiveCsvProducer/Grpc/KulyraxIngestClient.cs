using AutomotiveCsvProducer.Events;
using AutomotiveCsvProducer.Grpc.Kulyrax;
using global::Grpc.Core;
using global::Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace AutomotiveCsvProducer.Grpc;

/// <summary>
/// Thrown when the server's first <c>Ack</c> reports
/// <c>AckProtocolVersion == 0</c> — the producer cannot establish that this
/// server populates <c>dlq_events</c> accurately, so it must not treat any
/// event as confirmed. This stops the producer rather than silently
/// proceeding as if every send were a success (see docs/concepts.md's
/// "Ack — what it proves, and what it doesn't" in the main repo).
/// </summary>
public sealed class IncompatibleServerException : Exception
{
    public IncompatibleServerException(string message) : base(message)
    {
    }
}

/// <summary>
/// One producer session over <c>Ingest.Stream</c>, sending events strictly
/// sequentially (send, wait for its own ack, only then send the next). This
/// is not just a simplicity choice: with at most one event ever in flight,
/// the single batch Kulyrax forms around it is a batch of one, so that
/// batch's <c>dlq_events</c> (0 or 1) unambiguously describes *this* event.
/// Send two events before confirming the first and a batch could cover
/// both, at which point a nonzero <c>dlq_events</c> no longer tells you
/// which one failed — the sequential contract here is what makes
/// <see cref="SendOutcome.DeadLettered"/> attributable to a specific event
/// at all, not merely a style preference.
/// </summary>
public sealed class KulyraxIngestClient : IAsyncDisposable
{
    private readonly ILogger<KulyraxIngestClient> _logger;
    private readonly string _endpoint;
    private GrpcChannel? _channel;
    private AsyncDuplexStreamingCall<EventEnvelope, Ack>? _call;

    /// <summary>
    /// Set once the server's compatibility with <c>dlq_events</c> accounting
    /// has been confirmed via a real <see cref="Ack"/>. Checked exactly once
    /// per process — a server doesn't change its protocol version mid-run.
    /// </summary>
    private bool _serverCompatibilityConfirmed;

    public KulyraxIngestClient(string endpoint, ILogger<KulyraxIngestClient> logger)
    {
        _endpoint = endpoint;
        _logger = logger;
    }

    private async Task<AsyncDuplexStreamingCall<EventEnvelope, Ack>> EnsureStreamAsync(CancellationToken ct)
    {
        if (_call is not null)
        {
            return _call;
        }

        _channel = GrpcChannel.ForAddress(_endpoint);
        var client = new Ingest.IngestClient(_channel);
        _call = client.Stream(cancellationToken: ct);
        return _call;
    }

    private async Task ResetStreamAsync()
    {
        try
        {
            _call?.Dispose();
        }
        catch
        {
            // best-effort — the call is already unusable either way
        }
        _call = null;

        if (_channel is not null)
        {
            await _channel.ShutdownAsync();
            _channel.Dispose();
            _channel = null;
        }
    }

    /// <summary>
    /// Send one event and wait for the ack that covers its own position,
    /// retrying transient failures with bounded exponential backoff and
    /// jitter. A resend after any failure (network error, timeout, or an
    /// ambiguous "might have already landed") is always safe to issue
    /// blindly here: Kulyrax gates every write on this entity's sequence
    /// (producer-contract.md §1), so a duplicate delivery of the identical
    /// event is recognized and silently skipped rather than double-applied
    /// — the resend is what actually gets us a trustworthy ack for this
    /// attempt, not a correctness risk.
    /// </summary>
    public async Task<SendResult> SendAndConfirmAsync(
        ProducerEvent ev,
        RetryOptions options,
        CancellationToken ct)
    {
        var attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                var call = await EnsureStreamAsync(ct);
                var envelope = new EventEnvelope
                {
                    EventType = ev.EventType,
                    Payload = ev.Payload,
                    Version = 1,
                    Sequence = (ulong)ev.Sequence,
                    Position = ev.Position,
                    EventId = ev.EventId,
                };
                await call.RequestStream.WriteAsync(envelope, ct);

                while (await call.ResponseStream.MoveNext(ct))
                {
                    var ack = call.ResponseStream.Current;
                    if (ack.Position != ev.Position)
                    {
                        // Another position (e.g. a stale ack still in
                        // flight from before a prior reconnect) — not ours,
                        // keep reading for the one that is.
                        continue;
                    }

                    if (!_serverCompatibilityConfirmed)
                    {
                        if (ack.AckProtocolVersion == 0)
                        {
                            // Zero is what every pre-dlq_events server sends by
                            // default — the producer has NOT established that
                            // dlq_events means anything on this connection,
                            // including when it reads 0. Fail loudly instead
                            // of silently treating this (or any later) event
                            // as confirmed.
                            throw new IncompatibleServerException(
                                "kulyrax server did not report ack_protocol_version >= 1 " +
                                "on its first Ack; dlq_events cannot be trusted on this " +
                                "connection, so no event can be confirmed successful. " +
                                "Upgrade the kulyrax-ingest server.");
                        }
                        _serverCompatibilityConfirmed = true;
                    }

                    return ack.DlqEvents > 0
                        ? new SendResult(
                            SendOutcome.DeadLettered,
                            $"kulyrax dead-lettered this event (dlq_events={ack.DlqEvents} for position {ack.Position})")
                        : new SendResult(SendOutcome.Confirmed);
                }

                // Response stream ended without the ack we were waiting
                // for — the server side closed the session. Treat like any
                // other transient failure and retry on a fresh stream.
                throw new RpcException(new Status(StatusCode.Unavailable, "ack stream ended unexpectedly"));
            }
            catch (Exception ex) when (ex is RpcException or IOException)
            {
                _logger.LogWarning(
                    ex,
                    "send attempt {Attempt}/{Max} failed for event {EventId} (position {Position})",
                    attempt,
                    options.MaxAttempts,
                    ev.EventId,
                    ev.Position);
                await ResetStreamAsync();

                if (attempt >= options.MaxAttempts)
                {
                    return new SendResult(
                        SendOutcome.RetryExhausted,
                        $"exhausted {options.MaxAttempts} attempts: {ex.Message}");
                }

                await DelayWithJitterAsync(attempt, options, ct);
            }
        }
    }

    private static async Task DelayWithJitterAsync(int attempt, RetryOptions options, CancellationToken ct)
    {
        var exponential = options.BaseDelay * Math.Pow(2, attempt - 1);
        var capped = exponential > options.MaxDelay ? options.MaxDelay : exponential;
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * capped.TotalMilliseconds * 0.25);
        await Task.Delay(capped + jitter, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await ResetStreamAsync();
    }
}
