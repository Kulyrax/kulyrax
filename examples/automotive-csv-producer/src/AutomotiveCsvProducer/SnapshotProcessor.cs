using System.Security.Cryptography;
using AutomotiveCsvProducer.Csv;
using AutomotiveCsvProducer.Events;
using AutomotiveCsvProducer.Grpc;
using AutomotiveCsvProducer.Ledger;
using AutomotiveCsvProducer.Snapshot;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AutomotiveCsvProducer;

/// <summary>
/// Orchestrates one tick of the pipeline: find the one file that should be
/// worked on next (resuming an in-progress one takes priority over starting
/// a new one), validate + diff + persist if it's new, then send every
/// unfinished event for it, sequentially, and commit the baseline only once
/// every one of them is confirmed.
/// </summary>
public sealed class SnapshotProcessor(
    ProducerLedger ledger,
    KulyraxIngestClient grpcClient,
    IOptions<ProducerOptions> options,
    ILogger<SnapshotProcessor> logger)
{
    private readonly ProducerOptions _options = options.Value;

    /// <returns>true if this tick did real work (processed, resumed, or rejected a file); false if there was nothing to do.</returns>
    public async Task<bool> TickAsync(CancellationToken ct)
    {
        var blocking = await ledger.GetBlockingFileAsync(ct);
        if (blocking is { Status: FileStatus.Failed })
        {
            logger.LogError(
                "pipeline is blocked by failed file {File}: {Reason}. No further files will be processed " +
                "until this is resolved (see README 'Recovering from a failed file').",
                blocking.FileName,
                blocking.FailureReason);
            return false;
        }

        string fileName;
        string filePath;
        if (blocking is { Status: FileStatus.InProgress })
        {
            fileName = blocking.FileName;
            filePath = Path.Combine(_options.InputDirectory, fileName);
            logger.LogInformation("resuming in-progress file {File}", fileName);
        }
        else
        {
            var next = FindNextFile();
            if (next is null)
            {
                return false;
            }
            filePath = next;
            fileName = Path.GetFileName(next);

            var handled = await StartNewFileAsync(fileName, filePath, ct);
            if (!handled)
            {
                // Validation or the diff safeguard rejected it — already
                // moved to failed/ and marked in the ledger. Nothing more
                // to do for this file; the caller's loop will pick up the
                // next one on its next tick.
                return true;
            }
        }

        return await SendPendingEventsAndCommitAsync(fileName, filePath, ct);
    }

    private string? FindNextFile()
    {
        if (!Directory.Exists(_options.InputDirectory))
        {
            return null;
        }
        return Directory.EnumerateFiles(_options.InputDirectory, "*.csv")
            .OrderBy(p => Path.GetFileName(p), StringComparer.Ordinal)
            .FirstOrDefault();
    }

    /// <returns>true if the file is now in_progress with events persisted and ready to send; false if it was rejected.</returns>
    private async Task<bool> StartNewFileAsync(string fileName, string filePath, CancellationToken ct)
    {
        var hash = await ComputeContentHashAsync(filePath, ct);
        var existing = await ledger.GetFileAsync(fileName, ct);

        if (existing is { Status: FileStatus.Succeeded })
        {
            // Already fully processed — most likely a crash between
            // committing the baseline and archiving the file. The ledger
            // is authoritative; just finish the move.
            logger.LogInformation("{File} was already processed successfully; archiving", fileName);
            ArchiveFile(filePath);
            return false;
        }

        if (existing is not null && existing.ContentHash != hash)
        {
            const string reason = "a file with this name was already processed with different content";
            logger.LogError("{File}: {Reason}", fileName, reason);
            await ledger.MarkFileFailedAsync(fileName, reason, ct);
            MoveToFailed(filePath);
            return false;
        }

        await ledger.BeginFileAsync(fileName, hash, ct);

        var lines = await File.ReadAllLinesAsync(filePath, ct);
        var validated = CsvValidator.Validate(lines);
        if (!validated.IsValid)
        {
            var reason = "csv validation failed: " + string.Join("; ", validated.Errors);
            logger.LogError("{File}: {Reason}", fileName, reason);
            await ledger.MarkFileFailedAsync(fileName, reason, ct);
            MoveToFailed(filePath);
            return false;
        }

        var baseline = await ledger.LoadBaselineAsync(ct);
        var current = validated.Records.ToDictionary(r => r.ProviderVehicleId, r => r);
        var diff = VehicleChangeDetector.Detect(baseline, current, new SnapshotDiffOptions(_options.MaxDropFraction));
        if (!diff.IsAccepted)
        {
            logger.LogError("{File}: {Reason}", fileName, diff.RejectionReason);
            await ledger.MarkFileFailedAsync(fileName, diff.RejectionReason!, ct);
            MoveToFailed(filePath);
            return false;
        }

        await PersistChangesAsync(fileName, hash, diff.Changes, ct);
        logger.LogInformation(
            "{File}: {Count} change(s) detected and persisted", fileName, diff.Changes.Count);
        return true;
    }

    private async Task PersistChangesAsync(
        string fileName,
        string contentHash,
        IReadOnlyList<VehicleChange> changes,
        CancellationToken ct)
    {
        var allEvents = new List<ProducerEvent>();
        var newSequences = new Dictionary<string, long>();
        var positionIndex = 0;

        // Group so a vehicle with both a price and a non-price change gets
        // its pair built together, sharing one sequence/position thread.
        var byVehicle = changes.GroupBy(c => c.ProviderVehicleId);
        foreach (var group in byVehicle)
        {
            var list = group.ToList();
            var currentSeq = await ledger.GetVehicleSequenceAsync(group.Key, ct);

            var priceChange = list.FirstOrDefault(c => c.Kind == ChangeKind.PriceChanged);
            var updateChange = list.FirstOrDefault(c => c.Kind == ChangeKind.Updated);

            IReadOnlyList<ProducerEvent> built;
            long nextSeq;
            if (priceChange is not null && updateChange is not null)
            {
                (built, nextSeq) = VehicleEventFactory.BuildCombined(
                    priceChange, updateChange, fileName, contentHash, currentSeq, positionIndex, out positionIndex);
            }
            else
            {
                var only = list.Single();
                (built, nextSeq) = VehicleEventFactory.Build(
                    only, fileName, contentHash, currentSeq, positionIndex, out positionIndex);
            }

            allEvents.AddRange(built);
            newSequences[group.Key] = nextSeq;
        }

        await ledger.PersistEventsAsync(allEvents, newSequences, ct);
    }

    private async Task<bool> SendPendingEventsAndCommitAsync(string fileName, string filePath, CancellationToken ct)
    {
        var retryOptions = new RetryOptions(
            _options.RetryMaxAttempts, _options.RetryBaseDelay, _options.RetryMaxDelay);

        var rows = await ledger.GetEventsForFileAsync(fileName, ct);
        foreach (var row in rows.Where(r => r.Status != EventStatus.Acked))
        {
            var result = await grpcClient.SendAndConfirmAsync(row.Event, retryOptions, ct);
            switch (result.Outcome)
            {
                case SendOutcome.Confirmed:
                    await ledger.MarkEventAckedAsync(row.Event.EventId, ct);
                    logger.LogInformation(
                        "event {EventId} ({Type}, {Vehicle}) confirmed",
                        row.Event.EventId,
                        row.Event.EventType,
                        row.Event.ProviderVehicleId);
                    break;

                case SendOutcome.DeadLettered:
                    logger.LogError(
                        "event {EventId} ({Type}, {Vehicle}) was dead-lettered: {Detail}",
                        row.Event.EventId,
                        row.Event.EventType,
                        row.Event.ProviderVehicleId,
                        result.Detail);
                    await ledger.MarkEventFailedAsync(row.Event.EventId, result.Detail ?? "dead-lettered", ct);
                    await ledger.MarkFileFailedAsync(
                        fileName,
                        $"event {row.Event.EventId} was dead-lettered: {result.Detail}",
                        ct);
                    // Deliberately not moved to failed/ — this is a
                    // permanent payload rejection mid-run, not a validation
                    // rejection, and the already-sent, already-acked events
                    // in this same file are real, applied changes. Moving
                    // the file would make it look like none of it happened.
                    // No further progress is possible until an operator
                    // resolves it, so tell the caller to back off rather
                    // than spin retrying the same dead letter immediately.
                    return false;

                case SendOutcome.RetryExhausted:
                    logger.LogError(
                        "event {EventId} ({Type}, {Vehicle}) exhausted its retry budget: {Detail}. " +
                        "Leaving {File} in_progress for a later tick to resume.",
                        row.Event.EventId,
                        row.Event.EventType,
                        row.Event.ProviderVehicleId,
                        result.Detail,
                        fileName);
                    // Transient (unlike DeadLettered) — worth trying again,
                    // but not in a tight loop; let the normal poll delay apply.
                    return false;
            }
        }

        await ledger.CommitBaselineAsync(fileName, ct);
        ArchiveFile(filePath);
        logger.LogInformation("{File}: all events confirmed, baseline committed, archived", fileName);
        return true;
    }

    private void ArchiveFile(string filePath)
    {
        MoveTo(filePath, _options.ArchiveDirectory);
    }

    private void MoveToFailed(string filePath)
    {
        MoveTo(filePath, _options.FailedDirectory);
    }

    private static void MoveTo(string filePath, string targetDir)
    {
        if (!File.Exists(filePath))
        {
            return; // already moved on a prior attempt
        }
        Directory.CreateDirectory(targetDir);
        var dest = Path.Combine(targetDir, Path.GetFileName(filePath));
        File.Move(filePath, dest, overwrite: true);
    }

    private static async Task<string> ComputeContentHashAsync(string filePath, CancellationToken ct)
    {
        await using var stream = File.OpenRead(filePath);
        var hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
