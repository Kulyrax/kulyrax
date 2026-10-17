using System.Reflection;
using AutomotiveCsvProducer.Csv;
using AutomotiveCsvProducer.Events;
using Npgsql;

namespace AutomotiveCsvProducer.Ledger;

/// <summary>
/// The producer's own durable state in Postgres — never confused with
/// Kulyrax's projection tables (a different database entirely in the
/// Docker Compose setup). Every write that must be consistent with another
/// (persisting events, committing the baseline) runs in one transaction,
/// per the task's explicit "use transactions for consistent producer-state
/// updates" requirement.
/// </summary>
public sealed class ProducerLedger(NpgsqlDataSource dataSource)
{
    public async Task InitializeSchemaAsync(CancellationToken ct)
    {
        var assembly = Assembly.GetExecutingAssembly();
        using var stream = assembly.GetManifestResourceStream(
            "AutomotiveCsvProducer.Ledger.LedgerSchema.sql")
            ?? throw new InvalidOperationException("embedded LedgerSchema.sql resource not found");
        using var reader = new StreamReader(stream);
        var sql = await reader.ReadToEndAsync(ct);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<ProcessedFileRecord?> GetFileAsync(string fileName, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT file_name, content_hash, status, failure_reason FROM processed_files WHERE file_name = $1",
            conn)
        { Parameters = { new() { Value = fileName } } };
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }
        return new ProcessedFileRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2).ParseFileStatus(),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    /// <summary>
    /// The one file still 'in_progress' or 'failed', if any — the task's
    /// "resume an incomplete file rather than starting another snapshot
    /// concurrently" and "process newer files only after the active
    /// snapshot has completed successfully" rules, in one query. By
    /// construction at most one row is ever in either state at a time: the
    /// worker never starts a new file while one is outstanding.
    /// </summary>
    public async Task<ProcessedFileRecord?> GetBlockingFileAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT file_name, content_hash, status, failure_reason FROM processed_files " +
            "WHERE status IN ('in_progress', 'failed') ORDER BY started_at LIMIT 1",
            conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }
        return new ProcessedFileRecord(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2).ParseFileStatus(),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    public async Task BeginFileAsync(string fileName, string contentHash, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO processed_files (file_name, content_hash, status) VALUES ($1, $2, 'in_progress') " +
            "ON CONFLICT (file_name) DO NOTHING",
            conn)
        {
            Parameters = { new() { Value = fileName }, new() { Value = contentHash } },
        };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task MarkFileFailedAsync(string fileName, string reason, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE processed_files SET status = 'failed', failure_reason = $2, finished_at = now() " +
            "WHERE file_name = $1",
            conn)
        {
            Parameters = { new() { Value = fileName }, new() { Value = reason } },
        };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyDictionary<string, VehicleRecord>> LoadBaselineAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT provider_vehicle_id, vin, make, model, year, price, mileage, condition, location, listing_status " +
            "FROM vehicle_baseline",
            conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var result = new Dictionary<string, VehicleRecord>();
        while (await reader.ReadAsync(ct))
        {
            var id = reader.GetString(0);
            result[id] = new VehicleRecord(
                id,
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt32(4),
                reader.GetDecimal(5),
                reader.GetInt32(6),
                reader.GetString(7),
                reader.GetString(8),
                reader.GetString(9));
        }
        return result;
    }

    public async Task<long> GetVehicleSequenceAsync(string providerVehicleId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT last_sequence FROM vehicle_sequence WHERE provider_vehicle_id = $1",
            conn)
        { Parameters = { new() { Value = providerVehicleId } } };
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is null ? 0L : (long)result;
    }

    /// <summary>
    /// Persist this run's generated events and the vehicle sequence
    /// counters they consumed, in one transaction. Idempotent: re-running
    /// this for the same file (e.g. a crash right after this call but
    /// before the status row reflects it) inserts nothing twice
    /// (<c>ON CONFLICT DO NOTHING</c> on the event's deterministic id) and
    /// re-sets each sequence counter to the same value it already holds.
    /// </summary>
    public async Task PersistEventsAsync(
        IReadOnlyList<ProducerEvent> events,
        IReadOnlyDictionary<string, long> newSequenceByVehicle,
        CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        foreach (var e in events)
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO producer_events (event_id, file_name, provider_vehicle_id, event_type, sequence, position, payload) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (event_id) DO NOTHING",
                conn,
                (NpgsqlTransaction)tx)
            {
                Parameters =
                {
                    new() { Value = e.EventId },
                    new() { Value = e.FileName },
                    new() { Value = e.ProviderVehicleId },
                    new() { Value = e.EventType },
                    new() { Value = e.Sequence },
                    new() { Value = e.Position },
                    new() { Value = e.Payload },
                },
            };
            await cmd.ExecuteNonQueryAsync(ct);
        }

        foreach (var (vehicleId, seq) in newSequenceByVehicle)
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO vehicle_sequence (provider_vehicle_id, last_sequence) VALUES ($1, $2) " +
                "ON CONFLICT (provider_vehicle_id) DO UPDATE SET last_sequence = EXCLUDED.last_sequence",
                conn,
                (NpgsqlTransaction)tx)
            {
                Parameters = { new() { Value = vehicleId }, new() { Value = seq } },
            };
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public async Task<IReadOnlyList<LedgerEventRow>> GetEventsForFileAsync(string fileName, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT event_id, file_name, provider_vehicle_id, event_type, sequence, position, payload, status " +
            "FROM producer_events WHERE file_name = $1 ORDER BY position",
            conn)
        { Parameters = { new() { Value = fileName } } };
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var result = new List<LedgerEventRow>();
        while (await reader.ReadAsync(ct))
        {
            var ev = new ProducerEvent(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt64(4),
                reader.GetString(5),
                reader.GetString(6));
            result.Add(new LedgerEventRow(ev, reader.GetString(7).ParseEventStatus()));
        }
        return result;
    }

    public async Task MarkEventAckedAsync(string eventId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE producer_events SET status = 'acked', updated_at = now() WHERE event_id = $1",
            conn)
        { Parameters = { new() { Value = eventId } } };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task MarkEventFailedAsync(string eventId, string reason, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE producer_events SET status = 'failed', failure_reason = $2, updated_at = now() WHERE event_id = $1",
            conn)
        { Parameters = { new() { Value = eventId }, new() { Value = reason } } };
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Apply every change this run produced to the baseline and mark the
    /// file succeeded, in one transaction — the task's "commit the new
    /// inventory baseline only after every event in the snapshot succeeds"
    /// rule. Only ever called after every one of this file's events is
    /// confirmed <see cref="EventStatus.Acked"/>.
    ///
    /// Deliberately derives the baseline action for each vehicle from its
    /// own already-persisted <c>producer_events</c> rows (every payload
    /// carries the full current record — the fat-event design in
    /// <see cref="Events.VehicleEventFactory"/> — so the row itself is
    /// enough) rather than taking a fresh <c>VehicleChange</c> list as a
    /// parameter. That list would only exist in memory on the instance that
    /// built it; a crash between persisting events and committing the
    /// baseline must be resumable from the ledger alone, with no in-memory
    /// state assumed to have survived.
    /// </summary>
    public async Task CommitBaselineAsync(string fileName, CancellationToken ct)
    {
        var events = await GetEventsForFileAsync(fileName, ct);
        // One decision per vehicle: if any of its events this run says
        // sold, the vehicle is sold (Sold only ever fires for a vehicle
        // absent from the current snapshot, so it can never coexist with a
        // Created/PriceChanged/Updated for the same vehicle in one run).
        // Otherwise every event for that vehicle carries the same current
        // record, so the latest by sequence is as good as any.
        var byVehicle = events
            .GroupBy(r => r.Event.ProviderVehicleId)
            .Select(g => g.OrderByDescending(r => r.Event.Sequence).First())
            .ToList();

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        foreach (var row in byVehicle)
        {
            using var doc = System.Text.Json.JsonDocument.Parse(row.Event.Payload);
            var root = doc.RootElement;
            var sold = root.TryGetProperty("sold", out var soldEl) && soldEl.GetBoolean();

            if (sold)
            {
                await using var cmd = new NpgsqlCommand(
                    "DELETE FROM vehicle_baseline WHERE provider_vehicle_id = $1", conn, (NpgsqlTransaction)tx)
                { Parameters = { new() { Value = row.Event.ProviderVehicleId } } };
                await cmd.ExecuteNonQueryAsync(ct);
                continue;
            }

            await using var upsert = new NpgsqlCommand(
                "INSERT INTO vehicle_baseline " +
                "(provider_vehicle_id, vin, make, model, year, price, mileage, condition, location, listing_status, updated_at) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now()) " +
                "ON CONFLICT (provider_vehicle_id) DO UPDATE SET " +
                "vin = EXCLUDED.vin, make = EXCLUDED.make, model = EXCLUDED.model, year = EXCLUDED.year, " +
                "price = EXCLUDED.price, mileage = EXCLUDED.mileage, condition = EXCLUDED.condition, " +
                "location = EXCLUDED.location, listing_status = EXCLUDED.listing_status, updated_at = now()",
                conn,
                (NpgsqlTransaction)tx)
            {
                Parameters =
                {
                    new() { Value = row.Event.ProviderVehicleId },
                    new() { Value = root.GetProperty("vin").GetString() },
                    new() { Value = root.GetProperty("make").GetString() },
                    new() { Value = root.GetProperty("model").GetString() },
                    new() { Value = root.GetProperty("year").GetInt32() },
                    new() { Value = root.GetProperty("price").GetDecimal() },
                    new() { Value = root.GetProperty("mileage").GetInt32() },
                    new() { Value = root.GetProperty("condition").GetString() },
                    new() { Value = root.GetProperty("location").GetString() },
                    new() { Value = root.GetProperty("listing_status").GetString() },
                },
            };
            await upsert.ExecuteNonQueryAsync(ct);
        }

        await using var markSucceeded = new NpgsqlCommand(
            "UPDATE processed_files SET status = 'succeeded', finished_at = now() WHERE file_name = $1",
            conn,
            (NpgsqlTransaction)tx)
        { Parameters = { new() { Value = fileName } } };
        await markSucceeded.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
    }
}
