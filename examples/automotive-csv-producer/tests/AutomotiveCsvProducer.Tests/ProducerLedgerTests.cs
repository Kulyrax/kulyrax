using AutomotiveCsvProducer.Events;
using AutomotiveCsvProducer.Ledger;
using Npgsql;
using Xunit;

namespace AutomotiveCsvProducer.Tests;

/// <summary>
/// Gated on <c>AUTOMOTIVE_LEDGER_URL</c> (a Postgres connection string). When
/// unset, every test prints a skip line and returns — mirroring the
/// skip-if-unset convention the Rust side of this repo uses throughout
/// (<c>crates/adapters/*/tests/*_backend.rs</c>), rather than failing CI
/// over a database it never promised to provide.
/// </summary>
public class ProducerLedgerTests : IAsyncLifetime
{
    private NpgsqlDataSource? _dataSource;
    private ProducerLedger? _ledger;

    private static string? LedgerUrl() =>
        Environment.GetEnvironmentVariable("AUTOMOTIVE_LEDGER_URL") is { Length: > 0 } v ? v : null;

    public async Task InitializeAsync()
    {
        var url = LedgerUrl();
        if (url is null)
        {
            return;
        }
        _dataSource = NpgsqlDataSource.Create(url);
        _ledger = new ProducerLedger(_dataSource);
        await _ledger.InitializeSchemaAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }
    }

    private static ProducerEvent Event(string id, string fileName, string vehicle, long seq, int posIndex) =>
        new(
            EventId: $"evt-{id}",
            FileName: fileName,
            ProviderVehicleId: vehicle,
            EventType: "VehicleCreated",
            Sequence: seq,
            Position: $"{fileName}:{posIndex:D6}",
            Payload: $"{{\"provider_vehicle_id\":\"{vehicle}\",\"vin\":\"V\",\"make\":\"M\",\"model\":\"X\",\"year\":2020,\"price\":1.0,\"mileage\":1,\"condition\":\"Used\",\"location\":\"L\",\"listing_status\":\"Active\",\"sold\":false}}");

    [Fact]
    public async Task RepeatedFileHashDoesNotCauseDuplicateProcessing()
    {
        if (_ledger is null)
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_LEDGER_URL not set");
            return;
        }

        var fileName = $"t-{Guid.NewGuid():N}.csv";
        await _ledger.BeginFileAsync(fileName, "hash-a", CancellationToken.None);
        // A second BeginFile for the same file name is what a restart or a
        // duplicate poll tick would do — must not error or create a second row.
        await _ledger.BeginFileAsync(fileName, "hash-a", CancellationToken.None);

        var row = await _ledger.GetFileAsync(fileName, CancellationToken.None);
        Assert.NotNull(row);
        Assert.Equal(FileStatus.InProgress, row!.Status);
    }

    [Fact]
    public async Task IncompleteRunResumesWithThePersistedEventIdsAndPayloads()
    {
        if (_ledger is null)
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_LEDGER_URL not set");
            return;
        }

        var fileName = $"t-{Guid.NewGuid():N}.csv";
        await _ledger.BeginFileAsync(fileName, "hash-b", CancellationToken.None);
        var ev = Event("resume-1", fileName, "v1", 1, 0);
        await _ledger.PersistEventsAsync(
            [ev], new Dictionary<string, long> { ["v1"] = 1 }, CancellationToken.None);

        // Simulate a crash-and-restart: read the events back as a resumed
        // run would, with no in-memory state surviving.
        var resumed = await _ledger.GetEventsForFileAsync(fileName, CancellationToken.None);
        var resumedRow = Assert.Single(resumed);
        Assert.Equal(ev.EventId, resumedRow.Event.EventId);
        Assert.Equal(ev.Payload, resumedRow.Event.Payload);
        Assert.Equal(EventStatus.Pending, resumedRow.Status);

        // A second persist attempt for the same (deterministic) event id is
        // what re-running the same diff on resume would produce — must not
        // create a duplicate row or change the existing one.
        await _ledger.PersistEventsAsync(
            [ev], new Dictionary<string, long> { ["v1"] = 1 }, CancellationToken.None);
        var afterRetry = await _ledger.GetEventsForFileAsync(fileName, CancellationToken.None);
        Assert.Single(afterRetry);
    }

    [Fact]
    public async Task BaselineIsNotCommittedWhileAnEventIsStillUnacked()
    {
        if (_ledger is null)
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_LEDGER_URL not set");
            return;
        }

        var fileName = $"t-{Guid.NewGuid():N}.csv";
        var vehicleId = $"v-{Guid.NewGuid():N}";
        await _ledger.BeginFileAsync(fileName, "hash-c", CancellationToken.None);
        var ev = Event("baseline-1", fileName, vehicleId, 1, 0);
        await _ledger.PersistEventsAsync(
            [ev], new Dictionary<string, long> { [vehicleId] = 1 }, CancellationToken.None);

        // Deliberately never ack the event and never call CommitBaselineAsync.
        var baseline = await _ledger.LoadBaselineAsync(CancellationToken.None);
        Assert.False(baseline.ContainsKey(vehicleId), "an unacked event's vehicle must not appear in the baseline");

        var file = await _ledger.GetFileAsync(fileName, CancellationToken.None);
        Assert.Equal(FileStatus.InProgress, file!.Status);
    }

    [Fact]
    public async Task BaselineIsCommittedOnlyAfterEveryEventIsAcked()
    {
        if (_ledger is null)
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_LEDGER_URL not set");
            return;
        }

        var fileName = $"t-{Guid.NewGuid():N}.csv";
        var vehicleId = $"v-{Guid.NewGuid():N}";
        await _ledger.BeginFileAsync(fileName, "hash-d", CancellationToken.None);
        var ev = Event("baseline-2", fileName, vehicleId, 1, 0);
        await _ledger.PersistEventsAsync(
            [ev], new Dictionary<string, long> { [vehicleId] = 1 }, CancellationToken.None);

        await _ledger.MarkEventAckedAsync(ev.EventId, CancellationToken.None);
        await _ledger.CommitBaselineAsync(fileName, CancellationToken.None);

        var baseline = await _ledger.LoadBaselineAsync(CancellationToken.None);
        Assert.True(baseline.ContainsKey(vehicleId));
        Assert.Equal("V", baseline[vehicleId].Vin);

        var file = await _ledger.GetFileAsync(fileName, CancellationToken.None);
        Assert.Equal(FileStatus.Succeeded, file!.Status);
    }
}
