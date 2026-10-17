using System.Net.Http.Json;
using Npgsql;
using Xunit;

namespace AutomotiveCsvProducer.Tests;

/// <summary>
/// The spec's "critical failure-injection scenario" against the real, running
/// stack (see README "Running the demo"): Elasticsearch is made to reject
/// writes for real (its index is set read-only — a genuine, deterministic
/// Elasticsearch failure, not a mock), while Postgres and MongoDB stay
/// healthy. A new snapshot changes exactly one vehicle, so exactly one event
/// is generated. This proves, against the actual Docker stack:
///
/// 1. Postgres projects the change (ContinueOnError means Elasticsearch's
///    failure does not block it) while Elasticsearch does not — partial
///    success across destinations, not atomicity.
/// 2. Kulyrax's Ack reports the dead-letter; the producer stops this
///    snapshot and does not advance its baseline.
/// 3. The file is left in <c>data/incoming/</c> (not moved to <c>failed/</c>)
///    because some of its events already landed for real.
/// 4. After the underlying issue is fixed and the documented recovery SQL is
///    run (README "Recovering from a failed file"), the same persisted
///    event — same event id, same payload — is retried and succeeds.
///
/// Gated on <c>AUTOMOTIVE_E2E=1</c>, same convention as
/// <see cref="EndToEndDockerTest"/>.
/// </summary>
public class FailureInjectionDockerTest
{
    private static bool Enabled() => Environment.GetEnvironmentVariable("AUTOMOTIVE_E2E") == "1";

    private const string KulyraxConnString =
        "Host=localhost;Port=5432;Database=kulyrax;Username=postgres;Password=postgres";
    private const string LedgerConnString =
        "Host=localhost;Port=5432;Database=producer_ledger;Username=postgres;Password=postgres";

    [Fact]
    public async Task ElasticsearchFailureHaltsTheSnapshotWithoutAdvancingTheBaselineThenRecovers()
    {
        if (!Enabled())
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_E2E not set to 1");
            return;
        }

        var repoRoot = FindExampleRoot();
        var incoming = Path.Combine(repoRoot, "data", "incoming");
        var archive = Path.Combine(repoRoot, "data", "archive");
        Directory.CreateDirectory(incoming);
        const string fileName = "2026-01-03.csv";

        using var http = new HttpClient();

        // 1. Make Elasticsearch genuinely reject writes to this index — not a
        // mock, a real cluster block an operator could hit (e.g. a disk
        // watermark).
        await SetIndexReadOnlyAsync(http, true);

        try
        {
            // 2. Drop a snapshot with exactly one change (v1003's price), so
            // exactly one event is generated.
            CopyAtomically(Path.Combine(repoRoot, "samples", fileName), Path.Combine(incoming, fileName));

            var (eventId, failedAt) = await WaitForFileStatusAsync(fileName, "failed", TimeSpan.FromSeconds(60));

            // 3. Postgres got the change for real (ContinueOnError); the
            // ledger's baseline did not advance; the file was not moved.
            Assert.True(failedAt.HasValue);
            await using (var kulyrax = new NpgsqlConnection(KulyraxConnString))
            {
                await kulyrax.OpenAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT price FROM vehicles WHERE provider_vehicle_id = 'v1003'", kulyrax);
                var price = (double)(await cmd.ExecuteScalarAsync())!;
                Assert.Equal(780000.00, price, precision: 2); // Postgres already has the new price
            }

            var esDoc = await FetchEsDocAsync(http, "v1003");
            Assert.Equal(750000.0, esDoc!["price"]!.GetValue<double>(), 0.01); // Elasticsearch still has the OLD price

            await using (var ledger = new NpgsqlConnection(LedgerConnString))
            {
                await ledger.OpenAsync();
                await using var cmd = new NpgsqlCommand(
                    "SELECT price FROM vehicle_baseline WHERE provider_vehicle_id = 'v1003'", ledger);
                var baselinePrice = (decimal)(await cmd.ExecuteScalarAsync())!;
                Assert.Equal(750000.00m, baselinePrice); // producer's own baseline did not advance either
            }

            Assert.True(File.Exists(Path.Combine(incoming, fileName)), "a dead-lettered file stays in incoming/, per the README");
            Assert.False(File.Exists(Path.Combine(archive, fileName)));

            // 4. Fix the underlying issue, then follow the documented
            // recovery procedure (README "Recovering from a failed file").
            await SetIndexReadOnlyAsync(http, false);
            await using (var ledger = new NpgsqlConnection(LedgerConnString))
            {
                await ledger.OpenAsync();
                await using var cmd = new NpgsqlCommand(
                    "DELETE FROM producer_events WHERE file_name = $1; DELETE FROM processed_files WHERE file_name = $1;",
                    ledger)
                { Parameters = { new() { Value = fileName } } };
                await cmd.ExecuteNonQueryAsync();
            }

            var (recoveredEventId, _) = await WaitForFileStatusAsync(fileName, "succeeded", TimeSpan.FromSeconds(60));
            Assert.Equal(eventId, recoveredEventId); // same deterministic event id regenerated from the same file content

            var esDocAfter = await FetchEsDocAsync(http, "v1003");
            Assert.Equal(780000.0, esDocAfter!["price"]!.GetValue<double>(), 0.01);
            Assert.False(File.Exists(Path.Combine(incoming, fileName)));
            Assert.True(File.Exists(Path.Combine(archive, fileName)));
        }
        finally
        {
            await SetIndexReadOnlyAsync(http, false); // never leave the shared index blocked for other tests
        }
    }

    private static async Task SetIndexReadOnlyAsync(HttpClient http, bool readOnly)
    {
        var resp = await http.PutAsync(
            "http://localhost:9200/vehicles/_settings",
            JsonContent(new { index = new { blocks = new { write = readOnly } } }));
        resp.EnsureSuccessStatusCode();
    }

    private static HttpContent JsonContent(object body) =>
        new StringContent(System.Text.Json.JsonSerializer.Serialize(body), System.Text.Encoding.UTF8, "application/json");

    private static async Task<System.Text.Json.Nodes.JsonObject?> FetchEsDocAsync(HttpClient http, string id)
    {
        var resp = await http.GetAsync($"http://localhost:9200/vehicles/_doc/{id}");
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<System.Text.Json.Nodes.JsonObject>();
        return body?["_source"]?.AsObject();
    }

    /// <returns>the file's one generated event id, and when it reached the target status (null if still waiting).</returns>
    private static async Task<(string EventId, DateTime? ReachedAt)> WaitForFileStatusAsync(
        string fileName, string status, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            await using var ledger = new NpgsqlConnection(LedgerConnString);
            await ledger.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "SELECT status FROM processed_files WHERE file_name = $1", ledger)
            { Parameters = { new() { Value = fileName } } };
            var actual = (string?)await cmd.ExecuteScalarAsync();
            if (actual == status)
            {
                await using var evCmd = new NpgsqlCommand(
                    "SELECT event_id FROM producer_events WHERE file_name = $1", ledger)
                { Parameters = { new() { Value = fileName } } };
                var eventId = (string)(await evCmd.ExecuteScalarAsync())!;
                return (eventId, DateTime.UtcNow);
            }
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
        throw new TimeoutException($"{fileName} did not reach status '{status}' within {timeout}");
    }

    private static void CopyAtomically(string source, string dest)
    {
        var tmp = dest + ".tmp";
        File.Copy(source, tmp, overwrite: true);
        File.Move(tmp, dest, overwrite: true);
    }

    private static string FindExampleRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker-compose.yml")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new InvalidOperationException("could not locate the example root (docker-compose.yml)");
    }
}
