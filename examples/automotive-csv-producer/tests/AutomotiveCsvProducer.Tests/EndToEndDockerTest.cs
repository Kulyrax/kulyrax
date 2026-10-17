using Npgsql;
using Xunit;

namespace AutomotiveCsvProducer.Tests;

/// <summary>
/// Exercises the real, running stack: drops both sample snapshots into the
/// shared <c>data/incoming</c> directory, waits for the producer to work
/// through them, and checks all three destinations. Gated on
/// <c>AUTOMOTIVE_E2E=1</c> — this assumes `docker compose up` (see README)
/// is already running, the same way the Rust side of this repo gates its
/// own DB-backed tests on an env var rather than owning container
/// lifecycle itself.
/// </summary>
public class EndToEndDockerTest
{
    private static bool Enabled() => Environment.GetEnvironmentVariable("AUTOMOTIVE_E2E") == "1";

    [Fact]
    public async Task BothSampleSnapshotsProjectCorrectlyToAllThreeStores()
    {
        if (!Enabled())
        {
            Console.WriteLine("SKIP: AUTOMOTIVE_E2E not set to 1");
            return;
        }

        var repoRoot = FindExampleRoot();
        var incoming = Path.Combine(repoRoot, "data", "incoming");
        Directory.CreateDirectory(incoming);

        CopyAtomically(Path.Combine(repoRoot, "samples", "2026-01-01.csv"), Path.Combine(incoming, "2026-01-01.csv"));
        await WaitUntilAsync(async () => await VehicleCountAsync() >= 4, TimeSpan.FromSeconds(60));

        CopyAtomically(Path.Combine(repoRoot, "samples", "2026-01-02.csv"), Path.Combine(incoming, "2026-01-02.csv"));
        await WaitUntilAsync(() => VehicleSoldAsync("v1004"), TimeSpan.FromSeconds(60));

        await using var conn = new NpgsqlConnection(
            "Host=localhost;Port=5432;Database=kulyrax;Username=postgres;Password=postgres");
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "SELECT provider_vehicle_id, price, mileage, sold FROM vehicles WHERE provider_vehicle_id = ANY($1)",
            conn)
        {
            Parameters = { new() { Value = new[] { "v1001", "v1002", "v1003", "v1004", "v1005" } } },
        };
        await using var reader = await cmd.ExecuteReaderAsync();
        // `price` is `double precision`, not `numeric` — see
        // postgres-init/00-init.sql for why — so this reads a double, not a
        // decimal.
        var rows = new Dictionary<string, (double Price, int Mileage, bool Sold)>();
        while (await reader.ReadAsync())
        {
            rows[reader.GetString(0)] = (reader.GetDouble(1), reader.GetInt32(2), reader.GetBoolean(3));
        }

        Assert.Equal(1150000.00, rows["v1001"].Price); // price changed
        Assert.Equal(8500, rows["v1002"].Mileage); // mileage changed
        Assert.False(rows["v1003"].Sold); // unchanged, still present
        Assert.True(rows["v1004"].Sold); // sold
        Assert.True(rows.ContainsKey("v1005")); // new arrival

        using var http = new HttpClient();
        var esJson = await http.GetStringAsync("http://localhost:9200/vehicles/_search?size=50");
        using var esDoc = System.Text.Json.JsonDocument.Parse(esJson);
        var esIds = esDoc.RootElement.GetProperty("hits").GetProperty("hits")
            .EnumerateArray()
            .Select(h => h.GetProperty("_source").GetProperty("provider_vehicle_id").GetString())
            .ToHashSet();
        Assert.Contains("v1005", esIds);
    }

    private static async Task<int> VehicleCountAsync()
    {
        await using var conn = new NpgsqlConnection(
            "Host=localhost;Port=5432;Database=kulyrax;Username=postgres;Password=postgres");
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM vehicles", conn);
        return (int)(long)(await cmd.ExecuteScalarAsync() ?? 0L);
    }

    private static async Task<bool> VehicleSoldAsync(string id)
    {
        await using var conn = new NpgsqlConnection(
            "Host=localhost;Port=5432;Database=kulyrax;Username=postgres;Password=postgres");
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT sold FROM vehicles WHERE provider_vehicle_id = $1", conn)
        { Parameters = { new() { Value = id } } };
        var result = await cmd.ExecuteScalarAsync();
        return result is true;
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await condition())
                {
                    return;
                }
            }
            catch
            {
                // service may not be reachable yet
            }
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
        throw new TimeoutException("condition was not met within the timeout");
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
