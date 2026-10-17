namespace AutomotiveCsvProducer;

public sealed class ProducerOptions
{
    /// <summary>Directory the provider drops finalized <c>*.csv</c> files into.</summary>
    public string InputDirectory { get; set; } = "/data/incoming";

    /// <summary>Where successfully processed files are moved.</summary>
    public string ArchiveDirectory { get; set; } = "/data/archive";

    /// <summary>Where files that failed validation or diffing are moved.</summary>
    public string FailedDirectory { get; set; } = "/data/failed";

    /// <summary>How often the worker polls <see cref="InputDirectory"/> for new files.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The Kulyrax gRPC ingest endpoint, e.g. <c>http://kulyrax-ingest:50051</c>.</summary>
    public string KulyraxEndpoint { get; set; } = "http://localhost:50051";

    /// <summary>Postgres connection string for the producer's own ledger.</summary>
    public string LedgerConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The drop-safeguard threshold (§5): if more than this fraction of the
    /// previous snapshot's vehicles are missing from a new one, the snapshot
    /// is rejected rather than treated as a mass sale.
    /// </summary>
    public double MaxDropFraction { get; set; } = 0.5;

    public int RetryMaxAttempts { get; set; } = 5;
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(200);
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);
}
