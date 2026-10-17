using AutomotiveCsvProducer.Ledger;
using Microsoft.Extensions.Options;

namespace AutomotiveCsvProducer;

public sealed class Worker(
    SnapshotProcessor processor,
    ProducerLedger ledger,
    IOptions<ProducerOptions> options,
    ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        Directory.CreateDirectory(opts.InputDirectory);
        Directory.CreateDirectory(opts.ArchiveDirectory);
        Directory.CreateDirectory(opts.FailedDirectory);

        logger.LogInformation(
            "automotive-csv-producer starting. watching {Dir}, kulyrax at {Endpoint}",
            opts.InputDirectory,
            opts.KulyraxEndpoint);

        await ledger.InitializeSchemaAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            bool didWork;
            try
            {
                didWork = await processor.TickAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "unhandled error during tick — will retry after the poll interval");
                didWork = false;
            }

            if (!didWork)
            {
                await Task.Delay(opts.PollInterval, stoppingToken);
            }
        }
    }
}
