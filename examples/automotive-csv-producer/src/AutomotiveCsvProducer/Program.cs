using AutomotiveCsvProducer;
using AutomotiveCsvProducer.Grpc;
using AutomotiveCsvProducer.Ledger;
using Npgsql;

// kulyrax ingest listens on plain TCP, no TLS — Kestrel/tonic negotiates
// HTTP/2 over cleartext (h2c), but .NET's SocketsHttpHandler refuses
// unencrypted HTTP/2 by default (it assumes "http://" means HTTP/1.1 unless
// told otherwise), which surfaces as the connection dying mid-stream rather
// than a clear "unsupported" error. This is the documented opt-in for
// talking h2c to a plaintext gRPC server.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<ProducerOptions>(builder.Configuration.GetSection("Producer"));

builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ProducerOptions>>().Value;
    return NpgsqlDataSource.Create(opts.LedgerConnectionString);
});
builder.Services.AddSingleton<ProducerLedger>();

builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ProducerOptions>>().Value;
    var logger = sp.GetRequiredService<ILogger<KulyraxIngestClient>>();
    return new KulyraxIngestClient(opts.KulyraxEndpoint, logger);
});

builder.Services.AddSingleton<SnapshotProcessor>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
