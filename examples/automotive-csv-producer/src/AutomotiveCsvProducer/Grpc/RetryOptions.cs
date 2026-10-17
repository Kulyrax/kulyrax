namespace AutomotiveCsvProducer.Grpc;

public sealed record RetryOptions(
    int MaxAttempts = 5,
    TimeSpan BaseDelay = default,
    TimeSpan MaxDelay = default)
{
    public static RetryOptions Default { get; } = new(
        MaxAttempts: 5,
        BaseDelay: TimeSpan.FromMilliseconds(200),
        MaxDelay: TimeSpan.FromSeconds(5));
}
