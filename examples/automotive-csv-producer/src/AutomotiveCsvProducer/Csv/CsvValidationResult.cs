namespace AutomotiveCsvProducer.Csv;

public sealed record CsvValidationResult(
    bool IsValid,
    IReadOnlyList<VehicleRecord> Records,
    IReadOnlyList<string> Errors)
{
    public static CsvValidationResult Success(IReadOnlyList<VehicleRecord> records) =>
        new(true, records, Array.Empty<string>());

    public static CsvValidationResult Failure(IReadOnlyList<string> errors) =>
        new(false, Array.Empty<VehicleRecord>(), errors);
}
