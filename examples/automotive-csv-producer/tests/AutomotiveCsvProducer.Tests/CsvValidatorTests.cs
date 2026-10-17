using AutomotiveCsvProducer.Csv;
using Xunit;

namespace AutomotiveCsvProducer.Tests;

public class CsvValidatorTests
{
    private const string ValidHeader =
        "provider_vehicle_id,vin,make,model,year,price,mileage,condition,location,listing_status";

    [Fact]
    public void ValidFileProducesExpectedRecords()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000.50,15000,Used,Mumbai,Active",
            "v2,VIN0002,Toyota,Fortuner,2023,3500000,5000,New,Pune,Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.True(result.IsValid, string.Join("; ", result.Errors));
        Assert.Equal(2, result.Records.Count);
        Assert.Equal("v1", result.Records[0].ProviderVehicleId);
        Assert.Equal(1200000.50m, result.Records[0].Price);
    }

    [Fact]
    public void WrongHeaderRejectsWholeFile()
    {
        var lines = new[]
        {
            "id,vin,make,model,year,price,mileage,condition,location,listing_status",
            "v1,VIN0001,Honda,City,2021,1200000,15000,Used,Mumbai,Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.False(result.IsValid);
        Assert.Empty(result.Records);
        Assert.Contains(result.Errors, e => e.Contains("header does not match"));
    }

    [Fact]
    public void OneBadRowRejectsTheWholeFileNotJustThatRow()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000,15000,Used,Mumbai,Active",
            // negative price on this row only
            "v2,VIN0002,Toyota,Fortuner,2023,-5,5000,New,Pune,Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.False(result.IsValid);
        Assert.Empty(result.Records);
        Assert.Contains(result.Errors, e => e.Contains("row 3") && e.Contains("price"));
    }

    [Fact]
    public void DuplicateProviderVehicleIdRejectsTheWholeFile()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000,15000,Used,Mumbai,Active",
            "v1,VIN0002,Toyota,Fortuner,2023,3500000,5000,New,Pune,Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("duplicate provider_vehicle_id"));
    }

    [Fact]
    public void InvalidConditionIsRejected()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000,15000,Pristine,Mumbai,Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("condition"));
    }

    [Fact]
    public void MissingColumnCountIsRejected()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000,15000,Used,Mumbai",
        };

        var result = CsvValidator.Validate(lines);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("expected 10 columns"));
    }

    [Fact]
    public void QuotedFieldWithEmbeddedCommaParsesCorrectly()
    {
        var lines = new[]
        {
            ValidHeader,
            "v1,VIN0001,Honda,City,2021,1200000,15000,Used,\"Mumbai, MH\",Active",
        };

        var result = CsvValidator.Validate(lines);

        Assert.True(result.IsValid, string.Join("; ", result.Errors));
        Assert.Equal("Mumbai, MH", result.Records[0].Location);
    }
}
