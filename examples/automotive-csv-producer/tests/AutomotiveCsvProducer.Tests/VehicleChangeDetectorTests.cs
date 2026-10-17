using AutomotiveCsvProducer.Csv;
using AutomotiveCsvProducer.Snapshot;
using Xunit;

namespace AutomotiveCsvProducer.Tests;

public class VehicleChangeDetectorTests
{
    private static VehicleRecord Vehicle(
        string id = "v1",
        string vin = "VIN0001",
        string make = "Honda",
        string model = "City",
        int year = 2021,
        decimal price = 1_200_000m,
        int mileage = 15000,
        string condition = "Used",
        string location = "Mumbai",
        string listingStatus = "Active") =>
        new(id, vin, make, model, year, price, mileage, condition, location, listingStatus);

    private static readonly SnapshotDiffOptions DefaultOptions = new();

    [Fact]
    public void FirstAppearanceInInitialSnapshotIsCreated()
    {
        var previous = new Dictionary<string, VehicleRecord>();
        var current = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle() };

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        var change = Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Created, change.Kind);
        Assert.Null(change.Previous);
        Assert.NotNull(change.Current);
    }

    [Fact]
    public void UnchangedVehicleProducesNoEvents()
    {
        var v = Vehicle();
        var previous = new Dictionary<string, VehicleRecord> { ["v1"] = v };
        var current = new Dictionary<string, VehicleRecord> { ["v1"] = v with { } };

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        Assert.Empty(result.Changes);
    }

    [Fact]
    public void PriceOnlyChangeProducesSinglePriceChangedEvent()
    {
        var previous = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle(price: 1_200_000m) };
        var current = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle(price: 1_100_000m) };

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        var change = Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.PriceChanged, change.Kind);
        Assert.Equal(1_200_000m, change.Previous!.Price);
        Assert.Equal(1_100_000m, change.Current!.Price);
    }

    [Fact]
    public void NonPriceOnlyChangeProducesSingleUpdatedEvent()
    {
        var previous = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle(mileage: 15000) };
        var current = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle(mileage: 15500) };

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        var change = Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Updated, change.Kind);
    }

    [Fact]
    public void CombinedPriceAndNonPriceChangeProducesBothEvents()
    {
        var previous = new Dictionary<string, VehicleRecord>
        {
            ["v1"] = Vehicle(price: 1_200_000m, mileage: 15000),
        };
        var current = new Dictionary<string, VehicleRecord>
        {
            ["v1"] = Vehicle(price: 1_100_000m, mileage: 15500),
        };

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        Assert.Equal(2, result.Changes.Count);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.PriceChanged);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Updated);
    }

    [Fact]
    public void DisappearanceFromValidatedSnapshotIsSold()
    {
        var previous = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle() };
        var current = new Dictionary<string, VehicleRecord>();

        // Below the drop safeguard: 1 of 1 is 100% — use a second untouched
        // vehicle so the drop fraction (1 of 2 = 50%) sits at the default
        // threshold boundary and is still accepted.
        previous["v2"] = Vehicle(id: "v2");
        current["v2"] = Vehicle(id: "v2");

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        var change = Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Sold, change.Kind);
        Assert.Equal("v1", change.ProviderVehicleId);
        Assert.Null(change.Current);
    }

    [Fact]
    public void ReappearanceOfPreviouslyAbsentVehicleIsCreatedNotUpdated()
    {
        // Day 1: v1 present. Day 2: v1 sold (absent). Day 3: v1 back — from
        // the detector's perspective this is day2-vs-day3, where v1 is
        // simply new again relative to the baseline it's comparing against.
        var previous = new Dictionary<string, VehicleRecord>(); // day 2 baseline: v1 absent
        var current = new Dictionary<string, VehicleRecord> { ["v1"] = Vehicle() }; // day 3: back

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        var change = Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Created, change.Kind);
    }

    [Fact]
    public void SuspiciouslyLargeDropIsRejectedWithNoEvents()
    {
        var previous = new Dictionary<string, VehicleRecord>();
        var current = new Dictionary<string, VehicleRecord>();
        for (var i = 0; i < 10; i++)
        {
            previous[$"v{i}"] = Vehicle(id: $"v{i}");
        }
        // Only 2 of 10 survive — an 80% drop, far past the 50% default safeguard.
        current["v0"] = previous["v0"];
        current["v1"] = previous["v1"];

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.False(result.IsAccepted);
        Assert.Empty(result.Changes);
        Assert.NotNull(result.RejectionReason);
    }

    [Fact]
    public void DropAtOrBelowThresholdIsAccepted()
    {
        var previous = new Dictionary<string, VehicleRecord>();
        var current = new Dictionary<string, VehicleRecord>();
        for (var i = 0; i < 10; i++)
        {
            previous[$"v{i}"] = Vehicle(id: $"v{i}");
        }
        // Exactly 50% survive — at the default threshold, not past it.
        for (var i = 0; i < 5; i++)
        {
            current[$"v{i}"] = previous[$"v{i}"];
        }

        var result = VehicleChangeDetector.Detect(previous, current, DefaultOptions);

        Assert.True(result.IsAccepted);
        Assert.Equal(5, result.Changes.Count(c => c.Kind == ChangeKind.Sold));
    }
}
