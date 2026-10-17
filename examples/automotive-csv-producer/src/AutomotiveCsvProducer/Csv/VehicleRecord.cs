namespace AutomotiveCsvProducer.Csv;

/// <summary>
/// One row of the provider's daily inventory snapshot, after validation.
/// All ten columns are required — there is no "partial" vehicle record.
/// </summary>
public sealed record VehicleRecord(
    string ProviderVehicleId,
    string Vin,
    string Make,
    string Model,
    int Year,
    decimal Price,
    int Mileage,
    string Condition,
    string Location,
    string ListingStatus)
{
    /// <summary>
    /// The normalized business fields used for snapshot comparison — ignores
    /// nothing textual (every field is already normalized by the time a
    /// <see cref="VehicleRecord"/> exists), but gives the comparer one place
    /// to compare against rather than reconstructing a tuple by hand.
    /// </summary>
    public bool HasSameBusinessFieldsAs(VehicleRecord other) =>
        Price == other.Price && HasSameNonPriceFieldsAs(other);

    /// <summary>
    /// Everything except <see cref="Price"/> — splits a change into its
    /// price component and its "everything else" component, since the two
    /// map onto different events (<c>VehiclePriceChanged</c> vs
    /// <c>VehicleUpdated</c>) and a change can touch either, both, or neither.
    /// </summary>
    public bool HasSameNonPriceFieldsAs(VehicleRecord other) =>
        Vin == other.Vin
        && Make == other.Make
        && Model == other.Model
        && Year == other.Year
        && Mileage == other.Mileage
        && Condition == other.Condition
        && Location == other.Location
        && ListingStatus == other.ListingStatus;
}
