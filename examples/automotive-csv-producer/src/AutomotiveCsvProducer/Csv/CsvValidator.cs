using System.Globalization;

namespace AutomotiveCsvProducer.Csv;

/// <summary>
/// Validates a whole provider inventory CSV against the fixed v1 schema
/// before a single <see cref="VehicleRecord"/> is produced. All-or-nothing:
/// any structural or row-level problem rejects the entire file, since a
/// partially-trusted snapshot is worse than no snapshot — a short read would
/// otherwise look identical to a real inventory drop.
/// </summary>
public static class CsvValidator
{
    public static readonly string[] ExpectedColumns =
    [
        "provider_vehicle_id",
        "vin",
        "make",
        "model",
        "year",
        "price",
        "mileage",
        "condition",
        "location",
        "listing_status",
    ];

    private static readonly HashSet<string> AllowedConditions = new(StringComparer.Ordinal)
    {
        "New",
        "Used",
        "CertifiedPreOwned",
    };

    private static readonly HashSet<string> AllowedListingStatuses = new(StringComparer.Ordinal)
    {
        "Active",
        "Inactive",
        "Pending",
    };

    public static CsvValidationResult Validate(IReadOnlyList<string> lines)
    {
        var errors = new List<string>();

        if (lines.Count == 0)
        {
            return CsvValidationResult.Failure(["file is empty: no header row"]);
        }

        var header = CsvLineParser.SplitLine(lines[0]);
        if (!header.SequenceEqual(ExpectedColumns, StringComparer.Ordinal))
        {
            errors.Add(
                $"header does not match the expected schema. expected: [{string.Join(",", ExpectedColumns)}], got: [{string.Join(",", header)}]");
            // A wrong header makes every row's column positions meaningless —
            // stop here rather than piling on confusing per-row noise.
            return CsvValidationResult.Failure(errors);
        }

        var records = new List<VehicleRecord>();
        var seenIds = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 1; i < lines.Count; i++)
        {
            var line = lines[i];
            if (line.Length == 0)
            {
                // A trailing blank line (common with a trailing newline) is
                // not a data row; anything else blank mid-file is still
                // skipped rather than treated as 10 empty columns, since a
                // genuinely blank line carries no column-count signal either way.
                continue;
            }

            var rowNumber = i + 1; // 1-based, header is row 1
            var fields = CsvLineParser.SplitLine(line);
            if (fields.Count != ExpectedColumns.Length)
            {
                errors.Add(
                    $"row {rowNumber}: expected {ExpectedColumns.Length} columns, got {fields.Count}");
                continue;
            }

            var record = ValidateRow(fields, rowNumber, errors);
            if (record is null)
            {
                continue;
            }

            if (!seenIds.Add(record.ProviderVehicleId))
            {
                errors.Add(
                    $"row {rowNumber}: duplicate provider_vehicle_id '{record.ProviderVehicleId}' within this file");
                continue;
            }

            records.Add(record);
        }

        if (records.Count == 0 && errors.Count == 0)
        {
            errors.Add("file has a valid header but zero data rows");
        }

        return errors.Count == 0
            ? CsvValidationResult.Success(records)
            : CsvValidationResult.Failure(errors);
    }

    private static VehicleRecord? ValidateRow(List<string> fields, int rowNumber, List<string> errors)
    {
        var providerVehicleId = fields[0].Trim();
        var vin = fields[1].Trim();
        var make = fields[2].Trim();
        var model = fields[3].Trim();
        var yearRaw = fields[4].Trim();
        var priceRaw = fields[5].Trim();
        var mileageRaw = fields[6].Trim();
        var condition = fields[7].Trim();
        var location = fields[8].Trim();
        var listingStatus = fields[9].Trim();

        var rowErrors = new List<string>();

        if (providerVehicleId.Length == 0)
        {
            rowErrors.Add("provider_vehicle_id must not be empty");
        }
        if (vin.Length == 0)
        {
            rowErrors.Add("vin must not be empty");
        }
        if (make.Length == 0)
        {
            rowErrors.Add("make must not be empty");
        }
        if (model.Length == 0)
        {
            rowErrors.Add("model must not be empty");
        }

        var year = 0;
        if (!int.TryParse(yearRaw, NumberStyles.None, CultureInfo.InvariantCulture, out year)
            || year < 1900 || year > 2100)
        {
            rowErrors.Add($"year must be an integer between 1900 and 2100, got '{yearRaw}'");
        }

        decimal price = 0;
        if (!decimal.TryParse(priceRaw, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out price)
            || price < 0)
        {
            rowErrors.Add($"price must be a non-negative decimal (INR, invariant culture), got '{priceRaw}'");
        }

        var mileage = 0;
        if (!int.TryParse(mileageRaw, NumberStyles.None, CultureInfo.InvariantCulture, out mileage)
            || mileage < 0)
        {
            rowErrors.Add($"mileage must be a non-negative integer, got '{mileageRaw}'");
        }

        if (!AllowedConditions.Contains(condition))
        {
            rowErrors.Add(
                $"condition must be one of [{string.Join(",", AllowedConditions)}], got '{condition}'");
        }
        if (location.Length == 0)
        {
            rowErrors.Add("location must not be empty");
        }
        if (!AllowedListingStatuses.Contains(listingStatus))
        {
            rowErrors.Add(
                $"listing_status must be one of [{string.Join(",", AllowedListingStatuses)}], got '{listingStatus}'");
        }

        if (rowErrors.Count > 0)
        {
            errors.AddRange(rowErrors.Select(e => $"row {rowNumber}: {e}"));
            return null;
        }

        return new VehicleRecord(
            providerVehicleId,
            vin,
            make,
            model,
            year,
            price,
            mileage,
            condition,
            location,
            listingStatus);
    }
}
