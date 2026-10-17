using System.Text;

namespace AutomotiveCsvProducer.Csv;

/// <summary>
/// A minimal RFC4180-ish CSV line splitter: comma-separated fields, double
/// quotes allowed around a field (and required when the field itself
/// contains a comma, a quote, or a newline), <c>""</c> inside a quoted field
/// is a literal quote. Deliberately hand-rolled rather than a NuGet
/// dependency — the schema is small and fixed, and this is the only CSV
/// feature the provider's export actually needs.
/// </summary>
public static class CsvLineParser
{
    public static List<string> SplitLine(string line)
    {
        var fields = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.Length && line[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    current.Append(c);
                }
            }
            else
            {
                switch (c)
                {
                    case ',':
                        fields.Add(current.ToString());
                        current.Clear();
                        break;
                    case '"' when current.Length == 0:
                        inQuotes = true;
                        break;
                    default:
                        current.Append(c);
                        break;
                }
            }
        }

        fields.Add(current.ToString());
        return fields;
    }
}
