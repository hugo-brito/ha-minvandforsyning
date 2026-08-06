using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using ProtoBuf.Data;

// Generates a fully synthetic protobuf-net-data fixture for the
// ha-minvandforsyning parser tests. No value here is derived from any real
// household trace. Emits meter_data.bin (native protobuf-net-data wire format,
// the same family the supplier API uses) and a matching ground_truth.json.

internal static class Program
{
    // Two contiguous UTC hourly windows bracketing the Danish DST transitions.
    // ReadingDate is a naive-UTC end-of-hour timestamp (matches the supplier API).
    private static readonly DateTime AutumnStart = new(2025, 10, 24, 0, 0, 0, DateTimeKind.Unspecified);
    private const int AutumnHours = 96;   // covers 2025-10-26 fall-back (25-hour local day)
    private static readonly DateTime SpringStart = new(2026, 3, 28, 0, 0, 0, DateTimeKind.Unspecified);
    private const int SpringHours = 72;   // covers 2026-03-29 spring-forward (23-hour local day)

    // Hand-authored synthetic hourly consumption profile in litres, indexed by
    // hour-of-day. Deterministic and not fitted to any real trace.
    private static readonly int[] Profile =
    {
        0, 0, 0, 0, 0, 2, 18, 35, 22, 8, 12, 20,
        40, 9, 33, 14, 19, 28, 55, 44, 38, 21, 12, 6,
    };

    private static void Main()
    {
        DataSet ds = BuildDataSet();

        using (FileStream fs = File.Create("meter_data.bin"))
        using (IDataReader reader = ds.CreateDataReader())
        {
            DataSerializer.Serialize(fs, reader);
        }

        File.WriteAllText("ground_truth.json", ToGroundTruth(ds), new UTF8Encoding(false));
        Console.WriteLine($"Wrote meter_data.bin and ground_truth.json (table 6 rows: {ds.Tables[6].Rows.Count})");
    }

    private static DataSet BuildDataSet()
    {
        var ds = new DataSet { Locale = CultureInfo.InvariantCulture };
        ds.Tables.Add(Table("t0",
            ("MR_AnalysisID", typeof(int)), ("CreatedDate", typeof(DateTime))));
        ds.Tables.Add(Table("t1",
            ("MR_Analysis_ItemID", typeof(int)), ("MR_AnalysisID", typeof(int)),
            ("AnalysisType", typeof(int)), ("KeyType", typeof(int))));
        ds.Tables.Add(Table("t2",
            ("MR_Analysis_Item_AcuteNightConsumptionID", typeof(int)), ("MR_Analysis_ItemID", typeof(int)),
            ("NumberOfRealReadingsInInterval", typeof(int)), ("NumberOfZeroConsumptionsInInterval", typeof(int)),
            ("NumberOfHighAlertLevelConsumptions", typeof(int))));
        ds.Tables.Add(Table("t3",
            ("MR_Analysis_Item_FullDayConsumptionID", typeof(int)), ("MR_Analysis_ItemID", typeof(int)),
            ("NumberOfRealReadingsInInterval", typeof(int)), ("MinimumHourlyConsumption", typeof(decimal)),
            ("LatestZeroConsumption", typeof(string))));
        ds.Tables.Add(Table("t4",
            ("MR_Analysis_Item_HistoricalNightConsumptionID", typeof(int)), ("MR_Analysis_ItemID", typeof(int)),
            ("NumberOfNightsWithMoreThan4HoursOfConsumption", typeof(int)), ("Summed_TotalNightConsumption", typeof(decimal))));
        ds.Tables.Add(Table("t5",
            ("MR_Analysis_Item_InfoCodeID", typeof(int)), ("MR_Analysis_ItemID", typeof(int)),
            ("Reading", typeof(decimal)), ("ReadingDate", typeof(DateTime)),
            ("InfoCode_Active", typeof(bool)), ("InfoCode_Value", typeof(byte))));

        DataTable t6 = Table("t6",
            ("TS", typeof(int)), ("ReadingDate", typeof(DateTime)), ("Reading", typeof(decimal)),
            ("InfoCode", typeof(int)), ("TSOfPrior", typeof(int)),
            ("Consumption", typeof(decimal)), ("OwingNext", typeof(decimal)));
        t6.Columns["InfoCode"]!.AllowDBNull = true;
        FillReadings(t6);
        ds.Tables.Add(t6);

        ds.Tables.Add(Table("t7",
            ("AnalysisType", typeof(int)), ("MeterInAnalysisCount", typeof(int))));
        return ds;
    }

    private static DataTable Table(string name, params (string Name, Type Type)[] cols)
    {
        var t = new DataTable(name) { Locale = CultureInfo.InvariantCulture };
        foreach ((string cn, Type ct) in cols)
        {
            t.Columns.Add(cn, ct);
        }
        return t;
    }

    private static void FillReadings(DataTable t)
    {
        int ts = 0;
        int milliM3 = 100_000; // synthetic baseline 100.000 m3, tracked in milli-m3

        void AddWindow(DateTime start, int hours)
        {
            for (int h = 0; h < hours; h++)
            {
                ts++;
                DateTime when = start.AddHours(h);
                int litres = Profile[when.Hour];
                milliM3 += litres;
                DataRow row = t.NewRow();
                row["TS"] = ts;
                row["ReadingDate"] = when;
                row["Reading"] = milliM3 / 1000m;
                row["InfoCode"] = DBNull.Value;
                row["TSOfPrior"] = ts - 1;
                row["Consumption"] = (decimal)litres;
                row["OwingNext"] = 0m;
                t.Rows.Add(row);
            }
        }

        AddWindow(AutumnStart, AutumnHours);
        AddWindow(SpringStart, SpringHours);
    }

    private static string ToGroundTruth(DataSet ds)
    {
        var tables = new List<object>();
        for (int ti = 0; ti < ds.Tables.Count; ti++)
        {
            DataTable t = ds.Tables[ti];
            var columns = new List<object>();
            foreach (DataColumn c in t.Columns)
            {
                columns.Add(new { name = c.ColumnName, type = ProtoTypeName(c.DataType) });
            }
            var rows = new List<Dictionary<string, object?>>();
            foreach (DataRow r in t.Rows)
            {
                var obj = new Dictionary<string, object?>();
                foreach (DataColumn c in t.Columns)
                {
                    obj[c.ColumnName] = Cell(r[c], c.DataType);
                }
                rows.Add(obj);
            }
            tables.Add(new { index = ti, columns, rows });
        }
        return JsonSerializer.Serialize(tables, new JsonSerializerOptions { WriteIndented = true });
    }

    private static object? Cell(object v, Type t)
    {
        if (v is null or DBNull)
        {
            return null;
        }
        if (t == typeof(DateTime))
        {
            return ((DateTime)v).ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }
        if (t == typeof(decimal))
        {
            return ((decimal)v).ToString(CultureInfo.InvariantCulture);
        }
        if (t == typeof(byte))
        {
            return (int)(byte)v;
        }
        if (t == typeof(bool))
        {
            return (bool)v;
        }
        if (t == typeof(int))
        {
            return (int)v;
        }
        return v.ToString();
    }

    private static string ProtoTypeName(Type t)
    {
        if (t == typeof(int)) return "Int32";
        if (t == typeof(DateTime)) return "DateTime";
        if (t == typeof(decimal)) return "Decimal";
        if (t == typeof(string)) return "String";
        if (t == typeof(bool)) return "Boolean";
        if (t == typeof(byte)) return "Byte";
        return t.Name;
    }
}
