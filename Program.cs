using Microsoft.VisualBasic.FileIO;

class Program
{
    static void Main(string[] args)
    {
        var csvFilePath = @"C:\Users\Jade.Lopez\OneDrive - WiseTech Global Pty Ltd\Desktop\Work Items\International Logistics Core\WI00876146 - ABEREF EK (176) - add new Productsdelete old ones\250422_UA_commodity_codes_TC_added.csv";
        var sqlFilePath = @"C:\Users\Jade.Lopez\OneDrive - WiseTech Global Pty Ltd\Desktop\Work Items\International Logistics Core\WI00876146 - ABEREF EK (176) - add new Productsdelete old ones\250422_UA_commodity_codes_TC_added.sql";

        var tables = new List<(string tableName, string columnNames)>
        {
            ("dbo.RefAirlineProductCode", "RAR_PK, RAR_AirlineID, RAR_Code, RAR_Description" ),
            ("dbo.RefAirlineCommodityCode", "RAC_PK, RAC_AirlineID, RAC_Code, RAC_Description" ),
            ("dbo.RefAirlineProductCodeCommodityCodePivot", "RPC_PK, RPC_AirlineID, RPC_RAR, RPC_RAC")
        };
        
        GenerateSQLFromCSV(csvFilePath, sqlFilePath, tables);
    }

    static void GenerateSQLFromCSV(string csvFilePath, string sqlFilePath, List<(string tableName, string columnNames)> tables)
    {
        var queries = new List<string>();

        // DELETE
        queries.Add("DECLARE @success bit = 1");
        queries.Add(@"
BEGIN TRANSACTION
BEGIN TRY");
        queries.Add("use RefDbRepoSafe");
        queries.Add(@"
-- Delete Product, Commodity & Pivot Codes.

DELETE RefAirlineProductCodeCommodityCodePivot WHERE RPC_AirlineID = '016';");

        var commodityCodeList = new List<string>();
        var productCodeList = new List<string>();

        using (var reader = new TextFieldParser(csvFilePath))
        {
            reader.SetDelimiters(",");
            reader.HasFieldsEnclosedInQuotes = true;

            reader.ReadFields(); // Ignore first line column names

            while (!reader.EndOfData)
            {
                var recordSplit = reader.ReadFields();
                if (!commodityCodeList.Contains($"'{recordSplit[3]}'"))
                {
                    commodityCodeList.Add($"'{recordSplit[3]}'");
                }

                if (!productCodeList.Contains($"'{recordSplit[1]}'"))
                {
                    productCodeList.Add($"'{recordSplit[1]}'");
                }
            }
        }

        var commodityCodes = string.Join(", ", commodityCodeList);
        var productCodes = string.Join(", ", productCodeList);

        queries.Add($@"
UPDATE RefDbVersionControl SET RVC_Deleted = 1, RVC_IsPublished = 0
    WHERE RVC_ParentPK IN (SELECT RAC_PK FROM RefAirlineCommodityCode WHERE RAC_AirlineID = '016' AND RAC_Code NOT IN ({commodityCodes})) 

UPDATE RefDbVersionControl SET RVC_Deleted = 1, RVC_IsPublished = 0
    WHERE RVC_ParentPK IN (SELECT RAR_PK FROM RefAirlineProductCode WHERE RAR_AirlineID = '016' AND RAR_Code NOT IN ({productCodes})) 
");

        // INSERT/UPDATE
        queries.Add("-- Insert or Update all Product, Commodity & Pivot Codes from the CSV into tables: 1. RefAirlineProductCode,  2. RefAirlineCommodityCode,  3. RefAirlineProductCodeCommodityCodePivot.");

        var firstPivotCode = true;
        foreach (var (tableName, columnNames) in tables)
        {
            using (var reader = new TextFieldParser(csvFilePath))
            {
                reader.SetDelimiters(",");
                reader.HasFieldsEnclosedInQuotes = true;

                reader.ReadFields(); // Ignore first line column names

                if (tableName == "dbo.RefAirlineProductCodeCommodityCodePivot" && firstPivotCode)
                {
                    queries.Add(@"
DECLARE @productPK UNIQUEIDENTIFIER;
DECLARE @commodityPK UNIQUEIDENTIFIER;
");
                    firstPivotCode = false;
                }

                while (!reader.EndOfData)
                {
                    var recordSplit = reader.ReadFields();
                    var singularQuery = CreateInsertOrUpdateQuery(tableName, columnNames, recordSplit);
                    if (!queries.Contains(singularQuery))
                    {
                        queries.Add(singularQuery);
                    }
                }
            }
        }

        queries.Add(@"
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION
    SET @success = 0
END CATCH

IF(@success = 1)
BEGIN
    COMMIT TRANSACTION
END
");

        File.WriteAllLines(sqlFilePath, queries);
        Console.WriteLine($"The SQL query has been written to {sqlFilePath}");
    }

    static string CreateInsertOrUpdateQuery(string tableName, string columnNames, string[] values)
    {
        return tableName switch
        {
            "dbo.RefAirlineProductCode" => GenerateProductCodeQuery(tableName, columnNames, values),
            "dbo.RefAirlineCommodityCode" => GenerateCommodityCodeQuery(tableName, columnNames, values),
            "dbo.RefAirlineProductCodeCommodityCodePivot" => GeneratePivotCodeQuery(tableName, columnNames, values),
            _ => string.Empty
        };
    }

    static string GenerateProductCodeQuery(string tableName, string columnNames, string[] values) =>
@$"
IF EXISTS (SELECT 1 FROM {tableName} WHERE RAR_Code = '{values[1]}' AND RAR_AirlineID = '016')
    UPDATE {tableName} SET RAR_Description = '{values[2]}' WHERE RAR_Code = '{values[1]}' AND RAR_AirlineID = '016'; 
ELSE
    INSERT INTO {tableName} ({columnNames}) VALUES (NEWID(), '{values[0]}', '{values[1]}', '{values[2]}');
";

    static string GenerateCommodityCodeQuery(string tableName, string columnNames, string[] values) =>
@$"
IF EXISTS (SELECT 1 FROM {tableName} WHERE RAC_Code = '{values[3]}' AND RAC_AirlineID = '016')
    UPDATE {tableName} SET RAC_Description = '{values[4]}' WHERE RAC_Code = '{values[3]}' AND RAC_AirlineID = '016'; 
ELSE
    INSERT INTO {tableName} ({columnNames}) VALUES (NEWID(), '{values[0]}', '{values[3]}', '{values[4]}');
";

    static string GeneratePivotCodeQuery(string tableName, string columnNames, string[] values) =>
$@"
SELECT @productPK = RAR_PK FROM dbo.RefAirlineProductCode WHERE RAR_Code = '{values[1]}' AND RAR_AirlineID = '016';
SELECT @commodityPK = RAC_PK FROM dbo.RefAirlineCommodityCode WHERE RAC_Code = '{values[3]}' AND RAC_AirlineID = '016';
                    
IF NOT EXISTS (SELECT 1 FROM {tableName} WHERE RPC_RAR = @productPK AND RPC_RAC = @commodityPK AND RPC_AirlineID = '016')
    INSERT INTO {tableName} ({columnNames}) VALUES (NEWID(), '{values[0]}', @productPK, @commodityPK);
";

}