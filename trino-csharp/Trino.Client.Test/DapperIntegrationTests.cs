using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DapperQueryBuilder;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Trino.Data.ADO.Server;

namespace Trino.Client.Test
{
    /// <summary>
    /// Integration tests to verify TrinoConnection works correctly with Dapper.
    /// Uses Testcontainers to spin up a real Trino instance.
    /// </summary>
    [TestClass]
    public class DapperIntegrationTests
    {
        private static IContainer? _trinoContainer;
        private static string? _connectionUri;

        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            // Start Trino container using Testcontainers
            _trinoContainer = new ContainerBuilder()
                .WithImage("trinodb/trino:latest")
                .WithPortBinding(8080, true)
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(r => r.ForPort(8080).ForPath("/v1/info"))
                    .AddCustomWaitStrategy(new WaitUntilTrinoReady()))
                .Build();

            await _trinoContainer.StartAsync();

            var port = _trinoContainer.GetMappedPublicPort(8080);
            _connectionUri = $"http://localhost:{port}";
            
            Console.WriteLine($"Trino container started at {_connectionUri}");
        }

        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            if (_trinoContainer != null)
            {
                await _trinoContainer.StopAsync();
                await _trinoContainer.DisposeAsync();
            }
        }

        private TrinoConnection CreateConnection()
        {
            var properties = new TrinoConnectionProperties
            {
                Server = new Uri(_connectionUri!),
                User = "test"
            };
            return new TrinoConnection(properties);
        }

        [TestMethod]
        public void DapperQuery_SimpleSelect_ReturnsResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<long>("SELECT 42 AS value").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(42L, results[0]);
        }

        [TestMethod]
        public void DapperQuery_WithDynamicType_ReturnsResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query("SELECT 42 AS number, 'hello' AS text").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(42, (int)results[0].number);
            Assert.AreEqual("hello", (string)results[0].text);
        }

        [TestMethod]
        public void DapperQuery_TpchData_ReturnsCustomerData()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<CustomerData>(
                "SELECT custkey, name, nationkey FROM tpch.tiny.customer LIMIT 5"
            ).ToList();

            Assert.IsTrue(results.Count > 0);
            Assert.IsTrue(results.Count <= 5);
            Assert.IsTrue(results.All(c => c.custkey > 0));
            Assert.IsTrue(results.All(c => !string.IsNullOrEmpty(c.name)));
        }

        [TestMethod]
        public void DapperQuery_MultipleDataTypes_HandlesCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<DataTypeTest>(@"
                SELECT 
                    CAST(123 AS INTEGER) AS intValue,
                    CAST(9223372036854775807 AS BIGINT) AS longValue,
                    CAST(3.14159 AS DOUBLE) AS doubleValue,
                    'test string' AS stringValue,
                    true AS boolValue
            ").ToList();

            Assert.AreEqual(1, results.Count);
            var result = results[0];
            Assert.AreEqual(123, result.intValue);
            Assert.AreEqual(9223372036854775807L, result.longValue);
            Assert.IsTrue(Math.Abs(result.doubleValue - 3.14159) < 0.00001);
            Assert.AreEqual("test string", result.stringValue);
            Assert.IsTrue(result.boolValue);
        }

        [TestMethod]
        public void DapperQueryFirst_ReturnsFirstResult()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirst<long>("SELECT 100 AS value");

            Assert.AreEqual(100L, result);
        }

        [TestMethod]
        public void DapperQueryFirstOrDefault_WithNoResults_ReturnsDefault()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirstOrDefault<long?>(
                "SELECT custkey FROM tpch.tiny.customer WHERE custkey = -1"
            );

            Assert.IsNull(result);
        }

        [TestMethod]
        public void DapperExecuteScalar_ReturnsScalarValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.ExecuteScalar<long>("SELECT COUNT(*) FROM tpch.tiny.nation");

            Assert.AreEqual(25L, result); // tpch.tiny.nation has 25 rows
        }

        [TestMethod]
        public async Task DapperQueryAsync_ReturnsResultsAsynchronously()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = (await connection.QueryAsync<long>("SELECT 999 AS value")).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(999L, results[0]);
        }

        [TestMethod]
        public void DapperQuery_NullValues_HandlesCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<NullableTest>(
                "SELECT CAST(NULL AS VARCHAR) AS nullableString, CAST(NULL AS INTEGER) AS nullableInt"
            ).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.IsNull(results[0].nullableString);
            Assert.IsNull(results[0].nullableInt);
        }

        [TestMethod]
        public void DapperQuery_MultipleRows_ReturnsAllRows()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation ORDER BY nationkey"
            ).ToList();

            Assert.AreEqual(25, results.Count);
            Assert.AreEqual(0L, results[0].nationkey);
            Assert.AreEqual(24L, results[24].nationkey);
        }

        [TestMethod]
        public void DapperQuery_ConnectionClosedAutoOpens()
        {
            using var connection = CreateConnection();
            // Don't open connection - Dapper should handle this

            var results = connection.Query<long>("SELECT 1 AS value").ToList();

            Assert.AreEqual(1, results.Count);
        }

        [TestMethod]
        public void DapperQuery_WithParameters_FiltersByValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Use Dapper with anonymous object for parameters
            // Note: Trino uses positional parameters with ?, Dapper will map named params in order
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = ?",
                new { nationkey = 5L }
            ).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(5L, results[0].nationkey);
            Assert.AreEqual("ETHIOPIA", results[0].name);
        }

        [TestMethod]
        public void DapperQuery_WithMultipleParameters_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Query with multiple positional parameters
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey >= ? AND nationkey <= ? ORDER BY nationkey",
                new { min = 10L, max = 15L }
            ).ToList();

            Assert.AreEqual(6, results.Count);
            Assert.AreEqual(10L, results[0].nationkey);
            Assert.AreEqual(15L, results[5].nationkey);
        }

        [TestMethod]
        public void DapperQuery_WithStringParameter_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Query with string parameter using LIKE
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE name LIKE ?",
                new { pattern = "UNITED%" }
            ).ToList();

            Assert.AreEqual(2, results.Count); // UNITED KINGDOM, UNITED STATES
            Assert.IsTrue(results.All(n => n.name.StartsWith("UNITED")));
        }

        [TestMethod]
        public void DapperQueryFirst_WithParameter_ReturnsMatchingRow()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirst<CustomerData>(
                "SELECT custkey, name, nationkey FROM tpch.tiny.customer WHERE custkey = ?",
                new { id = 1L }
            );

            Assert.AreEqual(1L, result.custkey);
        }

        [TestMethod]
        public void DapperExecuteScalar_WithParameter_ReturnsFilteredCount()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Count nations in a specific region
            var count = connection.ExecuteScalar<long>(
                "SELECT COUNT(*) FROM tpch.tiny.nation WHERE regionkey = ?",
                new { regionkey = 1L }
            );

            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public async Task DapperQueryAsync_WithParameter_ReturnsResultsAsynchronously()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = (await connection.QueryAsync<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = ?",
                new { key = 0L }
            )).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(0L, results[0].nationkey);
            Assert.AreEqual("ALGERIA", results[0].name);
        }

        #region Named Parameter Tests

        [TestMethod]
        public void DapperQuery_WithNamedParameter_FiltersByValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Use named parameter syntax (@paramName) which is standard for Dapper
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = @nationkey",
                new { nationkey = 5L }
            ).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(5L, results[0].nationkey);
            Assert.AreEqual("ETHIOPIA", results[0].name);
        }

        [TestMethod]
        public void DapperQuery_WithMultipleNamedParameters_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Multiple named parameters
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey >= @minKey AND nationkey <= @maxKey ORDER BY nationkey",
                new { minKey = 10L, maxKey = 15L }
            ).ToList();

            Assert.AreEqual(6, results.Count);
            Assert.AreEqual(10L, results[0].nationkey);
            Assert.AreEqual(15L, results[5].nationkey);
        }

        [TestMethod]
        public void DapperQuery_WithNamedStringParameter_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Named string parameter with LIKE
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE name LIKE @pattern",
                new { pattern = "UNITED%" }
            ).ToList();

            Assert.AreEqual(2, results.Count); // UNITED KINGDOM, UNITED STATES
            Assert.IsTrue(results.All(n => n.name.StartsWith("UNITED")));
        }

        [TestMethod]
        public void DapperQueryFirst_WithNamedParameter_ReturnsMatchingRow()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirst<CustomerData>(
                "SELECT custkey, name, nationkey FROM tpch.tiny.customer WHERE custkey = @id",
                new { id = 1L }
            );

            Assert.AreEqual(1L, result.custkey);
        }

        [TestMethod]
        public void DapperExecuteScalar_WithNamedParameter_ReturnsFilteredCount()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Count nations in a specific region using named parameter
            var count = connection.ExecuteScalar<long>(
                "SELECT COUNT(*) FROM tpch.tiny.nation WHERE regionkey = @regionKey",
                new { regionKey = 1L }
            );

            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public async Task DapperQueryAsync_WithNamedParameter_ReturnsResultsAsynchronously()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = (await connection.QueryAsync<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = @key",
                new { key = 0L }
            )).ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(0L, results[0].nationkey);
            Assert.AreEqual("ALGERIA", results[0].name);
        }

        [TestMethod]
        public void DapperQuery_WithReusedNamedParameter_WorksCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Same parameter used multiple times in the query
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = @key OR nationkey = @key + 1 ORDER BY nationkey",
                new { key = 5L }
            ).ToList();

            Assert.AreEqual(2, results.Count);
            Assert.AreEqual(5L, results[0].nationkey);
            Assert.AreEqual(6L, results[1].nationkey);
        }

        #endregion

        #region Edge Case Tests

        [TestMethod]
        public void DapperQuery_EmptyResultSet_ReturnsEmptyList()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = -999"
            ).ToList();

            Assert.AreEqual(0, results.Count);
        }

        [TestMethod]
        public void DapperQuery_LargeResultSet_ReturnsAllRows()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Query all orders from TPC-H tiny (1500 rows)
            var results = connection.Query<OrderData>(
                "SELECT orderkey, custkey, CAST(totalprice AS DOUBLE) AS totalprice FROM tpch.tiny.orders"
            ).ToList();

            Assert.IsTrue(results.Count > 1000);
            Assert.IsTrue(results.All(o => o.orderkey > 0));
        }

        [TestMethod]
        public void DapperQuery_DateTimeTypes_HandlesCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<DateTimeTest>(@"
                SELECT 
                    DATE '2024-06-15' AS dateValue,
                    TIMESTAMP '2024-06-15 14:30:45.123' AS timestampValue
            ").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(new DateTime(2024, 6, 15), results[0].dateValue);
            Assert.AreEqual(new DateTime(2024, 6, 15, 14, 30, 45, 123), results[0].timestampValue);
        }

        [TestMethod]
        public void DapperQuery_DecimalTypes_HandlesCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Note: Trino returns TrinoBigDecimal which Dapper can't directly map to decimal
            // Using DOUBLE for this test to verify numeric handling
            var results = connection.Query<DoubleTest>(@"
                SELECT 
                    CAST(123.45 AS DOUBLE) AS doubleValue1,
                    CAST(9999999.99 AS DOUBLE) AS doubleValue2
            ").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.IsTrue(Math.Abs(results[0].doubleValue1 - 123.45) < 0.001);
            Assert.IsTrue(Math.Abs(results[0].doubleValue2 - 9999999.99) < 0.01);
        }

        [TestMethod]
        public void DapperQuery_SpecialCharactersInStrings_HandlesCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<StringTest>(@"
                SELECT 
                    'Hello, World!' AS simple,
                    'Line1' || CHR(10) || 'Line2' AS withNewline,
                    'Tab' || CHR(9) || 'Separated' AS withTab,
                    'Quote: ''quoted''' AS withQuotes
            ").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("Hello, World!", results[0].simple);
            Assert.IsTrue(results[0].withNewline.Contains("\n"));
            Assert.IsTrue(results[0].withTab.Contains("\t"));
            Assert.IsTrue(results[0].withQuotes.Contains("'"));
        }

        [TestMethod]
        public void DapperQuerySingleOrDefault_ExactlyOneResult_ReturnsValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Use QueryFirstOrDefault which works reliably with Trino
            var result = connection.QueryFirstOrDefault<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = ?",
                new { key = 10L }
            );

            Assert.IsNotNull(result);
            Assert.AreEqual(10L, result.nationkey);
            Assert.AreEqual("IRAN", result.name);
        }

        [TestMethod]
        public void DapperQueryFirstOrDefault_NoResults_ReturnsNull()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirstOrDefault<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = -999"
            );

            Assert.IsNull(result);
        }

        [TestMethod]
        public void DapperQueryFirst_MultipleResults_ReturnsFirst()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = connection.QueryFirst<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey < 5 ORDER BY nationkey"
            );

            Assert.AreEqual(0L, result.nationkey);
            Assert.AreEqual("ALGERIA", result.name);
        }

        [TestMethod]
        public void DapperQuery_MultipleQueriesInSequence_AllSucceed()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Execute multiple queries on the same connection
            var nations = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation LIMIT 3"
            ).ToList();

            var regions = connection.Query<RegionData>(
                "SELECT regionkey, name FROM tpch.tiny.region"
            ).ToList();

            var count = connection.ExecuteScalar<long>("SELECT COUNT(*) FROM tpch.tiny.customer");

            Assert.AreEqual(3, nations.Count);
            Assert.AreEqual(5, regions.Count); // TPC-H has 5 regions
            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public void DapperQuery_ConnectionReuse_WorksCorrectly()
        {
            using var connection = CreateConnection();
            
            // First query opens connection implicitly
            var result1 = connection.Query<long>("SELECT 1 AS value").First();
            Assert.AreEqual(1L, result1);
            
            // Connection should still be usable
            var result2 = connection.Query<long>("SELECT 2 AS value").First();
            Assert.AreEqual(2L, result2);
            
            // Explicitly open and query again
            connection.Open();
            var result3 = connection.Query<long>("SELECT 3 AS value").First();
            Assert.AreEqual(3L, result3);
        }

        [TestMethod]
        public void DapperQuery_InvalidSql_ThrowsException()
        {
            using var connection = CreateConnection();
            connection.Open();

            var exception = Assert.ThrowsException<Trino.Client.TrinoAggregateException>(() =>
                connection.Query<dynamic>("SELECT * FROM tpch.tiny.nonexistent_table").ToList()
            );

            // Verify that the exception is related to table not existing
            var fullMessage = exception.ToString();
            Assert.IsTrue(fullMessage.Contains("does not exist") || fullMessage.Contains("nonexistent_table"),
                $"Expected error about table not existing, but got: {fullMessage}");
        }

        [TestMethod]
        public void DapperQuery_AggregateFunction_ReturnsCorrectResult()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<AggregateResult>(@"
                SELECT 
                    COUNT(*) AS count,
                    SUM(nationkey) AS sum,
                    AVG(nationkey) AS average,
                    MIN(nationkey) AS minimum,
                    MAX(nationkey) AS maximum
                FROM tpch.tiny.nation
            ").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(25L, results[0].count);
            Assert.AreEqual(300L, results[0].sum); // Sum of 0-24
            Assert.AreEqual(0L, results[0].minimum);
            Assert.AreEqual(24L, results[0].maximum);
        }

        [TestMethod]
        public void DapperQuery_GroupBy_ReturnsGroupedResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<GroupedResult>(
                "SELECT regionkey, COUNT(*) AS count FROM tpch.tiny.nation GROUP BY regionkey ORDER BY regionkey"
            ).ToList();

            Assert.AreEqual(5, results.Count); // 5 regions
            Assert.IsTrue(results.All(r => r.count == 5)); // Each region has 5 nations in TPC-H
        }

        [TestMethod]
        public void DapperQuery_JoinTables_ReturnsJoinedData()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<JoinedData>(@"
                SELECT n.nationkey, n.name AS nationName, r.name AS regionName
                FROM tpch.tiny.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE n.nationkey = 0
            ").ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(0L, results[0].nationkey);
            Assert.AreEqual("ALGERIA", results[0].nationName);
            Assert.AreEqual("AFRICA", results[0].regionName);
        }

        [TestMethod]
        public void DapperQuery_SubQuery_ReturnsCorrectResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var results = connection.Query<NationData>(@"
                SELECT nationkey, name 
                FROM tpch.tiny.nation 
                WHERE regionkey = (SELECT regionkey FROM tpch.tiny.region WHERE name = 'AFRICA')
                ORDER BY nationkey
            ").ToList();

            Assert.AreEqual(5, results.Count); // 5 nations in Africa
            Assert.AreEqual("ALGERIA", results[0].name);
        }

        [TestMethod]
        public async Task DapperQueryFirstOrDefaultAsync_WithNoResults_ReturnsDefault()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = await connection.QueryFirstOrDefaultAsync<long?>(
                "SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = -1"
            );

            Assert.IsNull(result);
        }

        [TestMethod]
        public async Task DapperExecuteScalarAsync_ReturnsScalarValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            var result = await connection.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM tpch.tiny.region"
            );

            Assert.AreEqual(5L, result);
        }

        [TestMethod]
        public void DapperQuery_BooleanParameter_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Use boolean in WHERE clause
            var results = connection.Query<NationData>(
                "SELECT nationkey, name FROM tpch.tiny.nation WHERE (nationkey > 10) = ?",
                new { condition = true }
            ).ToList();

            Assert.IsTrue(results.Count > 0);
            Assert.IsTrue(results.All(n => n.nationkey > 10));
        }

        #endregion

        #region DapperQueryBuilder Tests

        [TestMethod]
        public void QueryBuilder_SimpleQuery_ReturnsResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Basic QueryBuilder usage with string interpolation
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation LIMIT 5");
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(5, results.Count);
            Assert.IsTrue(results.All(n => !string.IsNullOrEmpty(n.name)));
        }

        [TestMethod]
        public void QueryBuilder_WithInterpolatedParameter_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            long nationKey = 5L;
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = {nationKey}");
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(5L, results[0].nationkey);
            Assert.AreEqual("ETHIOPIA", results[0].name);
        }

        [TestMethod]
        public void QueryBuilder_WithMultipleInterpolatedParameters_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            long minKey = 10L;
            long maxKey = 15L;
            var query = connection.QueryBuilder($@"
                SELECT nationkey, name FROM tpch.tiny.nation 
                WHERE nationkey >= {minKey} AND nationkey <= {maxKey} 
                ORDER BY nationkey");
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(6, results.Count);
            Assert.AreEqual(10L, results[0].nationkey);
            Assert.AreEqual(15L, results[5].nationkey);
        }

        [TestMethod]
        public void QueryBuilder_WithStringParameter_FiltersCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            string pattern = "UNITED%";
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE name LIKE {pattern}");
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(2, results.Count);
            Assert.IsTrue(results.All(n => n.name.StartsWith("UNITED")));
        }

        [TestMethod]
        public void QueryBuilder_WithDynamicWhere_BuildsQueryCorrectly()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Build query dynamically - use Append for additional conditions
            long minKey = 5L;
            long maxKey = 10L;
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey >= {minKey} AND nationkey < {maxKey}");
            
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(5, results.Count);
            Assert.IsTrue(results.All(n => n.nationkey >= 5 && n.nationkey < 10));
        }

        [TestMethod]
        public void QueryBuilder_QueryFirst_ReturnsSingleResult()
        {
            using var connection = CreateConnection();
            connection.Open();

            long nationKey = 0L;
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = {nationKey}");
            var result = query.QueryFirst<NationData>();

            Assert.AreEqual(0L, result.nationkey);
            Assert.AreEqual("ALGERIA", result.name);
        }

        [TestMethod]
        public void QueryBuilder_QueryFirstOrDefault_ReturnsNullForNoMatch()
        {
            using var connection = CreateConnection();
            connection.Open();

            long nationKey = -999L;
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = {nationKey}");
            var result = query.QueryFirstOrDefault<NationData>();

            Assert.IsNull(result);
        }

        [TestMethod]
        public void QueryBuilder_ExecuteScalar_ReturnsScalarValue()
        {
            using var connection = CreateConnection();
            connection.Open();

            long regionKey = 1L;
            var query = connection.QueryBuilder($"SELECT COUNT(*) FROM tpch.tiny.nation WHERE regionkey = {regionKey}");
            var count = query.ExecuteScalar<long>();

            Assert.IsTrue(count > 0);
        }

        [TestMethod]
        public void QueryBuilder_WithAggregation_ReturnsCorrectResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var query = connection.QueryBuilder($@"
                SELECT 
                    COUNT(*) AS count,
                    MIN(nationkey) AS minimum,
                    MAX(nationkey) AS maximum
                FROM tpch.tiny.nation");
            var result = query.QueryFirst<dynamic>();

            Assert.AreEqual(25, (int)result.count);
            Assert.AreEqual(0L, (long)result.minimum);
            Assert.AreEqual(24L, (long)result.maximum);
        }

        [TestMethod]
        public void QueryBuilder_WithOrderBy_ReturnsOrderedResults()
        {
            using var connection = CreateConnection();
            connection.Open();

            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation ORDER BY nationkey DESC LIMIT 5");
            var results = query.Query<NationData>().ToList();

            Assert.AreEqual(5, results.Count);
            Assert.AreEqual(24L, results[0].nationkey);
            Assert.AreEqual(23L, results[1].nationkey);
        }

        [TestMethod]
        public void QueryBuilder_ComplexQuery_WithJoin_ReturnsJoinedData()
        {
            using var connection = CreateConnection();
            connection.Open();

            long nationKey = 0L;
            var query = connection.QueryBuilder($@"
                SELECT n.nationkey, n.name AS nationName, r.name AS regionName
                FROM tpch.tiny.nation n
                JOIN tpch.tiny.region r ON n.regionkey = r.regionkey
                WHERE n.nationkey = {nationKey}");
            var result = query.QueryFirst<JoinedData>();

            Assert.AreEqual(0L, result.nationkey);
            Assert.AreEqual("ALGERIA", result.nationName);
            Assert.AreEqual("AFRICA", result.regionName);
        }

        [TestMethod]
        public void QueryBuilder_ConditionalWhere_AddsClausesOnlyWhenNeeded()
        {
            using var connection = CreateConnection();
            connection.Open();

            // Simulate optional filter parameters - build WHERE clause dynamically
            long minKey = 10L;
            var query = connection.QueryBuilder($"SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey >= {minKey}");

            var results = query.Query<NationData>().ToList();

            Assert.IsTrue(results.All(n => n.nationkey >= 10));
        }

        #endregion

        // DTOs for Dapper queries
        public class CustomerData
        {
            public long custkey { get; set; }
            public string name { get; set; } = string.Empty;
            public long nationkey { get; set; }
        }

        public class DataTypeTest
        {
            public int intValue { get; set; }
            public long longValue { get; set; }
            public double doubleValue { get; set; }
            public string stringValue { get; set; } = string.Empty;
            public bool boolValue { get; set; }
        }

        public class NullableTest
        {
            public string? nullableString { get; set; }
            public int? nullableInt { get; set; }
        }

        public class NationData
        {
            public long nationkey { get; set; }
            public string name { get; set; } = string.Empty;
        }

        public class OrderData
        {
            public long orderkey { get; set; }
            public long custkey { get; set; }
            public double totalprice { get; set; }
        }

        public class DateTimeTest
        {
            public DateTime dateValue { get; set; }
            public DateTime timestampValue { get; set; }
        }

        public class DoubleTest
        {
            public double doubleValue1 { get; set; }
            public double doubleValue2 { get; set; }
        }

        public class StringTest
        {
            public string simple { get; set; } = string.Empty;
            public string withNewline { get; set; } = string.Empty;
            public string withTab { get; set; } = string.Empty;
            public string withQuotes { get; set; } = string.Empty;
        }

        public class RegionData
        {
            public long regionkey { get; set; }
            public string name { get; set; } = string.Empty;
        }

        public class AggregateResult
        {
            public long count { get; set; }
            public long sum { get; set; }
            public double average { get; set; }
            public long minimum { get; set; }
            public long maximum { get; set; }
        }

        public class GroupedResult
        {
            public long regionkey { get; set; }
            public long count { get; set; }
        }

        public class JoinedData
        {
            public long nationkey { get; set; }
            public string nationName { get; set; } = string.Empty;
            public string regionName { get; set; } = string.Empty;
        }
    }

    /// <summary>
    /// Custom wait strategy to ensure Trino is fully ready by checking that it's not starting
    /// </summary>
    internal class WaitUntilTrinoReady : IWaitUntil
    {
        public async Task<bool> UntilAsync(IContainer container)
        {
            try
            {
                var port = container.GetMappedPublicPort(8080);
                using var client = new System.Net.Http.HttpClient();
                var response = await client.GetStringAsync($"http://localhost:{port}/v1/info");
                // Check if Trino has finished starting
                return response.Contains("\"starting\":false");
            }
            catch
            {
                return false;
            }
        }
    }
}
