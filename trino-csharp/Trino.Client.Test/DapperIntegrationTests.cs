using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
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
