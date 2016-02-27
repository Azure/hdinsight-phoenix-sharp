// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

namespace PhoenixSharp.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using PhoenixSharp;
    using Apache.Phoenix;
    using PhoenixSharp.UnitTests.Utilities;
    using pbc = global::Google.Protobuf.Collections;
    using System.Diagnostics;

    [TestClass]
    public class PhoenixClientTests : DisposableContextSpecification
    {
        private ClusterCredentials _credentials;

        protected override void Context()
        {
            _credentials = ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt");
        }

        [TestMethod]
        public async Task SimpleTest()
        {
            var client = new PhoenixClient(_credentials);
            const string hex_characters = "0123456789abcdef";
            var random = new Random();
            // Generating a random connection ID
            string connId = new string(Enumerable.Repeat(hex_characters, 8).Select(s=>s[random.Next(s.Length)]).ToArray());
            // Opening connection
            OpenConnectionResponse openConnResponse = await client.OpenConnectionRequestAsync(connId);
            // Syncing connection
            ConnectionProperties connProperties = new ConnectionProperties
            {
                HasAutoCommit = true,
                AutoCommit = true,
                HasReadOnly = true,
                ReadOnly = false,
                TransactionIsolation = 0,
                Catalog = "",
                Schema = "",
                IsDirty = true
            };
            ConnectionSyncResponse connSyncResponse = await client.ConnectionSyncRequestAsync(connId, connProperties);

            // Creating statement 1
            CreateStatementResponse createStatementResponse1 = await client.CreateStatementRequestAsync(connId);
            // Running query 1
            string sql1 = "CREATE TABLE Persons (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
            ExecuteResponse execResponse1 = await client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse1.StatementId);
            // Closing statement 1
            CloseStatementResponse closeStatementResponse1 = await client.CloseStatementRequestAsync(connId, createStatementResponse1.StatementId);

            // Creating statement 2
            CreateStatementResponse createStatementResponse2 = await client.CreateStatementRequestAsync(connId);
            // Running query 2
            string sql2 = "UPSERT INTO Persons VALUES ('duo','xu')";
            ExecuteResponse execResponse2 = await client.PrepareAndExecuteRequestAsync(connId, sql2, 100, createStatementResponse2.StatementId);
            // Closing statement 2
            CloseStatementResponse closeStatementResponse2 = await client.CloseStatementRequestAsync(connId, createStatementResponse2.StatementId);

            // Creating statement 3
            CreateStatementResponse createStatementResponse3 = await client.CreateStatementRequestAsync(connId);
            // Running query 3
            string sql3 = "select count(*) from Persons";
            ExecuteResponse execResponse3 = await client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse3.StatementId);
            long count = execResponse3.Results[0].FirstFrame.Rows[0].Value[0].Value[0].NumberValue;
            Assert.AreEqual(1, count);
            // Closing statement 3
            CloseStatementResponse closeStatementResponse3 = await client.CloseStatementRequestAsync(connId, createStatementResponse3.StatementId);

            // Creating statement 4
            CreateStatementResponse createStatementResponse4 = await client.CreateStatementRequestAsync(connId);
            // Running query 4
            string sql4 = "DROP TABLE Persons";
            ExecuteResponse execResponse4 = await client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse4.StatementId);
            // Closing statement 4
            CloseStatementResponse closeStatementResponse4 = await client.CloseStatementRequestAsync(connId, createStatementResponse4.StatementId);
            
            // Closing connection
            CloseConnectionResponse closeConnResponse = await client.CloseConnectionRequestAsync(connId);
        }

        [TestMethod]
        public async Task ManyRowInsertTest()
        {
            var client = new PhoenixClient(_credentials);
            const string hex_characters = "0123456789abcdef";
            var random = new Random();
            // Generating a random connection ID
            string connId = new string(Enumerable.Repeat(hex_characters, 8).Select(s => s[random.Next(s.Length)]).ToArray());
            // Opening connection
            OpenConnectionResponse openConnResponse = await client.OpenConnectionRequestAsync(connId);
            // Syncing connection
            ConnectionProperties connProperties = new ConnectionProperties
            {
                HasAutoCommit = true,
                AutoCommit = false,
                HasReadOnly = true,
                ReadOnly = false,
                TransactionIsolation = 0,
                Catalog = "",
                Schema = "",
                IsDirty = true
            };
            ConnectionSyncResponse connSyncResponse = await client.ConnectionSyncRequestAsync(connId, connProperties);

            // Creating statement 1
            CreateStatementResponse createStatementResponse1 = await client.CreateStatementRequestAsync(connId);
            // Running query 1
            string sql1 = "CREATE TABLE Persons (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
            ExecuteResponse execResponse1 = await client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse1.StatementId);
            // Closing statement 1
            CloseStatementResponse closeStatementResponse1 = await client.CloseStatementRequestAsync(connId, createStatementResponse1.StatementId);

            // Commit statement 1
            await client.CommitRequestAsync(connId);

            // Creating statement 2
            string sql2 = "UPSERT INTO Persons VALUES (?,?)";
            PrepareResponse prepareResponse = await client.PrepareRequestAsync(connId, sql2, 100);
            StatementHandle statementHandle = prepareResponse.Statement;
            for (int i=0; i < 10; i++)
            {
                pbc::RepeatedField<TypedValue> list = new pbc.RepeatedField<TypedValue>();
                TypedValue v1 = new TypedValue
                {
                    StringValue = "d"+i,
                    Type = Rep.STRING
                };
                TypedValue v2 = new TypedValue
                {
                    StringValue = "x"+i,
                    Type = Rep.STRING
                };
                list.Add(v1);
                list.Add(v2);
                ExecuteResponse executeResponse = await client.ExecuteRequestAsync(statementHandle, list, 100, true);
            }

            // Commit statement 2
            await client.CommitRequestAsync(connId);

            // Creating statement 3
            CreateStatementResponse createStatementResponse3 = await client.CreateStatementRequestAsync(connId);
            // Running query 3
            string sql3 = "select count(*) from Persons";
            ExecuteResponse execResponse3 = await client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse3.StatementId);
            long count = execResponse3.Results[0].FirstFrame.Rows[0].Value[0].Value[0].NumberValue;
            Assert.AreEqual(10, count);
            // Closing statement 3
            CloseStatementResponse closeStatementResponse3 = await client.CloseStatementRequestAsync(connId, createStatementResponse3.StatementId);

            // Creating statement 4
            CreateStatementResponse createStatementResponse4 = await client.CreateStatementRequestAsync(connId);
            // Running query 4
            string sql4 = "DROP TABLE Persons";
            ExecuteResponse execResponse4 = await client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse4.StatementId);
            // Closing statement 4
            CloseStatementResponse closeStatementResponse4 = await client.CloseStatementRequestAsync(connId, createStatementResponse4.StatementId);

            // Commit statement 4
            await client.CommitRequestAsync(connId);

            // Closing connection
            CloseConnectionResponse closeConnResponse = await client.CloseConnectionRequestAsync(connId);
        }

        [TestMethod]
        public async Task QueryManyRowTest()
        {
            var client = new PhoenixClient(_credentials);
            const string hex_characters = "0123456789abcdef";
            var random = new Random();
            // Generating a random connection ID
            string connId = new string(Enumerable.Repeat(hex_characters, 8).Select(s => s[random.Next(s.Length)]).ToArray());
            // Opening connection
            OpenConnectionResponse openConnResponse = await client.OpenConnectionRequestAsync(connId);
            // Syncing connection
            ConnectionProperties connProperties = new ConnectionProperties
            {
                HasAutoCommit = true,
                AutoCommit = false,
                HasReadOnly = true,
                ReadOnly = false,
                TransactionIsolation = 0,
                Catalog = "",
                Schema = "",
                IsDirty = true
            };
            ConnectionSyncResponse connSyncResponse = await client.ConnectionSyncRequestAsync(connId, connProperties);

            // Creating statement 1
            CreateStatementResponse createStatementResponse1 = await client.CreateStatementRequestAsync(connId);
            // Running query 1
            string sql1 = "CREATE TABLE Persons (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
            ExecuteResponse execResponse1 = await client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse1.StatementId);
            // Closing statement 1
            CloseStatementResponse closeStatementResponse1 = await client.CloseStatementRequestAsync(connId, createStatementResponse1.StatementId);

            // Commit statement 1
            await client.CommitRequestAsync(connId);

            // Creating statement 2
            string sql2 = "UPSERT INTO Persons VALUES (?,?)";
            PrepareResponse prepareResponse = await client.PrepareRequestAsync(connId, sql2, 100);
            StatementHandle statementHandle = prepareResponse.Statement;
            for (int i = 0; i < 10; i++)
            {
                pbc::RepeatedField<TypedValue> list = new pbc.RepeatedField<TypedValue>();
                TypedValue v1 = new TypedValue
                {
                    StringValue = "d" + i,
                    Type = Rep.STRING
                };
                TypedValue v2 = new TypedValue
                {
                    StringValue = "x" + i,
                    Type = Rep.STRING
                };
                list.Add(v1);
                list.Add(v2);
                ExecuteResponse executeResponse = await client.ExecuteRequestAsync(statementHandle, list, 100, true);
            }

            // Commit statement 2
            await client.CommitRequestAsync(connId);

            // Creating statement 3
            CreateStatementResponse createStatementResponse3 = await client.CreateStatementRequestAsync(connId);
            // Running query 3
            string sql3 = "select * from Persons";
            ExecuteResponse execResponse3 = await client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse3.StatementId);
            pbc::RepeatedField<Row> rows = execResponse3.Results[0].FirstFrame.Rows;
            for (int i = 0; i < rows.Count; i++)
            {
                Row row = rows[i];
                Debug.WriteLine(row.Value[0].Value[0].StringValue + " " + row.Value[1].Value[0].StringValue);
            }
            // Closing statement 3
            CloseStatementResponse closeStatementResponse3 = await client.CloseStatementRequestAsync(connId, createStatementResponse3.StatementId);

            // Creating statement 4
            CreateStatementResponse createStatementResponse4 = await client.CreateStatementRequestAsync(connId);
            // Running query 4
            string sql4 = "DROP TABLE Persons";
            ExecuteResponse execResponse4 = await client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse4.StatementId);
            // Closing statement 4
            CloseStatementResponse closeStatementResponse4 = await client.CloseStatementRequestAsync(connId, createStatementResponse4.StatementId);

            // Commit statement 4
            await client.CommitRequestAsync(connId);

            // Closing connection
            CloseConnectionResponse closeConnResponse = await client.CloseConnectionRequestAsync(connId);
        }
    }




}
