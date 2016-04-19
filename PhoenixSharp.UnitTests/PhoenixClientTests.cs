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
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using PhoenixSharp;
    using Apache.Phoenix;
    using Utilities;
    using pbc = Google.Protobuf.Collections;
    using System.Diagnostics;
    using Interfaces;

    [TestClass]
    public class PhoenixClientTests : DisposableContextSpecification
    {
        private ClusterCredentials _credentials;

        protected override void Context()
        {
            _credentials = ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt");
        }

        [TestMethod]
        public void TableOperationTest()
        {
            var client = new PhoenixClient(_credentials);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            OpenConnectionResponse openConnResponse = null;
            try
            {
                // Opening connection
                openConnResponse = client.OpenConnectionRequestAsync(connId, options).Result;
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
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();

                // List system tables
                pbc.RepeatedField<string> list = new pbc.RepeatedField<string>();
                list.Add("SYSTEM TABLE");
                ResultSetResponse tablesResponse = client.TablesRequestAsync("", "", "", list, true, connId, options).Result;
                Assert.AreEqual(4, tablesResponse.FirstFrame.Rows.Count);
                
                // List all table types
                ResultSetResponse tableTypeResponse = client.TableTypesRequestAsync(connId, options).Result;
                Assert.AreEqual(6, tableTypeResponse.FirstFrame.Rows.Count);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
        }

        [TestMethod]
        public void SimpleTest()
        {
            var client = new PhoenixClient(_credentials);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            string tableName = "Persons" + connId;
            OpenConnectionResponse openConnResponse = null;
            CreateStatementResponse createStatementResponse = null;
            try
            {
                // Opening connection
                openConnResponse = client.OpenConnectionRequestAsync(connId, options).Result;
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
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();


                createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;
                // Running query 1
                string sql1 = "CREATE TABLE " + tableName + " (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
                client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse.StatementId, options).Wait();

                // Running query 2
                string sql2 = "UPSERT INTO " + tableName + " VALUES ('duo','xu')";
                client.PrepareAndExecuteRequestAsync(connId, sql2, 100, createStatementResponse.StatementId, options).Wait();

                // Running query 3
                string sql3 = "select count(*) from " + tableName;
                ExecuteResponse execResponse3 = client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse.StatementId, options).Result;
                long count = execResponse3.Results[0].FirstFrame.Rows[0].Value[0].Value[0].NumberValue;
                Assert.AreEqual(1, count);

                // Running query 4
                string sql4 = "DROP TABLE " + tableName;
                client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse.StatementId, options).Wait();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                if (createStatementResponse != null)
                {
                    client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options).Wait();
                    createStatementResponse = null;
                }

                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
        }

        [TestMethod]
        public void ManyRowInsertTest()
        {
            var client = new PhoenixClient(_credentials);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            string tableName = "Persons" + connId;

            OpenConnectionResponse openConnResponse = null;
            CreateStatementResponse createStatementResponse = null;
            try
            {
                // Opening connection
                openConnResponse = client.OpenConnectionRequestAsync(connId, options).Result;
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
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();

                createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;
                // Running query 1
                string sql1 = "CREATE TABLE " + tableName + " (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
                ExecuteResponse execResponse1 = client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse.StatementId, options).Result;

                // Commit statement 1
                client.CommitRequestAsync(connId, options).Wait();

                // Creating statement 2
                string sql2 = "UPSERT INTO " + tableName + " VALUES (?,?)";
                PrepareResponse prepareResponse = client.PrepareRequestAsync(connId, sql2, 100, options).Result;
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
                    ExecuteResponse executeResponse = client.ExecuteRequestAsync(statementHandle, list, 100, true, options).Result;
                }

                // Commit statement 2
                client.CommitRequestAsync(connId, options).Wait();

                // Running query 3
                string sql3 = "select count(*) from " + tableName;
                ExecuteResponse execResponse3 = client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse.StatementId, options).Result;
                long count = execResponse3.Results[0].FirstFrame.Rows[0].Value[0].Value[0].NumberValue;
                Assert.AreEqual(10, count);

                // Running query 4
                string sql4 = "DROP TABLE " + tableName;
                client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse.StatementId, options).Wait();
               
                // Commit statement 4
                client.CommitRequestAsync(connId, options).Wait();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                if (createStatementResponse != null)
                {
                    client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options).Wait();
                    createStatementResponse = null;
                }

                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
        }

        [TestMethod]
        public void QueryManyRowTest()
        {
            var client = new PhoenixClient(_credentials);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            string tableName = "Persons" + connId;

            OpenConnectionResponse openConnResponse = null;
            CreateStatementResponse createStatementResponse = null;
            try
            {
                // Opening connection
                openConnResponse = client.OpenConnectionRequestAsync(connId, options).Result;
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
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();

                createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;
                // Running query 1
                string sql1 = "CREATE TABLE " + tableName + " (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
                client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse.StatementId, options).Wait();

                // Commit statement 1
                client.CommitRequestAsync(connId, options).Wait();

                // Creating statement 2
                string sql2 = "UPSERT INTO " + tableName + " VALUES (?,?)";
                PrepareResponse prepareResponse = client.PrepareRequestAsync(connId, sql2, 100, options).Result;
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
                    ExecuteResponse executeResponse = client.ExecuteRequestAsync(statementHandle, list, 100, true, options).Result;
                }

                // Commit statement 2
                client.CommitRequestAsync(connId, options).Wait();

                // Running query 3
                string sql3 = "select * from " + tableName;
                ExecuteResponse execResponse3 = client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse.StatementId, options).Result;
                pbc::RepeatedField<Row> rows = execResponse3.Results[0].FirstFrame.Rows;
                for (int i = 0; i < rows.Count; i++)
                {
                    Row row = rows[i];
                    Debug.WriteLine(row.Value[0].Value[0].StringValue + " " + row.Value[1].Value[0].StringValue);
                }

                // Running query 4
                string sql4 = "DROP TABLE " + tableName;
                client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse.StatementId, options).Wait();

                // Commit statement 4
                client.CommitRequestAsync(connId, options).Wait();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                if (createStatementResponse != null)
                {
                    client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options).Wait();
                    createStatementResponse = null;
                }

                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
        }

        [TestMethod]
        public void CommitRollbackTest()
        {
            var client = new PhoenixClient(_credentials);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            string tableName = "Persons" + connId;

            OpenConnectionResponse openConnResponse = null;
            CreateStatementResponse createStatementResponse = null;
            try
            {
                // Opening connection 1
                openConnResponse = client.OpenConnectionRequestAsync(connId, options).Result;
                // Syncing connection 1
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
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();

                createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;
                // Running query 1
                string sql1 = "CREATE TABLE " + tableName + " (LastName varchar(255) PRIMARY KEY,FirstName varchar(255))";
                client.PrepareAndExecuteRequestAsync(connId, sql1, 100, createStatementResponse.StatementId, options).Wait();

                // Commit statement 1
                client.CommitRequestAsync(connId, options).Wait();

                // Creating statement 2
                string sql2 = "UPSERT INTO " + tableName + " VALUES (?,?)";
                PrepareResponse prepareResponse = client.PrepareRequestAsync(connId, sql2, 100, options).Result;
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
                    ExecuteResponse executeResponse = client.ExecuteRequestAsync(statementHandle, list, 100, true, options).Result;
                }

                // Rollback
                client.RollbackRequestAsync(connId, options).Wait();

                // Commit statement 2
                client.CommitRequestAsync(connId, options).Wait();

                // Running query 3
                string sql3 = "select count(*) from " + tableName;
                ExecuteResponse execResponse3 = client.PrepareAndExecuteRequestAsync(connId, sql3, 100, createStatementResponse.StatementId, options).Result;
                long count3 = execResponse3.Results[0].FirstFrame.Rows[0].Value[0].Value[0].NumberValue;
                Assert.AreEqual(0, count3);

                // Running query 4
                string sql4 = "DROP TABLE " + tableName;
                client.PrepareAndExecuteRequestAsync(connId, sql4, 100, createStatementResponse.StatementId, options).Wait();

                // Commit statement 4
                client.CommitRequestAsync(connId, options).Wait();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                if (createStatementResponse != null)
                {
                    client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options).Wait();
                    createStatementResponse = null;
                }

                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
        }

        private string GenerateRandomConnId()
        {
            const string hex_characters = "0123456789abcdef";
            var random = new Random();
            // Generating a random connection ID
            return new string(Enumerable.Repeat(hex_characters, 8).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }




}
