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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using PhoenixSharp;
    using Apache.Phoenix;
    using pbc = Google.Protobuf.Collections;
    using Interfaces;

    [TestClass]
    public class VNetTests
    {
        [TestMethod]
        public void TableOperationTest()
        {
            var client = new PhoenixClient(null);
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetVNetDefaultOptions();
            // In VNET mode, PQS requests will be http://<PQS workernode ip>:8765
            options.AlternativeHost = "10.17.0.13";
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

        private string GenerateRandomConnId()
        {
            const string hex_characters = "0123456789abcdef";
            var random = new Random();
            // Generating a random connection ID
            return new string(Enumerable.Repeat(hex_characters, 8).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}