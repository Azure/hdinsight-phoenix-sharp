using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using PhoenixSharp.Interfaces;
using Apache.Phoenix;
using pbc = Google.Protobuf.Collections;
using System.Net;

namespace PhoenixSharp.UnitTests
{
    class ConnectionRetryTests:PhoenixClientTests
    {
        [Test]
        public void TestRetry()
        {
            var client = new PhoenixClient(_credentials);

            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
            options.AlternativeEndpoint = "hbasephoenix0/";

            var attempts = 0;
            do
            {
                try
                {
                    attempts++;
                    string connId = GenerateRandomConnId();
                    string tableName = "Persons" + connId;
                    OpenConnectionResponse openConnResponse = null;
                    CreateStatementResponse createStatementResponse = null;
                    pbc::MapField<string, string> info = new pbc::MapField<string, string>();
                    openConnResponse = client.OpenConnectionRequestAsync(connId, info, options).Result;
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
                    break;
                }
                catch (Exception ex)
                {
                    if (ex.GetBaseException().Message.Contains("NoSuchConnectionException"))
                    {
                        continue;
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }
            } while (attempts < 10);

            if(attempts == 10)
            {
                Assert.Fail();
            }
        }
    }
}
