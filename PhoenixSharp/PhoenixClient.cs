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

namespace PhoenixSharp
{
    using Interfaces;
    using Requester;
    using System.Threading.Tasks;
    using Apache.Phoenix;
    using System.Net;
    using System.IO;
    using Google.Protobuf;
    using pbc = Google.Protobuf.Collections;

    public class PhoenixClient : IPhoenixClient
    {
        private readonly IWebRequester _requester;

        /// <summary>
        /// Initializes a new instance of the <see cref="PhoenixClient"/> class. If the client is used
        /// within a VNET or on an on-premise cluster, no cluster credentials required. Pass null in 
        /// this case.
        /// </summary>
        public PhoenixClient(ClusterCredentials credentials)
        {
            if (credentials != null) // gateway mode
            {
                _requester = new GatewayWebRequester(credentials);
            }
            else // vnet mode
            {
                _requester = new VNetWebRequester();
            }
        }

        /// <summary>
        /// This request is used as a short-hand for create a Statement and fetching the first batch 
        /// of results in a single call without any parameter substitution.
        /// </summary>
        public async Task<ExecuteResponse> PrepareAndExecuteRequestAsync(string connectionId, string sql, ulong maxRowCount, uint statementId, RequestOptions options)
        {
            PrepareAndExecuteRequest req = new PrepareAndExecuteRequest
            {
                Sql = sql,
                ConnectionId = connectionId,
                StatementId = statementId,
                MaxRowCount = maxRowCount
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "PrepareAndExecuteRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "PrepareAndExecuteRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ExecuteResponse res = ExecuteResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to open a new Connection in the Phoenix query server.
        /// </summary>
        public async Task<OpenConnectionResponse> OpenConnectionRequestAsync(string connectionId, RequestOptions options)
        {
            OpenConnectionRequest req = new OpenConnectionRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "OpenConnectionRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "OpenConnectionRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    OpenConnectionResponse res = OpenConnectionResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to ensure that the client and server have a consistent view of the database properties.
        /// </summary>
        public async Task<ConnectionSyncResponse> ConnectionSyncRequestAsync(string connectionId, ConnectionProperties props, RequestOptions options)
        {
            ConnectionSyncRequest req = new ConnectionSyncRequest
            {
                ConnectionId = connectionId,
                ConnProps = props
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "ConnectionSyncRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "ConnectionSyncRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ConnectionSyncResponse res = ConnectionSyncResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to create a new Statement in the Phoenix query server.
        /// </summary>
        public async Task<CreateStatementResponse> CreateStatementRequestAsync(string connectionId, RequestOptions options)
        {
            CreateStatementRequest req = new CreateStatementRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "CreateStatementRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "CreateStatementRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    CreateStatementResponse res = CreateStatementResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to close the Statement object in the Phoenix query server identified by the given IDs.
        /// </summary>
        public async Task<CloseStatementResponse> CloseStatementRequestAsync(string connectionId, uint statementId, RequestOptions options)
        {
            CloseStatementRequest req = new CloseStatementRequest
            {
                ConnectionId = connectionId,
                StatementId = statementId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "CloseStatementRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "CloseStatementRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    CloseStatementResponse res = CloseStatementResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to close the Connection object in the Phoenix query server identified by the given IDs.
        /// </summary>
        public async Task<CloseConnectionResponse> CloseConnectionRequestAsync(string connectionId, RequestOptions options)
        {
            CloseConnectionRequest req = new CloseConnectionRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "CloseConnectionRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "CloseConnectionRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    CloseConnectionResponse res = CloseConnectionResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to issue a commit on the Connection in the Phoenix query server identified by the given ID.
        /// </summary>
        public async Task<CommitResponse> CommitRequestAsync(string connectionId, RequestOptions options)
        {
            CommitRequest req = new CommitRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "CommitRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "CommitRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    CommitResponse res = CommitResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to issue a rollback on the Connection in the Phoenix query server identified by the given ID.
        /// </summary>
        public async Task<RollbackResponse> RollbackRequestAsync(string connectionId, RequestOptions options)
        {
            RollbackRequest req = new RollbackRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "RollbackRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "RollbackRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    RollbackResponse res = RollbackResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to create create a new Statement with the given query in the Phoenix query server.
        /// </summary>
        public async Task<PrepareResponse> PrepareRequestAsync(string connectionId, string sql, ulong maxRowCount, RequestOptions options)
        {
            PrepareRequest req = new PrepareRequest
            {
                ConnectionId = connectionId,
                Sql = sql,
                MaxRowCount = maxRowCount
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "PrepareRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "PrepareRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    PrepareResponse res = PrepareResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to execute a PreparedStatement, optionally with values to bind to the parameters in the Statement.
        /// </summary>
        public async Task<ExecuteResponse> ExecuteRequestAsync(StatementHandle statementHandle, pbc::RepeatedField<TypedValue> parameterValues, ulong maxRowCount, bool hasParameterValues, RequestOptions options)
        {
            ExecuteRequest req = new ExecuteRequest
            {
                StatementHandle = statementHandle,
                ParameterValues = parameterValues,
                MaxRowCount = maxRowCount,
                HasParameterValues = hasParameterValues
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "ExecuteRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "ExecuteRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              statementHandle.ConnectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ExecuteResponse res = ExecuteResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to fetch the tables available in this database filtered by the provided criteria.
        /// </summary>
        public async Task<ResultSetResponse> TablesRequestAsync(string catalog, string schemaPattern, string tableNamePattern, pbc::RepeatedField<string> typeList, bool hasTypeList, string connectionId, RequestOptions options)
        {
            TablesRequest req = new TablesRequest
            {
                Catalog = catalog,
                SchemaPattern = schemaPattern,
                TableNamePattern = tableNamePattern,
                TypeList = typeList,
                HasTypeList = hasTypeList,
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "TablesRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "TablesRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ResultSetResponse res = ResultSetResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to fetch the available catalog names in the database.
        /// </summary>
        public async Task<ResultSetResponse> CatalogsRequestAsync(string connectionId, RequestOptions options)
        {
            CatalogsRequest req = new CatalogsRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "CatalogsRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "CatalogsRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ResultSetResponse res = ResultSetResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to fetch the table types available in this database.
        /// </summary>
        public async Task<ResultSetResponse> TableTypesRequestAsync(string connectionId, RequestOptions options)
        {
            TableTypesRequest req = new TableTypesRequest
            {
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "TableTypesRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "TableTypesRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ResultSetResponse res = ResultSetResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to fetch the table types available in this database.
        /// </summary>
        public async Task<ResultSetResponse> SchemasRequestAsync(string catalog, string schemaPattern, string connectionId, RequestOptions options)
        {
            SchemasRequest req = new SchemasRequest
            {
                Catalog = catalog,
                SchemaPattern = schemaPattern,
                ConnectionId = connectionId
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "SchemasRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "SchemasRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    ResultSetResponse res = ResultSetResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        /// <summary>
        /// This request is used to fetch a batch of rows from a Statement previously created.
        /// </summary>
        public async Task<FetchResponse> FetchRequestAsync(string connectionId, uint statementId, ulong offset, uint fetchMaxRowCount, RequestOptions options)
        {
            FetchRequest req = new FetchRequest
            {
                ConnectionId = connectionId,
                StatementId = statementId,
                Offset = offset,
                FetchMaxRowCount = fetchMaxRowCount
            };

            WireMessage msg = new WireMessage
            {
                Name = Constants.WireMessagePrefix + "FetchRequest",
                WrappedMessage = req.ToByteString()
            };

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), options))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.WebResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "FetchRequestAsync failed! connectionId: {0}, Response code was: {1}, Response body was: {2}",
                              connectionId,
                              webResponse.WebResponse.StatusCode,
                              message));
                    }
                }
                else
                {
                    WireMessage output = WireMessage.Parser.ParseFrom(webResponse.WebResponse.GetResponseStream());
                    FetchResponse res = FetchResponse.Parser.ParseFrom(output.WrappedMessage);
                    return res;
                }
            }
        }

        private async Task<Response> PostRequestAsync(byte[] request, RequestOptions options)
        {
            return await ExecuteMethodAsync("POST", request, options);
        }

        private async Task<Response> ExecuteMethodAsync(
            string method,
            byte[] request,
            RequestOptions options)
        {
            using (var input = new MemoryStream(request))
            {
                return await _requester.IssueWebRequestAsync(method: method, input: input, options: options);
            }
        }

    }
}
