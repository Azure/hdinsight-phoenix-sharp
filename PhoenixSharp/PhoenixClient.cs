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
    using PhoenixSharp.Interfaces;
    using PhoenixSharp.Internal;
    using PhoenixSharp.Requester;
    using PhoenixSharp.Internal.Extensions;
    using System.Threading.Tasks;
    using Apache.Phoenix;
    using System.Net;
    using System.IO;
    using Google.Protobuf;
    using System;
    using pbc = global::Google.Protobuf.Collections;
    using System.Collections.Generic;

    public class PhoenixClient : IPhoenixClient
    {
        private readonly IWebRequester _requester;
        private readonly RequestOptions _globalRequestOptions;

        public PhoenixClient(ClusterCredentials credentials)
            : this(credentials, RequestOptions.GetDefaultOptions())
        {
        }

        public PhoenixClient(ClusterCredentials credentials, RequestOptions globalRequestOptions = null)
        {
            _globalRequestOptions = globalRequestOptions ?? RequestOptions.GetDefaultOptions();
            _globalRequestOptions.Validate();
            if (credentials != null) // gateway mode
            {
                _requester = new GatewayWebRequester(credentials);
            }
            else // vnet mode
            {
                _requester = new VNetWebRequester();
            }
        }

        public async Task<ExecuteResponse> PrepareAndExecuteRequestAsync(string connectionId, string sql, ulong maxRowCount, uint statementId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<OpenConnectionResponse> OpenConnectionRequestAsync(string connectionId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<ConnectionSyncResponse> ConnectionSyncRequestAsync(string connectionId, ConnectionProperties props, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<CreateStatementResponse> CreateStatementRequestAsync(string connectionId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<CloseStatementResponse> CloseStatementRequestAsync(string connectionId, uint statementId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<CloseConnectionResponse> CloseConnectionRequestAsync(string connectionId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<CommitResponse> CommitRequestAsync(string connectionId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<RollbackResponse> RollbackRequestAsync(string connectionId, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<PrepareResponse> PrepareRequestAsync(string connectionId, string sql, ulong maxRowCount, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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

        public async Task<ExecuteResponse> ExecuteRequestAsync(StatementHandle statementHandle, pbc::RepeatedField<TypedValue> parameterValues, ulong maxRowCount, bool hasParameterValues, RequestOptions options = null)
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

            var optionToUse = options ?? _globalRequestOptions;

            using (Response webResponse = await PostRequestAsync(msg.ToByteArray(), optionToUse))
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
