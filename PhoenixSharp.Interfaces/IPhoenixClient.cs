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

namespace PhoenixSharp.Interfaces
{
    using System.Threading.Tasks;
    using Apache.Phoenix;
    using pbc = Google.Protobuf.Collections;

    public interface IPhoenixClient
    {
        Task<ExecuteResponse> PrepareAndExecuteRequestAsync(string connectionId, string sql, ulong maxRowCount, uint statementId, RequestOptions options);
        Task<OpenConnectionResponse> OpenConnectionRequestAsync(string connectionId, RequestOptions options);
        Task<ConnectionSyncResponse> ConnectionSyncRequestAsync(string connectionId, ConnectionProperties props, RequestOptions options);
        Task<CreateStatementResponse> CreateStatementRequestAsync(string connectionId, RequestOptions options);
        Task<CloseStatementResponse> CloseStatementRequestAsync(string connectionId, uint statementId, RequestOptions options);
        Task<CloseConnectionResponse> CloseConnectionRequestAsync(string connectionId, RequestOptions options);
        Task<CommitResponse> CommitRequestAsync(string connectionId, RequestOptions options);
        Task<RollbackResponse> RollbackRequestAsync(string connectionId, RequestOptions options);
        Task<PrepareResponse> PrepareRequestAsync(string connectionId, string sql, ulong maxRowCount, RequestOptions options);
        Task<ExecuteResponse> ExecuteRequestAsync(StatementHandle statementHandle, pbc::RepeatedField<TypedValue> parameterValues, ulong maxRowCount, bool hasParameterValues, RequestOptions options);
        Task<ResultSetResponse> TablesRequestAsync(string catalog, string schemaPattern, string tableNamePattern, pbc::RepeatedField<string> typeList, bool hasTypeList, string connectionId, RequestOptions options);
        Task<ResultSetResponse> CatalogsRequestAsync(string connectionId, RequestOptions options);
        Task<ResultSetResponse> TableTypesRequestAsync(string connectionId, RequestOptions options);
        Task<ResultSetResponse> SchemasRequestAsync(string catalog, string schemaPattern, string connectionId, RequestOptions options);
        Task<ResultSetResponse> ColumnsRequestAsync(string catalog, string schemaPattern, string tableNamePattern, string columnNamePattern, string connectionId, RequestOptions options);
        Task<FetchResponse> FetchRequestAsync(string connectionId, uint statementId, ulong offset, uint fetchMaxRowCount, RequestOptions options);
        Task<ExecuteBatchResponse> PrepareAndExecuteBatchRequestAsync(string connectionId, uint statementId, pbc::RepeatedField<string> sqlCommands, RequestOptions options);
        Task<ExecuteBatchResponse> ExecuteBatchRequestAsync(string connectionId, uint statementId, pbc::RepeatedField<UpdateBatch> updates, RequestOptions options);
    }
}
