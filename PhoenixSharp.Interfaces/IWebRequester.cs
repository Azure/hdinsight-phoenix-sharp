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
    using System;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    public interface IWebRequester
    {
        Task<Response> IssueWebRequestAsync(string method, Stream input, RequestOptions options);
    }

    public class Response : IDisposable
    {
        public HttpWebResponse WebResponse { get; set; }
        public TimeSpan RequestLatency { get; set; }
        public Action<Response> PostRequestAction { get; set; }

        public void Dispose()
        {
            if (PostRequestAction != null)
            {
                PostRequestAction(this);
            }

            if (null != WebResponse)
            {
                WebResponse.Dispose();
            }
        }
    }
}
