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

namespace PhoenixSharp.Requester
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using Interfaces;

    /// <summary>
    /// 
    /// </summary>
    public sealed class VNetWebRequester : IWebRequester
    {
        private readonly string _contentType;

        /// <summary>
        /// Initializes a new instance of the <see cref="VNetWebRequester"/> class.
        /// </summary>
        /// <param name="contentType">Type of the content.</param>
        public VNetWebRequester(string contentType = "application/x-google-protobuf")
        {
            _contentType = contentType;
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(
            string method, Stream input, RequestOptions options)
        {
            options.Validate();
            Stopwatch watch = Stopwatch.StartNew();
            UriBuilder builder = new UriBuilder(
                "http",
                options.AlternativeHost,
                options.Port,
                options.AlternativeEndpoint);
            Debug.WriteLine("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, builder.Uri);
            HttpWebRequest httpWebRequest = WebRequest.CreateHttp(builder.Uri);
            httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
            httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
            httpWebRequest.Timeout = options.TimeoutMillis;
            httpWebRequest.KeepAlive = options.KeepAlive;
            httpWebRequest.Credentials = null;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;

            if (options.AdditionalHeaders != null)
            {
                foreach (var kv in options.AdditionalHeaders)
                {
                    httpWebRequest.Headers.Add(kv.Key, kv.Value);
                }
            }

            if (input != null)
            {
                // seek to the beginning, so we copy everything in this buffer
                input.Seek(0, SeekOrigin.Begin);
                using (Stream req = httpWebRequest.GetRequestStream())
                {
                    await input.CopyToAsync(req);
                }
            }

            var response = (await httpWebRequest.GetResponseAsync()) as HttpWebResponse;
            return new Response()
            {
                WebResponse = response,
                RequestLatency = watch.Elapsed
            };
        }
    }
}
