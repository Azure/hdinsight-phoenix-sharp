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
    using System.Threading;
    using System.Threading.Tasks;
    using Interfaces;
    using PhoenixSharp.Internal.Helpers;

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
            Debug.WriteLine("Issuing request to endpoint {0}", builder.Uri);
            HttpWebRequest httpWebRequest = WebRequest.CreateHttp(builder.Uri);
            //httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
            httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
            httpWebRequest.Timeout = options.TimeoutMillis;
            httpWebRequest.KeepAlive = options.KeepAlive;
            httpWebRequest.Credentials = null;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;
            httpWebRequest.Pipelined = options.Pipelined;

            if (options.AdditionalHeaders != null)
            {
                foreach (var kv in options.AdditionalHeaders)
                {
                    httpWebRequest.Headers.Add(kv.Key, kv.Value);
                }
            }

            long remainingTime = options.TimeoutMillis;
            if (input != null)
            {
                // expecting the caller to seek to the beginning or to the location where it needs to be copied from
                Stream req = null;
                try
                {
                    req = await httpWebRequest.GetRequestStreamAsync().WithTimeout(
                                                TimeSpan.FromMilliseconds(remainingTime),
                                                "Waiting for RequestStream");

                    remainingTime = options.TimeoutMillis - watch.ElapsedMilliseconds;
                    if (remainingTime <= 0)
                    {
                        remainingTime = 0;
                    }

                    await input.CopyToAsync(req).WithTimeout(
                                TimeSpan.FromMilliseconds(remainingTime),
                                "Waiting for CopyToAsync",
                                CancellationToken.None);
                }
                catch (TimeoutException)
                {
                    httpWebRequest.Abort();
                    throw;
                }
                finally
                {
                    if (req != null)
                    {
                        req.Close();
                    }
                }
            }

            try
            {
                remainingTime = options.TimeoutMillis - watch.ElapsedMilliseconds;
                if (remainingTime <= 0)
                {
                    remainingTime = 0;
                }

                HttpWebResponse response = (HttpWebResponse)await httpWebRequest.GetResponseAsync().WithTimeout(
                                                                TimeSpan.FromMilliseconds(remainingTime),
                                                                "Waiting for GetResponseAsync");
                return new Response()
                {
                    WebResponse = response,
                    RequestLatency = watch.Elapsed
                };
            }
            catch (Exception ex)
            {
                if (ex is WebException)
                {
                    WebException we = (WebException)ex;
                    var resp = we.Response as HttpWebResponse;
                    return new Response()
                    {
                        WebResponse = resp,
                        RequestLatency = watch.Elapsed
                    };
                }
                else
                {
                    httpWebRequest.Abort();
                    throw;
                }
            }

        }
    }
}
