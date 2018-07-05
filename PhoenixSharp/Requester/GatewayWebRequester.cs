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
    using PhoenixSharp.Interfaces;
    using PhoenixSharp.Internal.Helpers;

    /// <summary>
    /// 
    /// </summary>
    public sealed class GatewayWebRequester : IWebRequester
    {
        private readonly string _contentType;
        private readonly CredentialCache _credentialCache;
        private readonly ClusterCredentials _credentials;

        /// <summary>
        /// Initializes a new instance of the <see cref="GatewayWebRequester"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="contentType">Type of the content.</param>
        public GatewayWebRequester(ClusterCredentials credentials, string contentType = "application/x-google-protobuf")
        {
            _credentials = credentials;
            _contentType = contentType;
            _credentialCache = new CredentialCache();
            InitCache();
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
                _credentials.ClusterUri.Scheme,
                _credentials.ClusterUri.Host,
                options.Port,
                options.AlternativeEndpoint);
            Debug.WriteLine("Issuing request to endpoint {0}", builder.Uri);
            HttpWebRequest httpWebRequest = WebRequest.CreateHttp(builder.Uri);
            //httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
            httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
            httpWebRequest.Timeout = options.TimeoutMillis;
            httpWebRequest.KeepAlive = options.KeepAlive;
            httpWebRequest.Credentials = _credentialCache;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;
            httpWebRequest.Pipelined = options.Pipelined;
            // ignore certificates
            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => { return true; };

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

        private void InitCache()
        {
            _credentialCache.Add(_credentials.ClusterUri, "Basic", new NetworkCredential(_credentials.UserName, _credentials.Password));
        }
    }
}
