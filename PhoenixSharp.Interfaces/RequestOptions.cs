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
    using System.Collections.Generic;

    public class RequestOptions
    {
        //public RetryPolicy RetryPolicy { get; set; }
        public string AlternativeEndpoint { get; set; }
        public bool KeepAlive { get; set; }
        public int TimeoutMillis { get; set; }
        //public int SerializationBufferSize { get; set; }
        //public int ReceiveBufferSize { get; set; }
        public bool UseNagle { get; set; }
        public int Port { get; set; }
        public Dictionary<string, string> AdditionalHeaders { get; set; }
        public string AlternativeHost { get; set; }
        public bool Pipelined { get; set; }

        public void Validate()
        {

        }

        public static RequestOptions GetGatewayDefaultOptions()
        {
            return new RequestOptions()
            {
                //RetryPolicy = RetryPolicy.DefaultExponential,
                KeepAlive = true,
                TimeoutMillis = 30000,
                //ReceiveBufferSize = 1024 * 1024 * 1,
                //SerializationBufferSize = 1024 * 1024 * 1,
                UseNagle = false,
                AlternativeEndpoint = null,
                Port = 443,
                AlternativeHost = null,
                Pipelined = false
            };
        }

        public static RequestOptions GetVNetDefaultOptions()
        {
            return new RequestOptions()
            {
                //RetryPolicy = RetryPolicy.DefaultExponential,
                KeepAlive = true,
                TimeoutMillis = 30000,
                //ReceiveBufferSize = 1024 * 1024 * 1,
                //SerializationBufferSize = 1024 * 1024 * 1,
                UseNagle = false,
                AlternativeEndpoint = null,
                Port = 8765,
                AlternativeHost = null,
                Pipelined = false
            };
        }
    }
}
