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
    using System;
    using System.Security;
    using System.Threading;
    using System.Diagnostics.CodeAnalysis;
    using Internal.Extensions;

    public sealed class ClusterCredentials
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterCredentials"/> class.
        /// </summary>
        /// <param name="clusterUri">The cluster URI.</param>
        /// <param name="userName">The username.</param>
        /// <param name="password">The password.</param>
        public ClusterCredentials(Uri clusterUri, string userName, string password)
        {
            ClusterUri = clusterUri;
            UserName = userName;
            Password = password;
        }

        /// <summary>
        /// Gets the cluster URI.
        /// </summary>
        /// <value>
        /// The cluster URI.
        /// </value>
        public Uri ClusterUri { get; private set; }

        /// <summary>
        /// Gets the name of the user.
        /// </summary>
        /// <value>
        /// The name of the user.
        /// </value>
        public string UserName { get; private set; }

        /// <summary>
        /// Gets the password of the user.
        /// </summary>
        /// <value>
        /// The password of the user.
        /// </value>
        public string Password { get; private set; }
    }
}
