﻿// Copyright (c) Microsoft Corporation
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

namespace PhoenixSharp.UnitTests.Utilities
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Provides common services for BDD-style (context/specification) unit tests.  
    /// Serves as an adapter between the MSTest framework and our BDD-style tests.
    /// </summary>
    //// Borrowed from the blog of SaintGimp (with permission).
    [TestClass]
    public abstract class ContextSpecification : TestBase
    {
        /// <summary>
        /// Sets up the environment for a specification context.
        /// </summary>
        protected virtual void Context()
        {
        }

        /// <summary>
        /// Acts on the context to create the observable condition.
        /// </summary>
        protected virtual void BecauseOf()
        {
        }

        /// <inheritdoc/>
        [TestInitialize]
        public override void TestInitialize()
        {
            base.TestInitialize();

            Context();
            BecauseOf();
        }
    }
}
