using System;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using Apache.Phoenix;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using NUnit.Framework;
using PhoenixSharp.Interfaces;
using pbc = Google.Protobuf.Collections;

namespace PhoenixSharp.UnitTests
{
    public class ConnectionRetryTests
    {
        private const string TABLE_NAME = "TestTable";
        private RequestOptions _options;
        private PhoenixClient _client;
        private string _connId;
        private const int RETRYCOUNT = 5;
        private int _retryCounter;

        [SetUp]
        public void Setup()
        {
            _client = new PhoenixClient(null);
            _connId = Guid.NewGuid().ToString();
            _options = RequestOptions.GetVNetDefaultOptions();

            OpenConnectionResponse openConnResponse = null;
            
            _retryCounter = 0;
            var retryCount = 5;
            var minBackoff = new TimeSpan(0, 0, 0, 0, 5);
            var maxBackoff = new TimeSpan(0, 0, 0, 0, 1000);
            var interval = new TimeSpan(0, 0, 0, 0, 200);
            var exponentialBackoff = new ExponentialBackoff(retryCount, minBackoff, maxBackoff, interval);
            _options.RetryPolicy = new RetryPolicy<AllErrorsTransientDetectionStrategy>(exponentialBackoff);
            _options.AlternativeEndpoint = "10.255.255.1";
            _options.Port = 8765;
            _options.TimeoutMillis = 100;

            _options.RetryPolicy.Retrying += (sender, largs) =>
            {
                // Log details of the retry.
                var msg = String.Format("Retry - Count:{0}, Delay:{1}, Exception:{2}",
                    largs.CurrentRetryCount, largs.Delay, largs.LastException.GetType().ToString());
                System.Diagnostics.Debug.WriteLine(msg, "Information");

                _retryCounter++;
            };
        }

        /// <summary>
        /// Load config, Insert Rows and Read back rows
        /// </summary>
        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void TestRetry()
        {
            var info = new pbc::MapField<string, string>();
            _client.OpenConnectionRequestAsync(_connId, info, _options).Wait();
            Assert.AreEqual(RETRYCOUNT, _retryCounter);
        }
    }
}
