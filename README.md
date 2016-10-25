## Microsoft .NET driver for Apache Phoenix Query Server

This is C# client library for Phoenix Query Server on Azure HDInsight. It currently targets HBase 1.1.2, Phoenix 4.4.0 and HDInsight 3.4 and later versions on Microsoft Azure. The communication works through Avatica APIs which uses Protocol Buffers as a serialization format.

### Getting Started

* [Avatica Protocol Buffers Reference](https://calcite.apache.org/docs/avatica_protobuf_reference.html) - Although Phoenix query server supports both Protocol Buffers and JSON to serialize its requests, this driver will only support Protocol Buffers. This page demonstrates all the Avatica APIs for reference.
* [Getting Started with Linux HBase Clusters](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-tutorial-get-started-linux/) - Phoenix query server along with Phoenix will be available in HDInsight Linux-based HBase clusters. This documentation article walks you through the steps to create Linux HBase clusters on Azure.

### Build
Import the solution file into VS2013 and compile. Retrieve the resulting *.dll files.

Here is the [nuget package](https://www.nuget.org/packages/Microsoft.Phoenix.Client)

NOTE: 2.0.0 will not work on HDI 3.4. HDI 3.5 includes Avatica 1.8.0 and Phoenix 4.7 which include some breaking changes. CALCITE-1458 will fix the incompatibility and 1.2.0 will work in the hotfixed release. However, since those APIs will eventually deprecate. 2.0.0 will not support those deprecate APIs.

More examples about how to use the SDK will be published on Azure websites soon. 

### Usage
* hdinsight-phoenix-sharp/PhoenixSharp.UnitTests/PhoenixClientTests.cs would be a good example to learn how to use the APIs.

* [Twitter streaming example](https://github.com/duoxu/tweet-sentiment-phoenix) - I recently updated the twitter streaming example from [HDInsight tutorial](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-analyze-twitter-sentiment/) with the PQS .NET APIs. Please check it out.
