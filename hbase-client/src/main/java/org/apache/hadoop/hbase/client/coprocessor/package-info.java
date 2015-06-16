/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
Provides client classes for invoking Coprocessor RPC protocols

<p>
<ul>
 <li><a href="#overview">Overview</a></li>
 <li><a href="#usage">Example Usage</a></li>
</ul>
</p>

<h2><a name="overview">Overview</a></h2>
<p>
The coprocessor framework provides a way for custom code to run in place on the
HBase region servers with each of a table's regions.  These client classes
enable applications to communicate with coprocessor instances via custom RPC
protocols.
</p>

<p>
In order to provide a custom RPC protocol to clients, a coprocessor implementation
must:
<ul>
 <li>Define a protocol buffer Service and supporting Message types for the RPC methods.
 See the
 <a href="https://developers.google.com/protocol-buffers/docs/proto#services">protocol buffer guide</a>
 for more details on defining services.</li>
 <li>Generate the Service and Message code using the protoc compiler</li>
 <li>Implement the generated Service interface in your coprocessor class and implement the
 {@link org.apache.hadoop.hbase.coprocessor.CoprocessorService} interface.  The
 {@link org.apache.hadoop.hbase.coprocessor.CoprocessorService#getService()}
 method should return a reference to the Endpoint's protocol buffer Service instance.
</ul>
Clients may then call the defined service methods on coprocessor instances via
the {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])},
{@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}, and
{@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
methods.
</p>

<p>
Since coprocessor Service instances are associated with individual regions within the table,
the client RPC calls must ultimately identify which regions should be used in the Service
method invocations.  Since regions are seldom handled directly in client code
and the region names may change over time, the coprocessor RPC calls use row keys
to identify which regions should be used for the method invocations.  Clients
can call coprocessor Service methods against either:
<ul>
 <li><strong>a single region</strong> - calling
   {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])}
   with a single row key.  This returns a {@link org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel}
   instance which communicates with the region containing the given row key (even if the
   row does not exist) as the RPC endpoint.  Clients can then use the {@code CoprocessorRpcChannel}
   instance in creating a new Service stub to call RPC methods on the region's coprocessor.</li>
 <li><strong>a range of regions</strong> - calling
   {@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}
   or {@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
   with a starting row key and an ending row key.  All regions in the table
   from the region containing the start row key to the region containing the end
   row key (inclusive), will we used as the RPC endpoints.</li>
</ul>
</p>

<p><em>Note that the row keys passed as parameters to the <code>Table</code>
methods are not passed directly to the coprocessor Service implementations.
They are only used to identify the regions for endpoints of the remote calls.
</em></p>

<p>
The {@link org.apache.hadoop.hbase.client.coprocessor.Batch} class defines two
interfaces used for coprocessor Service invocations against multiple regions.  Clients implement
{@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} to call methods of the actual
coprocessor Service instance.  The interface's <code>call()</code> method will be called once
per selected region, passing the Service instance for the region as a parameter.  Clients
can optionally implement {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback}
to be notified of the results from each region invocation as they complete.
The instance's {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
method will be called with the {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
return value from each region.
</p>

<h2><a name="usage">Example usage</a></h2>
<p>
To start with, let's use a fictitious coprocessor, <code>RowCountEndpoint</code>
that counts the number of rows and key-values in each region where it is running.
For clients to query this information, the coprocessor defines the following protocol buffer
service:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
message CountRequest {
}

message CountResponse {
  required int64 count = 1 [default = 0];
}

service RowCountService {
  rpc getRowCount(CountRequest)
    returns (CountResponse);
  rpc getKeyValueCount(CountRequest)
    returns (CountResponse);
}
</pre></blockquote></div>

<p>
Next run the protoc compiler on the .proto file to generate Java code for the Service interface.
The generated {@code RowCountService} interface should look something like:
</p>
<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
public static abstract class RowCountService
  implements com.google.protobuf.Service {
  ...
  public interface Interface {
    public abstract void getRowCount(
        com.google.protobuf.RpcController controller,
        org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos.CountRequest request,
        com.google.protobuf.RpcCallback<org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos.CountResponse> done);

    public abstract void getKeyValueCount(
        com.google.protobuf.RpcController controller,
        org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos.CountRequest request,
        com.google.protobuf.RpcCallback<org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos.CountResponse> done);
  }
}
</pre></blockquote></div>

<p>
Our coprocessor Service will need to implement this interface and the {@link org.apache.hadoop.hbase.coprocessor.CoprocessorService}
in order to be registered correctly as an endpoint.  For the sake of simplicity the server-side
implementation is omitted.  To see the implementing code, please see the
{@link org.apache.hadoop.hbase.coprocessor.example.RowCountEndpoint} class in the HBase source code.
</p>

<p>
Now we need a way to access the results that <code>RowCountService</code>
is making available.  If we want to find the row count for all regions, we could
use:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
Connection connection = ConnectionFactory.createConnection(conf);
Table table = connection.getTable(TableName.valueOf("mytable"));
final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
Map<byte[],Long> results = table.coprocessorService(
    ExampleProtos.RowCountService.class, // the protocol interface we're invoking
    null, null,                          // start and end row keys
    new Batch.Call<ExampleProtos.RowCountService,Long>() {
        public Long call(ExampleProtos.RowCountService counter) throws IOException {
          BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
              new BlockingRpcCallback<ExampleProtos.CountResponse>();
          counter.getRowCount(null, request, rpcCallback);
          ExampleProtos.CountResponse response = rpcCallback.get();
          return response.hasCount() ? response.getCount() : 0;
        }
    });
</pre></blockquote></div>

<p>
This will return a <code>java.util.Map</code> of the <code>counter.getRowCount()</code>
result for the <code>RowCountService</code> instance running in each region
of <code>mytable</code>, keyed by the region name.
</p>

<p>
By implementing {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
as an anonymous class, we can invoke <code>RowCountService</code> methods
directly against the {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
method's argument.  Calling {@link org.apache.hadoop.hbase.client.Table#coprocessorService(Class, byte[], byte[], org.apache.hadoop.hbase.client.coprocessor.Batch.Call)}
will take care of invoking <code>Batch.Call.call()</code> against our anonymous class
with the <code>RowCountService</code> instance for each table region.
</p>

<p>
Implementing {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call} also allows you to
perform additional processing against each region's Service instance.  For example, if you would
like to combine row count and key-value count for each region:
</p>

<div style="background-color: #cccccc; padding: 2px">
<blockquote><pre>
Connection connection = ConnectionFactory.createConnection(conf);
Table table = connection.getTable(TableName.valueOf("mytable"));
// combine row count and kv count for region
final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
Map<byte[],Long> results = table.coprocessorService(
    ExampleProtos.RowCountService.class, // the protocol interface we're invoking
    null, null,                          // start and end row keys
    new Batch.Call<ExampleProtos.RowCountService,Pair<Long,Long>>() {
       public Long call(ExampleProtos.RowCountService counter) throws IOException {
         BlockingRpcCallback<ExampleProtos.CountResponse> rowCallback =
             new BlockingRpcCallback<ExampleProtos.CountResponse>();
         counter.getRowCount(null, request, rowCallback);

         BlockingRpcCallback<ExampleProtos.CountResponse> kvCallback =
             new BlockingRpcCallback<ExampleProtos.CountResponse>();
         counter.getKeyValueCount(null, request, kvCallback);

         ExampleProtos.CountResponse rowResponse = rowCallback.get();
         ExampleProtos.CountResponse kvResponse = kvCallback.get();
         return new Pair(rowResponse.hasCount() ? rowResponse.getCount() : 0,
             kvResponse.hasCount() ? kvResponse.getCount() : 0);
    }
});
</pre></blockquote></div>
*/
package org.apache.hadoop.hbase.client.coprocessor;
