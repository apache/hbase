/**
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
package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestRpcClientLeaks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcClientLeaks.class);

  @Rule
  public TestName name = new TestName();

  private static BlockingQueue<Socket> SAVED_SOCKETS = new LinkedBlockingQueue<>();

  public static class MyRpcClientImpl extends BlockingRpcClient {

    // Exceptions thrown only when this is set to false.
    private static boolean throwException = false;

    public MyRpcClientImpl(Configuration conf) {
      super(conf);
    }

    public MyRpcClientImpl(Configuration conf, String clusterId, SocketAddress address,
        MetricsConnection metrics) {
      super(conf, clusterId, address, metrics);
    }

    @Override
    protected BlockingRpcConnection createConnection(ConnectionId remoteId) throws IOException {
      return new BlockingRpcConnection(this, remoteId) {
        @Override
        protected synchronized void setupConnection() throws IOException {
          super.setupConnection();
          if (throwException) {
            SAVED_SOCKETS.add(socket);
            throw new IOException(
                "Sample exception for verifying socket closure in case of exceptions.");
          }
        }
      };
    }

    public static void enableThrowExceptions() {
      throwException = true;
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  public static final Logger LOG = LoggerFactory.getLogger(TestRpcClientLeaks.class);

  @Test
  public void testSocketClosed() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, fam1).close();

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, MyRpcClientImpl.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection connection = ConnectionFactory.createConnection(conf);
      Table table = connection.getTable(TableName.valueOf(name.getMethodName()))) {
      MyRpcClientImpl.enableThrowExceptions();
      table.get(new Get(Bytes.toBytes("asd")));
      fail("Should fail because the injected error");
    } catch (RetriesExhaustedException e) {
      // expected
    }
    for (Socket socket : SAVED_SOCKETS) {
      assertTrue("Socket " + socket + " is not closed", socket.isClosed());
    }
  }
}
