/*
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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.TestThriftServerCmdLine.createBoundServer;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

/**
 * Start the HBase Thrift HTTP server on a random port through the command-line
 * interface and talk to it from client side.
 */
@Category({ClientTests.class, LargeTests.class})
public class TestThriftHttpServer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftHttpServer.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestThriftHttpServer.class);

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);
    TEST_UTIL.startMiniCluster();
    //ensure that server time increments every time we do an operation, otherwise
    //successive puts having the same timestamp will override each other
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testExceptionThrownWhenMisConfigured() throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set("hbase.thrift.security.qop", "privacy");
    conf.setBoolean("hbase.thrift.ssl.enabled", false);
    ExpectedException thrown = ExpectedException.none();
    ThriftServerRunner tsr = null;
    try {
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Thrift HTTP Server's QoP is privacy, " +
          "but hbase.thrift.ssl.enabled is false");
      tsr = TestThriftServerCmdLine.createBoundServer(() -> new ThriftServer(conf));
      fail("Thrift HTTP Server starts up even with wrong security configurations.");
    } catch (Exception e) {
      LOG.info("Expected!", e);
    } finally {
      if (tsr != null) {
        tsr.close();
      }
    }
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testRunThriftServerWithHeaderBufferLength() throws Exception {
    // Test thrift server with HTTP header length less than 64k
    try {
      runThriftServer(1024 * 63);
    } catch (TTransportException tex) {
      assertFalse(tex.getMessage().equals("HTTP Response code: 431"));
    }

    // Test thrift server with HTTP header length more than 64k, expect an exception
    exception.expect(TTransportException.class);
    exception.expectMessage("HTTP Response code: 431");
    runThriftServer(1024 * 64);
  }

  protected Supplier<ThriftServer> getThriftServerSupplier() {
    return () -> new ThriftServer(TEST_UTIL.getConfiguration());
  }

  @Test
  public void testRunThriftServer() throws Exception {
    runThriftServer(0);
  }

  void runThriftServer(int customHeaderSize) throws Exception {
    // Add retries in case we see stuff like connection reset
    Exception clientSideException =  null;
    for (int i = 0; i < 10; i++) {
      clientSideException =  null;
      ThriftServerRunner tsr = createBoundServer(getThriftServerSupplier());
      String url = "http://" + HConstants.LOCALHOST + ":" + tsr.getThriftServer().listenPort;
      try {
        checkHttpMethods(url);
        talkToThriftServer(url, customHeaderSize);
        break;
      } catch (Exception ex) {
        clientSideException = ex;
        LOG.info("Client-side Exception", ex);
      } finally {
        tsr.close();
        tsr.join();
        if (tsr.getRunException() != null) {
          LOG.error("Invocation of HBase Thrift server threw exception", tsr.getRunException());
          throw tsr.getRunException();
        }
      }
    }

    if (clientSideException != null) {
      LOG.error("Thrift Client", clientSideException);
      throw clientSideException;
    }
  }

  private void checkHttpMethods(String url) throws Exception {
    // HTTP TRACE method should be disabled for security
    // See https://www.owasp.org/index.php/Cross_Site_Tracing
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("TRACE");
    conn.connect();
    Assert.assertEquals(conn.getResponseMessage(),
      HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }

  protected static volatile boolean tableCreated = false;

  protected void talkToThriftServer(String url, int customHeaderSize) throws Exception {
    THttpClient httpClient = new THttpClient(url);
    httpClient.open();

    if (customHeaderSize > 0) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < customHeaderSize; i++) {
        sb.append("a");
      }
      httpClient.setCustomHeader("User-Agent", sb.toString());
    }

    try {
      TProtocol prot;
      prot = new TBinaryProtocol(httpClient);
      Hbase.Client client = new Hbase.Client(prot);
      if (!tableCreated){
        TestThriftServer.createTestTables(client);
        tableCreated = true;
      }
      TestThriftServer.checkTableList(client);
    } finally {
      httpClient.close();
    }
  }
}
