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

import static org.apache.hadoop.hbase.thrift.Constants.BIND_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.COMPACT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.FRAMED_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.INFOPORT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.PORT_OPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

/**
 * Start the HBase Thrift server on a random port through the command-line
 * interface and talk to it from client side.
 */
@Category({ClientTests.class, LargeTests.class})
@RunWith(Parameterized.class)
public class TestThriftServerCmdLine {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftServerCmdLine.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestThriftServerCmdLine.class);

  protected final ImplType implType;
  protected boolean specifyFramed;
  protected boolean specifyBindIP;
  protected boolean specifyCompact;

  protected static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Thread cmdLineThread;
  private volatile Exception cmdLineException;

  private Exception clientSideException;

  private volatile ThriftServer thriftServer;
  protected int port;

  @Parameters
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<>();
    for (ImplType implType : ImplType.values()) {
      for (boolean specifyFramed : new boolean[] {false, true}) {
        for (boolean specifyBindIP : new boolean[] {false, true}) {
          if (specifyBindIP && !implType.canSpecifyBindIP) {
            continue;
          }
          for (boolean specifyCompact : new boolean[] {false, true}) {
            parameters.add(new Object[] {
              implType, specifyFramed, specifyBindIP, specifyCompact
            });
          }
        }
      }
    }
    return parameters;
  }

  public TestThriftServerCmdLine(ImplType implType, boolean specifyFramed,
      boolean specifyBindIP, boolean specifyCompact) {
    this.implType = implType;
    this.specifyFramed = specifyFramed;
    this.specifyBindIP = specifyBindIP;
    this.specifyCompact = specifyCompact;
    LOG.debug(getParametersString());
  }

  private String getParametersString() {
    return "implType=" + implType + ", " +
        "specifyFramed=" + specifyFramed + ", " +
        "specifyBindIP=" + specifyBindIP + ", " +
        "specifyCompact=" + specifyCompact;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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

  private void startCmdLineThread(final String[] args) {
    LOG.info("Starting HBase Thrift server with command line: " + Joiner.on(" ").join(args));

    cmdLineException = null;
    cmdLineThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          thriftServer.run(args);
        } catch (Exception e) {
          LOG.error("Error when start thrift server", e);
          cmdLineException = e;
        }
      }
    });
    cmdLineThread.setName(ThriftServer.class.getSimpleName() +
        "-cmdline");
    cmdLineThread.start();
  }

  protected ThriftServer createThriftServer() {
    return new ThriftServer(TEST_UTIL.getConfiguration());
  }

  private int getRandomPort() {
    return HBaseTestingUtility.randomFreePort();
  }

  /**
   * Server can fail to bind if clashing address. Add retrying until we get a good server.
   */
  ThriftServer createBoundServer() {
    return createBoundServer(false, false);
  }

  private ServerSocket getBoundSocket() {
    ServerSocket ss = null;
    while (true) {
      port = getRandomPort();
      try {
        ss = new ServerSocket();
        ss.bind(new InetSocketAddress(port));
        break;
      } catch (IOException ioe) {
        LOG.warn("Failed bind", ioe);
        try {
          ss.close();
        } catch (IOException ioe2) {
          LOG.warn("FAILED CLOSE of failed bind socket", ioe2);
        }
      }
    }
    return ss;
  }

  /**
   * @param protocolPortClash This param is just so we can manufacture a port clash so we can test
   *   the code does the right thing when this happens during actual test runs. Ugly but works.
   * @see TestBindExceptionHandling#testProtocolPortClash()
   */
  ThriftServer createBoundServer(boolean protocolPortClash, boolean infoPortClash) {
    if (protocolPortClash && infoPortClash) {
      throw new RuntimeException("Can't set both at same time");
    }
    boolean testClashOfFirstProtocolPort = protocolPortClash;
    boolean testClashOfFirstInfoPort = infoPortClash;
    List<String> args = new ArrayList<>();
    ServerSocket ss = null;
    for (int i = 0; i < 100; i++) {
      args.clear();
      if (implType != null) {
        String serverTypeOption = implType.toString();
        assertTrue(serverTypeOption.startsWith("-"));
        args.add(serverTypeOption);
      }
      if (testClashOfFirstProtocolPort) {
        // Test what happens if already something bound to the socket.
        // Occupy the random port we just pulled.
        ss = getBoundSocket();
        port = ss.getLocalPort();
        testClashOfFirstProtocolPort = false;
      } else {
        port = getRandomPort();
      }
      args.add("-" + PORT_OPTION);
      args.add(String.valueOf(port));
      args.add("-" + INFOPORT_OPTION);
      int infoPort;
      if (testClashOfFirstInfoPort) {
        ss = getBoundSocket();
        infoPort = ss.getLocalPort();
        testClashOfFirstInfoPort = false;
      } else {
        infoPort = getRandomPort();
      }
      args.add(String.valueOf(infoPort));

      if (specifyFramed) {
        args.add("-" + FRAMED_OPTION);
      }
      if (specifyBindIP) {
        args.add("-" + BIND_OPTION);
        args.add(InetAddress.getLoopbackAddress().getHostName());
      }
      if (specifyCompact) {
        args.add("-" + COMPACT_OPTION);
      }
      args.add("start");

      thriftServer = createThriftServer();
      startCmdLineThread(args.toArray(new String[args.size()]));
      // wait up to 10s for the server to start
      for (int ii = 0; ii < 100 && (thriftServer.tserver == null); ii++) {
        Threads.sleep(100);
      }
      if (isBindException(cmdLineException)) {
        LOG.info("BindException; trying new port", cmdLineException);
        cmdLineException =  null;
        thriftServer.stop();
        continue;
      }
      break;
    }
    if (ss != null) {
      try {
        ss.close();
      } catch (IOException ioe) {
        LOG.warn("Failed close", ioe);
      }
    }
    Class<? extends TServer> expectedClass = implType != null ?
      implType.serverClass : TBoundedThreadPoolServer.class;
    assertEquals(expectedClass, thriftServer.tserver.getClass());
    LOG.info("Server={}", args);
    return thriftServer;
  }

  private boolean isBindException(Exception cmdLineException) {
    if (cmdLineException == null) {
      return false;
    }
    if (cmdLineException instanceof BindException) {
      return true;
    }
    if (cmdLineException.getCause() != null &&
        cmdLineException.getCause() instanceof BindException) {
      return true;
    }
    return false;
  }

  @Test
  public void testRunThriftServer() throws Exception {
    ThriftServer thriftServer = createBoundServer();
    // Add retries in case we see stuff like connection reset
    for (int i = 0; i < 10; i++) {
      try {
        talkToThriftServer();
        break;
      } catch (Exception ex) {
        clientSideException = ex;
        LOG.info("Exception", ex);
      } finally {
        stopCmdLineThread();
        thriftServer.stop();
      }
    }

    if (clientSideException != null) {
      LOG.error("Thrift client threw an exception. Parameters:" +
          getParametersString(), clientSideException);
      throw new Exception(clientSideException);
    }
  }

  protected static volatile boolean tableCreated = false;

  protected void talkToThriftServer() throws Exception {
    LOG.info("Talking to port=" + this.port);
    TSocket sock = new TSocket(InetAddress.getLoopbackAddress().getHostName(), port);
    TTransport transport = sock;
    if (specifyFramed || implType.isAlwaysFramed) {
      transport = new TFramedTransport(transport);
    }

    sock.open();
    try {
      TProtocol prot;
      if (specifyCompact) {
        prot = new TCompactProtocol(transport);
      } else {
        prot = new TBinaryProtocol(transport);
      }

      Hbase.Client client = new Hbase.Client(prot);
      if (!tableCreated){
        TestThriftServer.createTestTables(client);
        tableCreated = true;
      }
      TestThriftServer.checkTableList(client);

    } finally {
      sock.close();
    }
  }

  private void stopCmdLineThread() throws Exception {
    LOG.debug("Stopping " + implType.simpleClassName() + " Thrift server");
    thriftServer.stop();
    cmdLineThread.join();
    if (cmdLineException != null) {
      LOG.error("Command-line invocation of HBase Thrift server threw an " +
          "exception", cmdLineException);
      throw new Exception(cmdLineException);
    }
  }
}

