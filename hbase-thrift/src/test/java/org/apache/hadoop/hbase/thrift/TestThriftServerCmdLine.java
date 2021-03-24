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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.net.BoundSocketMaker;
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
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
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
            parameters.add(new Object[]{implType, specifyFramed,
              specifyBindIP, specifyCompact});
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

  static ThriftServerRunner startCmdLineThread(Supplier<ThriftServer> supplier,
      final String[] args) {
    LOG.info("Starting HBase Thrift server with command line: " + Joiner.on(" ").join(args));
    ThriftServerRunner tsr = new ThriftServerRunner(supplier.get(), args);
    tsr.setName(ThriftServer.class.getSimpleName() + "-cmdline");
    tsr.start();
    return tsr;
  }

  static int getRandomPort() {
    return HBaseTestingUtility.randomFreePort();
  }

  protected Supplier<ThriftServer> getThriftServerSupplier() {
    return () -> new ThriftServer(TEST_UTIL.getConfiguration());
  }

  static ThriftServerRunner createBoundServer(Supplier<ThriftServer> thriftServerSupplier)
      throws Exception {
    return createBoundServer(thriftServerSupplier, false, false);
  }

  static ThriftServerRunner createBoundServer(Supplier<ThriftServer> thriftServerSupplier,
      boolean protocolPortClash, boolean infoPortClash) throws Exception {
    return createBoundServer(thriftServerSupplier, null, false, false,
      false, protocolPortClash, infoPortClash);
  }

  static ThriftServerRunner createBoundServer(Supplier<ThriftServer> thriftServerSupplier,
      ImplType implType, boolean specifyFramed, boolean specifyCompact, boolean specifyBindIP)
      throws Exception {
    return createBoundServer(thriftServerSupplier, implType, specifyFramed, specifyCompact,
      specifyBindIP, false, false);
  }

  /**
   * @param protocolPortClash This param is just so we can manufacture a port clash so we can test
   *   the code does the right thing when this happens during actual test runs. Ugly but works.
   * @see TestBindExceptionHandling#testProtocolPortClash()
   */
  static ThriftServerRunner createBoundServer(Supplier<ThriftServer> thriftServerSupplier,
      ImplType implType, boolean specifyFramed, boolean specifyCompact, boolean specifyBindIP,
      boolean protocolPortClash, boolean infoPortClash) throws Exception {
    if (protocolPortClash && infoPortClash) {
      throw new RuntimeException("Can't set both at same time");
    }
    boolean testClashOfFirstProtocolPort = protocolPortClash;
    boolean testClashOfFirstInfoPort = infoPortClash;
    List<String> args = new ArrayList<>();
    BoundSocketMaker bsm = null;
    int port = -1;
    ThriftServerRunner tsr = null;
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
        bsm = new BoundSocketMaker(() -> getRandomPort());
        port = bsm.getPort();
        testClashOfFirstProtocolPort = false;
      } else {
        port = getRandomPort();
      }
      args.add("-" + PORT_OPTION);
      args.add(String.valueOf(port));
      args.add("-" + INFOPORT_OPTION);
      int infoPort;
      if (testClashOfFirstInfoPort) {
        bsm = new BoundSocketMaker(() -> getRandomPort());
        infoPort = bsm.getPort();
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

      tsr = startCmdLineThread(thriftServerSupplier, args.toArray(new String[args.size()]));
      // wait up to 10s for the server to start
      for (int ii = 0; ii < 100 && (tsr.getThriftServer().tserver == null &&
          tsr.getRunException() == null); ii++) {
        Threads.sleep(100);
      }
      if (isBindException(tsr.getRunException())) {
        LOG.info("BindException; trying new port", tsr.getRunException());
        try {
          tsr.close();
          tsr.join();
        } catch (IOException | InterruptedException ioe) {
          LOG.warn("Exception closing", ioe);
        }
        continue;
      }
      break;
    }
    if (bsm != null) {
      try {
        bsm.close();
      } catch (IOException ioe) {
        LOG.warn("Failed close", ioe);
      }
    }
    if (tsr.getRunException() != null) {
      throw tsr.getRunException();
    }
    if (tsr.getThriftServer().tserver != null) {
      Class<? extends TServer> expectedClass =
        implType != null ? implType.serverClass : TBoundedThreadPoolServer.class;
      assertEquals(expectedClass, tsr.getThriftServer().tserver.getClass());
    }
    return tsr;
  }

  private static boolean isBindException(Exception cmdLineException) {
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
    // Add retries in case we see stuff like connection reset
    Exception clientSideException =  null;
    for (int i = 0; i < 10; i++) {
      clientSideException =  null;
      ThriftServerRunner thriftServerRunner = createBoundServer(getThriftServerSupplier(),
        this.implType, this.specifyFramed, this.specifyCompact, this.specifyBindIP);
      try {
        talkToThriftServer(thriftServerRunner.getThriftServer().listenPort);
        break;
      } catch (Exception ex) {
        clientSideException = ex;
        LOG.info("Exception", ex);
      } finally {
        LOG.debug("Stopping " + this.implType.simpleClassName() + " Thrift server");
        thriftServerRunner.close();
        thriftServerRunner.join();
        if (thriftServerRunner.getRunException() != null) {
          LOG.error("Command-line invocation of HBase Thrift server threw exception",
            thriftServerRunner.getRunException());
          throw thriftServerRunner.getRunException();
        }
      }
    }

    if (clientSideException != null) {
      LOG.error("Thrift Client; parameters={}", getParametersString(), clientSideException);
      throw new Exception(clientSideException);
    }
  }

  protected static volatile boolean tableCreated = false;

  protected void talkToThriftServer(int port) throws Exception {
    LOG.info("Talking to port={}", port);
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
}

