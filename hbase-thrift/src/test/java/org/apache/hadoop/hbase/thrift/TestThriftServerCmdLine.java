/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner.ImplType;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Joiner;

/**
 * Start the HBase Thrift server on a random port through the command-line
 * interface and talk to it from client side.
 */
@Category({ClientTests.class, LargeTests.class})
@RunWith(Parameterized.class)
public class TestThriftServerCmdLine {

  public static final Log LOG =
      LogFactory.getLog(TestThriftServerCmdLine.class);

  private final ImplType implType;
  private boolean specifyFramed;
  private boolean specifyBindIP;
  private boolean specifyCompact;

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Thread cmdLineThread;
  private volatile Exception cmdLineException;

  private Exception clientSideException;

  private ThriftServer thriftServer;
  private int port;

  @Parameters
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
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
    TEST_UTIL.getConfiguration().setBoolean("hbase.table.sanity.checks", false);
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
          thriftServer.doMain(args);
        } catch (Exception e) {
          cmdLineException = e;
        }
      }
    });
    cmdLineThread.setName(ThriftServer.class.getSimpleName() +
        "-cmdline");
    cmdLineThread.start();
  }

  @Test(timeout=600000)
  public void testRunThriftServer() throws Exception {
    List<String> args = new ArrayList<String>();
    if (implType != null) {
      String serverTypeOption = implType.toString();
      assertTrue(serverTypeOption.startsWith("-"));
      args.add(serverTypeOption);
    }
    port = HBaseTestingUtility.randomFreePort();
    args.add("-" + ThriftServer.PORT_OPTION);
    args.add(String.valueOf(port));
    if (specifyFramed) {
      args.add("-" + ThriftServer.FRAMED_OPTION);
    }
    if (specifyBindIP) {
      args.add("-" + ThriftServer.BIND_OPTION);
      args.add(InetAddress.getLocalHost().getHostName());
    }
    if (specifyCompact) {
      args.add("-" + ThriftServer.COMPACT_OPTION);
    }
    args.add("start");

    thriftServer = new ThriftServer(TEST_UTIL.getConfiguration());
    startCmdLineThread(args.toArray(new String[args.size()]));

    // wait up to 10s for the server to start
    for (int i = 0; i < 100
        && (thriftServer.serverRunner == null || thriftServer.serverRunner.tserver == null); i++) {
      Thread.sleep(100);
    }

    Class<? extends TServer> expectedClass = implType != null ?
        implType.serverClass : TBoundedThreadPoolServer.class;
    assertEquals(expectedClass,
                 thriftServer.serverRunner.tserver.getClass());

    try {
      talkToThriftServer();
    } catch (Exception ex) {
      clientSideException = ex;
    } finally {
      stopCmdLineThread();
    }

    if (clientSideException != null) {
      LOG.error("Thrift client threw an exception. Parameters:" +
          getParametersString(), clientSideException);
      throw new Exception(clientSideException);
    }
  }

  private static volatile boolean tableCreated = false;

  private void talkToThriftServer() throws Exception {
    TSocket sock = new TSocket(InetAddress.getLocalHost().getHostName(),
        port);
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

