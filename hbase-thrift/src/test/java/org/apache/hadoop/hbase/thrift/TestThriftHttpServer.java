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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;

/**
 * Start the HBase Thrift HTTP server on a random port through the command-line
 * interface and talk to it from client side.
 */
@Category({ClientTests.class, LargeTests.class})

public class TestThriftHttpServer {

  private static final Log LOG =
      LogFactory.getLog(TestThriftHttpServer.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Thread httpServerThread;
  private volatile Exception httpServerException;

  private Exception clientSideException;

  private ThriftServer thriftServer;
  private int port;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.thrift.http", true);
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

  private void startHttpServerThread(final String[] args) {
    LOG.info("Starting HBase Thrift server with HTTP server: " + Joiner.on(" ").join(args));

    httpServerException = null;
    httpServerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          thriftServer.doMain(args);
        } catch (Exception e) {
          httpServerException = e;
        }
      }
    });
    httpServerThread.setName(ThriftServer.class.getSimpleName() +
        "-httpServer");
    httpServerThread.start();
  }

  @Test(timeout=600000)
  public void testRunThriftServer() throws Exception {
    List<String> args = new ArrayList<String>();
    port = HBaseTestingUtility.randomFreePort();
    args.add("-" + ThriftServer.PORT_OPTION);
    args.add(String.valueOf(port));
    args.add("start");

    thriftServer = new ThriftServer(TEST_UTIL.getConfiguration());
    startHttpServerThread(args.toArray(new String[args.size()]));

    // wait up to 10s for the server to start
    for (int i = 0; i < 100
        && ( thriftServer.serverRunner == null ||  thriftServer.serverRunner.httpServer ==
        null); i++) {
      Thread.sleep(100);
    }

    try {
      talkToThriftServer();
    } catch (Exception ex) {
      clientSideException = ex;
    } finally {
      stopHttpServerThread();
    }

    if (clientSideException != null) {
      LOG.error("Thrift client threw an exception " + clientSideException);
      throw new Exception(clientSideException);
    }
  }

  private static volatile boolean tableCreated = false;

  private void talkToThriftServer() throws Exception {
    THttpClient httpClient = new THttpClient(
        "http://"+ HConstants.LOCALHOST + ":" + port);
    httpClient.open();
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

  private void stopHttpServerThread() throws Exception {
    LOG.debug("Stopping " + " Thrift HTTP server");
    thriftServer.stop();
    httpServerThread.join();
    if (httpServerException != null) {
      LOG.error("Command-line invocation of HBase Thrift server threw an " +
          "exception", httpServerException);
      throw new Exception(httpServerException);
    }
  }
}
