/*
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * A base class for Thrift API tests. 
 */
public class ThriftServerTestBase {

  private static Log LOG = LogFactory.getLog(TestMutationWriteToWAL.class);

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static ThriftServerRunner serverRunner;
  protected static int thriftServerPort;

  private static List<TSocket> clientSockets = new ArrayList<TSocket>();
  
  private static ExecutorService thriftExec = Executors.newSingleThreadExecutor();
  private static Future<?> thriftFuture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Thread.currentThread().setName("tearDownAfterClass");
    Configuration conf = TEST_UTIL.getConfiguration();
    thriftServerPort = HBaseTestingUtility.randomFreePort();
    conf.setInt(HConstants.THRIFT_PROXY_PREFIX + HConstants.THRIFT_PORT_SUFFIX,
        thriftServerPort);
    TEST_UTIL.startMiniCluster();
    serverRunner = new ThriftServerRunner(conf, HConstants.THRIFT_PROXY_PREFIX);

    thriftFuture = thriftExec.submit(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("thriftServerThread");
        serverRunner.run();
      }
    });
    
    LOG.info("Waiting for Thrift server to come online");
    HBaseTestingUtility.waitForHostPort(HConstants.LOCALHOST, thriftServerPort);
    LOG.info("Thrift server is online");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HBaseTestingUtility.setThreadNameFromMethod();
    closeClientSockets();
    LOG.info("Shutting down Thrift server");
    serverRunner.shutdown();
    thriftFuture.get();  // this will throw an exception if the Thrift thread failed
    TEST_UTIL.shutdownMiniCluster();
  }
  
  public static Hbase.Client createClient() throws TTransportException, UnknownHostException {
    LOG.info("Connecting Thrift server on port " + thriftServerPort);
    TSocket sock = new TSocket(InetAddress.getLocalHost().getHostName(),
        thriftServerPort);
    TTransport transport = new TFramedTransport(sock);
    sock.open();
    synchronized (clientSockets) {
      clientSockets.add(sock);
    }
    TProtocol prot = new TBinaryProtocol(transport);
    return new Hbase.Client(prot);
  }
  
  public static void closeClientSockets() {
    synchronized (clientSockets) {
      for (TSocket sock : clientSockets) {
        sock.close();
      }
      clientSockets.clear();
    }
  }
  
}

