/**
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
package org.apache.hadoop.hbase.thrift2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseThrift2TestingUtility {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseThrift2TestingUtility.class);
  private Thread thriftServerTwoThread;
  private volatile Exception thriftServerTwoException;
  private int testServerPort;
  private ThriftServer thriftServer;
  public int getServerPort() {
    return testServerPort;
  }

  public void startThriftServer(Configuration conf) throws Exception {
    testServerPort = HBaseTestingUtility.randomFreePort();
    LOG.info("Starting Thrift Server Two on port: " + testServerPort);
    thriftServer = new ThriftServer(conf);
    thriftServerTwoException = null;
    thriftServerTwoThread = new Thread(() -> {
      try {
        thriftServer.run(new String[]{"-p", String.valueOf(testServerPort)});
      } catch (Exception e) {
        thriftServerTwoException = e;
      }
    });
    thriftServerTwoThread.setName(ThriftServer.class.getSimpleName() + "-two");
    thriftServerTwoThread.start();

    if (thriftServerTwoException != null) {
      LOG.error("HBase Thrift server Two threw an exception ", thriftServerTwoException);
      throw new Exception(thriftServerTwoException);
    }

    // wait up to 10s for the server to start
    for (int i = 0; i < 100 &&
        (thriftServer.getTserver() == null || !thriftServer.getTserver().isServing()); i++) {
      Thread.sleep(100);
    }

    LOG.info("Started Thrift Server Two on port " + testServerPort);
  }

  public void stopThriftServer() throws Exception{
    LOG.debug("Stopping Thrift Server Two");
    thriftServer.stop();
    thriftServerTwoThread.join();
  }
}