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
package org.apache.hadoop.hbase.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseThriftTestingUtility {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseThriftTestingUtility.class);
  private Thread thriftServerOneThread;
  private volatile Exception thriftServerOneException;
  private ThriftServer thriftServer;
  private int testServerPort;

  public int getServerPort() {
    return testServerPort;
  }

  public void startThriftServer(Configuration conf) throws Exception {
    List<String> args = new ArrayList<>();
    testServerPort = HBaseTestingUtility.randomFreePort();
    args.add("-" + Constants.PORT_OPTION);
    args.add(String.valueOf(testServerPort));
    thriftServer = new ThriftServer(conf);
    LOG.info("Starting Thrift Server One on port: " + testServerPort);

    thriftServerOneException = null;
    thriftServerOneThread = new Thread(() -> {
      try {
        thriftServer.run(args.toArray(new String[args.size()]));
      } catch (Exception e) {
        thriftServerOneException = e;
      }
    });
    thriftServerOneThread.setName(ThriftServer.class.getSimpleName() + "-one");
    thriftServerOneThread.start();

    if (thriftServerOneException != null) {
      LOG.error("HBase Thrift server threw an exception ", thriftServerOneException);
      throw new Exception(thriftServerOneException);
    }

    // wait up to 10s for the server to start
    for (int i = 0; i < 100
        && (thriftServer.tserver == null || !thriftServer.tserver.isServing()); i++) {
      Thread.sleep(100);
    }

    LOG.info("Started Thrift Server One on port " + testServerPort);
  }

  public void stopThriftServer() throws Exception{
    LOG.debug("Stopping Thrift Server One");
    thriftServer.stop();
    thriftServerOneThread.join();
  }
}