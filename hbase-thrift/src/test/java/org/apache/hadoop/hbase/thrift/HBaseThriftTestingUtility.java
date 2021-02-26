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

import static org.apache.hadoop.hbase.thrift.Constants.INFOPORT_OPTION;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.thrift.ThriftMetrics.ThriftServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

public class HBaseThriftTestingUtility {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseThriftTestingUtility.class);
  private Thread thriftServerThread;
  private volatile Exception thriftServerException;
  private ThriftServer thriftServer;
  private int port;

  public int getServerPort() {
    return port;
  }

  /**
   * start thrift server
   * @param conf configuration
   * @param type the type of thrift server
   * @throws Exception When starting the thrift server fails.
   */
  public void startThriftServer(Configuration conf, ThriftServerType type) throws Exception {
    List<String> args = new ArrayList<>();
    port = HBaseTestingUtility.randomFreePort();
    args.add("-" + Constants.PORT_OPTION);
    args.add(String.valueOf(port));
    args.add("-" + INFOPORT_OPTION);
    int infoPort = HBaseTestingUtility.randomFreePort();
    args.add(String.valueOf(infoPort));

    LOG.info("Starting Thrift Server {} on port: {} ", type, port);
    thriftServer = createThriftServer(conf, type);
    startThriftServerThread(args.toArray(new String[args.size()]));
    // wait up to 10s for the server to start
    waitForThriftServer();
    LOG.info("Started Thrift Server {} on port {}", type, port);
  }

  private void startThriftServerThread(final String[] args) {
    LOG.info("Starting HBase Thrift server with command line: " + Joiner.on(" ").join(args));

    thriftServerException = null;
    thriftServerThread = new Thread(() -> {
      try {
        thriftServer.run(args);
      } catch (Exception e) {
        thriftServerException = e;
      }
    });
    thriftServerThread.setName(ThriftServer.class.getSimpleName());
    thriftServerThread.start();
  }

  /**
   * create a new thrift server
   * @param conf configuration
   * @param type the type of thrift server
   * @return the instance of ThriftServer
   */
  private ThriftServer createThriftServer(Configuration conf, ThriftServerType type) {
    switch (type) {
      case ONE:
        return new ThriftServer(conf);
      case TWO:
        return new org.apache.hadoop.hbase.thrift2.ThriftServer(conf);
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  private void waitForThriftServer() throws Exception {
    boolean isServing = false;
    int i = 0;
    while (i++ < 100) {
      if (thriftServer.tserver == null) {
        Thread.sleep(100);
      } else {
        isServing = true;
        break;
      }
    }

    if (!isServing) {
      if (thriftServer != null) {
        thriftServer.stop();
      }
      throw new IOException("Failed to start thrift server ");
    }
  }

  public void stopThriftServer() throws Exception{
    LOG.debug("Stopping Thrift Server");
    thriftServer.stop();
    thriftServerThread.join();
    if (thriftServerException != null) {
      LOG.error("HBase Thrift server threw an exception ", thriftServerException);
      throw new Exception(thriftServerException);
    }
  }
}