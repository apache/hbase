/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerTransport;

/**
 * A TThreadedSelectorServer.Args that reads hadoop configuration
 */
@InterfaceAudience.Private
public class HThreadedSelectorServerArgs extends TThreadedSelectorServer.Args {
  private static final Log LOG = LogFactory.getLog(TThreadedSelectorServer.class);

  /**
   * Number of selector threads for reading and writing socket
   */
  public static final String SELECTOR_THREADS_CONF_KEY =
      "hbase.thrift.selector.threads";

  /**
   * Number fo threads for processing the thrift calls
   */
  public static final String WORKER_THREADS_CONF_KEY =
      "hbase.thrift.worker.threads";

  /**
   * Time to wait for server to stop gracefully
   */
  public static final String STOP_TIMEOUT_CONF_KEY =
      "hbase.thrift.stop.timeout.seconds";

  /**
   * Maximum number of accepted elements per selector
   */
  public static final String ACCEPT_QUEUE_SIZE_PER_THREAD_CONF_KEY =
      "hbase.thrift.accept.queue.size.per.selector";

  /**
   * The strategy for handling new accepted connections.
   */
  public static final String ACCEPT_POLICY_CONF_KEY =
      "hbase.thrift.accept.policy";

  public HThreadedSelectorServerArgs(
      TNonblockingServerTransport transport, Configuration conf) {
    super(transport);
    readConf(conf);
  }

  private void readConf(Configuration conf) {
    int selectorThreads = conf.getInt(
        SELECTOR_THREADS_CONF_KEY, getSelectorThreads());
    int workerThreads = conf.getInt(
        WORKER_THREADS_CONF_KEY, getWorkerThreads());
    int stopTimeoutVal = conf.getInt(
        STOP_TIMEOUT_CONF_KEY, getStopTimeoutVal());
    int acceptQueueSizePerThread = conf.getInt(
        ACCEPT_QUEUE_SIZE_PER_THREAD_CONF_KEY, getAcceptQueueSizePerThread());
    AcceptPolicy acceptPolicy = AcceptPolicy.valueOf(conf.get(
        ACCEPT_POLICY_CONF_KEY, getAcceptPolicy().toString()).toUpperCase());

    super.selectorThreads(selectorThreads)
         .workerThreads(workerThreads)
         .stopTimeoutVal(stopTimeoutVal)
         .acceptQueueSizePerThread(acceptQueueSizePerThread)
         .acceptPolicy(acceptPolicy);

    LOG.info("Read configuration selectorThreads:" + selectorThreads +
             " workerThreads:" + workerThreads +
             " stopTimeoutVal:" + stopTimeoutVal + "sec" +
             " acceptQueueSizePerThread:" + acceptQueueSizePerThread +
             " acceptPolicy:" + acceptPolicy);
  }
}
