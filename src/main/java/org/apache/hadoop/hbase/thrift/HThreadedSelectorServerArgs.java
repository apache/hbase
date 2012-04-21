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

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TThreadedSelectorServer.Args that reads hadoop configuration
 */
public class HThreadedSelectorServerArgs extends TThreadedSelectorServer.Args {

  private static final Logger LOG =
      LoggerFactory.getLogger(TThreadedSelectorServer.class);

  /**
   * Number of selector threads for reading and writing socket
   */
  public static final String SELECTOR_THREADS_SUFFIX =
      "selector.threads";

  /**
   * Number of threads for processing the thrift calls
   */
  public static final String WORKER_THREADS_SUFFIX =
      "worker.threads";

  /**
   * Time to wait for server to stop gracefully
   */
  public static final String STOP_TIMEOUT_SUFFIX =
      "stop.timeout.seconds";

  /**
   * Maximum number of accepted elements per selector
   */
  public static final String ACCEPT_QUEUE_SIZE_PER_THREAD_SUFFIX =
      "accept.queue.size.per.selector";

  /**
   * The strategy for handling new accepted connections.
   */
  public static final String ACCEPT_POLICY_SUFFIX =
      "accept.policy";

  public HThreadedSelectorServerArgs(TNonblockingServerTransport transport, Configuration conf,
      String confKeyPrefix) {
    super(transport);
    readConf(conf, confKeyPrefix);
  }

  private void readConf(Configuration conf, String confKeyPrefix) {
    int selectorThreads = conf.getInt(
        confKeyPrefix + SELECTOR_THREADS_SUFFIX, getSelectorThreads());
    int workerThreads = conf.getInt(
        confKeyPrefix + WORKER_THREADS_SUFFIX, getWorkerThreads());
    int stopTimeoutVal = conf.getInt(
        confKeyPrefix + STOP_TIMEOUT_SUFFIX, getStopTimeoutVal());
    int acceptQueueSizePerThread = conf.getInt(
        confKeyPrefix + ACCEPT_QUEUE_SIZE_PER_THREAD_SUFFIX, getAcceptQueueSizePerThread());
    AcceptPolicy acceptPolicy = AcceptPolicy.valueOf(conf.get(
        confKeyPrefix + ACCEPT_POLICY_SUFFIX, getAcceptPolicy().toString()).toUpperCase());

    super.selectorThreads(selectorThreads)
         .workerThreads(workerThreads)
         .stopTimeoutVal(stopTimeoutVal)
         .acceptQueueSizePerThread(acceptQueueSizePerThread)
         .acceptPolicy(acceptPolicy);

    LOG.info("Read Thrift server configuration from keys with prefix '" + confKeyPrefix + "':" +
             " selectorThreads:" + selectorThreads +
             " workerThreads:" + workerThreads +
             " stopTimeoutVal:" + stopTimeoutVal + "sec" +
             " acceptQueueSizePerThread:" + acceptQueueSizePerThread +
             " acceptPolicy:" + acceptPolicy);
  }
}
