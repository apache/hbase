/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.ipc;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.util.MBeanUtil;

/**
 * Exports HBase RPC statistics recorded in {@link HBaseRpcMetrics} as an MBean
 * for JMX monitoring.
 */
public class HBaseRPCStatistics implements HBaseRPCStatisticsMBean {
  private final HBaseRpcMetrics rpcMetrics;
  private final ObjectName mbeanName;
  private final HBaseServer server;

  public HBaseRPCStatistics(HBaseRpcMetrics metrics, 
      String hostName, String port, HBaseServer hbaseServer) {
    rpcMetrics = metrics;

    String name = String.format("RPCStatistics-%s", 
        (port != null ? port : "unknown"));

    mbeanName = MBeanUtil.registerMBean("HBase", name, this);
    server = hbaseServer;
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  /**
   * Returns average RPC processing time since reset was last called
   */
  @Override
  public long getRpcProcessingTimeAverage() {
    return rpcMetrics.rpcProcessingTime.getPreviousIntervalAverageTime();
  }

  /**
   * The maximum RPC processing time for current update interval.
   */
  @Override
  public long getRpcProcessingTimeMax() {
    return rpcMetrics.rpcProcessingTime.getMaxTime();
  }

  /**
   * The minimum RPC operation processing time since reset was last called
   */
  @Override
  public long getRpcProcessingTimeMin() {
    return rpcMetrics.rpcProcessingTime.getMinTime();
  }

  /**
   * The number of RPC operations in the last sampling interval
   */
  @Override
  public int getRpcNumOps() {
    return rpcMetrics.rpcProcessingTime.getPreviousIntervalNumOps();
  }

  /**
   * The average RPC operation queued time in the last sampling interval
   */
  @Override
  public long getRpcQueueTimeAverage() {
    return rpcMetrics.rpcQueueTime.getPreviousIntervalAverageTime();
  }

  /**
   * The maximum RPC operation queued time since reset was last called
   */
  @Override
  public long getRpcQueueTimeMax() {
    return rpcMetrics.rpcQueueTime.getMaxTime();
  }

  /**
   * The minimum RPC operation queued time since reset was last called
   */
  @Override
  public long getRpcQueueTimeMin() {
    return rpcMetrics.rpcQueueTime.getMinTime();
  }

  /**
   * The number of RPC calls in the queue
   */
  @Override
  public int getCallQueueLen() {
    return server.getCallQueueLen();
  }

  /**
   * The number of current RPC connections
   */
  @Override
  public int getNumOpenConnections() {
    return server.getNumOpenConnections();
  }

  /**
   * Resets minimum and maximum values for RPC queue
   * and processing timers.
   */
  @Override
  public void resetAllMinMax() {
    rpcMetrics.rpcProcessingTime.resetMinMax();
    rpcMetrics.rpcQueueTime.resetMinMax();
  }

}
