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

/**
 * JMX MBean interface for exporting statistics on HBase RPC calls
 */
public interface HBaseRPCStatisticsMBean {

  /**
   * Returns average RPC processing time since reset was last called
   */
  public long getRpcProcessingTimeAverage();

  /**
   * The maximum RPC processing time for current update interval.
   */
  public long getRpcProcessingTimeMax();

  /**
   * The minimum RPC operation processing time since reset was last called
   */
  public long getRpcProcessingTimeMin();

  /**
   * The number of RPC operations in the last sampling interval
   */
  public int getRpcNumOps();

  /**
   * The average RPC operation queued time in the last sampling interval
   */
  public long getRpcQueueTimeAverage();

  /**
   * The maximum RPC operation queued time since reset was last called
   */
  public long getRpcQueueTimeMax();

  /**
   * The minimum RPC operation queued time since reset was last called
   */
  public long getRpcQueueTimeMin();

  /**
   * Reset all min max times
   */
  void resetAllMinMax();
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections();
  
  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen();  
}
