/**
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

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface of a class that will export metrics about Thrift to hadoop's metrics2.
 */
public interface MetricsThriftServerSource extends BaseSource {

  String BATCH_GET_KEY = "batchGet";
  String BATCH_MUTATE_KEY = "batchMutate";
  String TIME_IN_QUEUE_KEY = "timeInQueue";
  String THRIFT_CALL_KEY = "thriftCall";
  String SLOW_THRIFT_CALL_KEY = "slowThriftCall";
  String CALL_QUEUE_LEN_KEY = "callQueueLen";

  /**
   * Add how long an operation was in the queue.
   * @param time
   */
  void incTimeInQueue(long time);

  /**
   * Set the call queue length.
   * @param len Time
   */
  void setCallQueueLen(int len);

  /**
   * Add how many keys were in a batch get.
   * @param diff Num Keys
   */
  void incNumRowKeysInBatchGet(int diff);

  /**
   * Add how many keys were in a batch mutate.
   * @param diff Num Keys
   */
  void incNumRowKeysInBatchMutate(int diff);

  /**
   * Add how long a method took
   * @param name Method name
   * @param time Time
   */
  void incMethodTime(String name, long time);

  /**
   * Add how long a call took
   * @param time Time
   */
  void incCall(long time);

  /**
   * Increment how long a slow call took.
   * @param time Time
   */
  void incSlowCall(long time);

}
