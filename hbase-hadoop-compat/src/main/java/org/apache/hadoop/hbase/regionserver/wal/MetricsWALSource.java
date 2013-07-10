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

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface of the source that will export metrics about the region server's HLog.
 */
public interface MetricsWALSource extends BaseSource {


  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "WAL";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about HBase RegionServer HLog";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


  static final String APPEND_TIME = "appendTime";
  static final String APPEND_TIME_DESC = "Time an append to the log took.";
  static final String APPEND_COUNT = "appendCount";
  static final String APPEND_COUNT_DESC = "Number of appends to the write ahead log.";
  static final String APPEND_SIZE = "appendSize";
  static final String APPEND_SIZE_DESC = "Size (in bytes) of the data appended to the HLog.";
  static final String SLOW_APPEND_COUNT = "slowAppendCount";
  static final String SLOW_APPEND_COUNT_DESC = "Number of appends that were slow.";
  static final String SYNC_TIME = "syncTime";
  static final String SYNC_TIME_DESC = "The time it took to sync the HLog to HDFS.";

  /**
   * Add the append size.
   */
  void incrementAppendSize(long size);

  /**
   * Add the time it took to append.
   */
  void incrementAppendTime(long time);

  /**
   * Increment the count of hlog appends
   */
  void incrementAppendCount();

  /**
   * Increment the number of appends that were slow
   */
  void incrementSlowAppendCount();

  /**
   * Add the time it took to sync the hlog.
   */
  void incrementSyncTime(long time);

}
