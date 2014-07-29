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
 * Interface of the source that will export metrics about the region server's WAL.
 */
public interface MetricsWALSource extends BaseSource {


  /**
   * The name of the metrics
   */
  String METRICS_NAME = "WAL";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer WAL";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


  String APPEND_TIME = "appendTime";
  String APPEND_TIME_DESC = "Time an append to the log took.";
  String APPEND_COUNT = "appendCount";
  String APPEND_COUNT_DESC = "Number of appends to the write ahead log.";
  String APPEND_SIZE = "appendSize";
  String APPEND_SIZE_DESC = "Size (in bytes) of the data appended to the WAL.";
  String SLOW_APPEND_COUNT = "slowAppendCount";
  String SLOW_APPEND_COUNT_DESC = "Number of appends that were slow.";
  String SYNC_TIME = "syncTime";
  String SYNC_TIME_DESC = "The time it took to sync the WAL to HDFS.";

  /**
   * Add the append size.
   */
  void incrementAppendSize(long size);

  /**
   * Add the time it took to append.
   */
  void incrementAppendTime(long time);

  /**
   * Increment the count of wal appends
   */
  void incrementAppendCount();

  /**
   * Increment the number of appends that were slow
   */
  void incrementSlowAppendCount();

  /**
   * Add the time it took to sync the wal.
   */
  void incrementSyncTime(long time);

}
