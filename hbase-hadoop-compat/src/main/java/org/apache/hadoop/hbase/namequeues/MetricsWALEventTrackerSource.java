/*
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
package org.apache.hadoop.hbase.namequeues;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsWALEventTrackerSource extends BaseSource {
  /**
   * The name of the metrics
   */
  String METRICS_NAME = "WALEventTracker";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer WALEventTracker";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String NUM_FAILED_PUTS = "numFailedPuts";
  String NUM_FAILED_PUTS_DESC = "Number of put requests that failed";

  String NUM_RECORDS_FAILED_PUTS = "numRecordsFailedPuts";
  String NUM_RECORDS_FAILED_PUTS_DESC = "number of records in failed puts";

  /*
   * Increment 2 counters, numFailedPuts and numRecordsFailedPuts
   */
  void incrFailedPuts(long numRecords);

  /*
   * Get the failed puts counter.
   */
  long getFailedPuts();

  /*
   * Get the number of records in failed puts.
   */
  long getNumRecordsFailedPuts();
}
