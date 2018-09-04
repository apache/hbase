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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * This interface will be implemented by a MetricsSource that will export metrics from
 * multiple regions of a table into the hadoop metrics system.
 */
public interface MetricsTableAggregateSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Tables";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer tables";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String NUM_TABLES = "numTables";
  String NUMBER_OF_TABLES_DESC = "Number of tables in the metrics system";

  /**
   * Register a MetricsTableSource as being open.
   *
   * @param table The table name
   * @param source the source for the table being opened.
   */
  void register(String table, MetricsTableSource source);

  /**
   * Remove a table's source. This is called when regions of a table are closed.
   *
   * @param table The table name
   */
  void deregister(String table);

}
