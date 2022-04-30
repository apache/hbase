/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query Per Second for each table in a RegionServer.
 */
@InterfaceAudience.Private
public interface MetricsTableQueryMeter {

  String TABLE_READ_QUERY_PER_SECOND = "tableReadQueryPerSecond";
  String TABLE_WRITE_QUERY_PER_SECOND = "tableWriteQueryPerSecond";

  /**
   * Update table read QPS
   * @param tableName The table the metric is for
   * @param count Number of occurrences to record
   */
  void updateTableReadQueryMeter(TableName tableName, long count);

  /**
   * Update table read QPS
   * @param tableName The table the metric is for
   */
  void updateTableReadQueryMeter(TableName tableName);

  /**
   * Update table write QPS
   * @param tableName The table the metric is for
   * @param count Number of occurrences to record
   */
  void updateTableWriteQueryMeter(TableName tableName, long count);

  /**
   * Update table write QPS
   * @param tableName The table the metric is for
   */
  void updateTableWriteQueryMeter(TableName tableName);
}
