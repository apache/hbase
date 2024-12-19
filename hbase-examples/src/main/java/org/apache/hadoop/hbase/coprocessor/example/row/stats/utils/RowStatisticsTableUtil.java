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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.RowStatistics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class RowStatisticsTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(RowStatisticsTableUtil.class);
  public static final String NAMESPACE = "stats";
  public static final String TABLE_STRING = "row-statistics";
  public static final TableName NAMESPACED_TABLE_NAME = TableName.valueOf(NAMESPACE, TABLE_STRING);
  public static final byte[] CF = Bytes.toBytes("0");
  public static final String TABLE_RECORDER_KEY = "tableRecorder";

  private RowStatisticsTableUtil() {
  }

  public static Put buildPutForRegion(byte[] regionRowKey, RowStatistics rowStatistics,
    boolean isMajor) {
    Put put = new Put(regionRowKey);
    String cq = rowStatistics.getColumnFamily() + (isMajor ? "1" : "0");
    String jsonString = rowStatistics.getJsonString();
    put.addColumn(CF, Bytes.toBytes(cq), Bytes.toBytes(jsonString));
    LOG.debug(jsonString);
    return put;
  }
}
