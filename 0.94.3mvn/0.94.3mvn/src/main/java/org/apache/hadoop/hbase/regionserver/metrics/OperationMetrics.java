/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver.metrics;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class provides a simplified interface to expose time varying metrics
 * about GET/DELETE/PUT/ICV operations on a region and on Column Families. All
 * metrics are stored in {@link RegionMetricsStorage} and exposed to hadoop
 * metrics through {@link RegionServerDynamicMetrics}.
 */
public class OperationMetrics {

  private static final String DELETE_KEY = "delete_";
  private static final String PUT_KEY = "put_";
  private static final String GET_KEY = "get_";
  private static final String ICV_KEY = "incrementColumnValue_";
  private static final String INCREMENT_KEY = "increment_";
  private static final String MULTIPUT_KEY = "multiput_";
  private static final String MULTIDELETE_KEY = "multidelete_";
  private static final String APPEND_KEY = "append_";
  
  /** Conf key controlling whether we should expose metrics.*/
  private static final String CONF_KEY =
      "hbase.metrics.exposeOperationTimes";

  private final String tableName;
  private final String regionName;
  private final String regionMetrixPrefix;
  private final Configuration conf;
  private final boolean exposeTimes;


  /**
   * Create a new OperationMetrics
   * @param conf The Configuration of the HRegion reporting operations coming in.
   * @param regionInfo The region info
   */
  public OperationMetrics(Configuration conf, HRegionInfo regionInfo) { 
    // Configure SchemaMetrics before trying to create a RegionOperationMetrics instance as
    // RegionOperationMetrics relies on SchemaMetrics to do naming.
    if (conf != null) {
      SchemaMetrics.configureGlobally(conf);
      
      this.conf = conf;
      if (regionInfo != null) {
        this.tableName = regionInfo.getTableNameAsString();
        this.regionName = regionInfo.getEncodedName();
      } else {
        this.tableName = SchemaMetrics.UNKNOWN;
        this.regionName = SchemaMetrics.UNKNOWN;
      }
      this.regionMetrixPrefix =
          SchemaMetrics.generateRegionMetricsPrefix(this.tableName, this.regionName);
      this.exposeTimes = this.conf.getBoolean(CONF_KEY, true);
    } else {
      //Make all the final values happy.
      this.conf = null;
      this.tableName = null;
      this.regionName = null;
      this.regionMetrixPrefix = null;
      this.exposeTimes = false;
    }
  }
  
  /**
   * This is used in creating a testing HRegion where the regionInfo is unknown
   * @param conf
   */
  public OperationMetrics() {
    this(null, null);
  }


  /**
   * Update the stats associated with {@link HTable#put(java.util.List)}.
   * 
   * @param columnFamilies Set of CF's this multiput is associated with
   * @param value the time
   */
  public void updateMultiPutMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, MULTIPUT_KEY, value);
  }

  /**
   * Update the stats associated with {@link HTable#delete(java.util.List)}.
   *
   * @param columnFamilies Set of CF's this multidelete is associated with
   * @param value the time
   */
  public void updateMultiDeleteMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, MULTIDELETE_KEY, value);
  }
  
  /**
   * Update the metrics associated with a {@link Get}
   * 
   * @param columnFamilies
   *          Set of Column Families in this get.
   * @param value
   *          the time
   */
  public void updateGetMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, GET_KEY, value);
  }
  
  /**
   * Update metrics associated with an {@link Increment}
   * @param columnFamilies
   * @param value
   */
  public void updateIncrementMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, INCREMENT_KEY, value);
  }
  
  
  /**
   * Update the metrics associated with an {@link Append}
   * @param columnFamilies
   * @param value
   */
  public void updateAppendMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, APPEND_KEY, value);
  }


  /**
   * Update the metrics associated with
   * {@link HTable#incrementColumnValue(byte[], byte[], byte[], long)}
   * 
   * @param columnFamily
   *          The single column family associated with an ICV
   * @param value
   *          the time
   */
  public void updateIncrementColumnValueMetrics(byte[] columnFamily, long value) {
    String cfMetricPrefix =
        SchemaMetrics.generateSchemaMetricsPrefix(this.tableName, Bytes.toString(columnFamily));
    doSafeIncTimeVarying(cfMetricPrefix, ICV_KEY, value);
    doSafeIncTimeVarying(this.regionMetrixPrefix, ICV_KEY, value);
  }

  /**
   * update metrics associated with a {@link Put}
   * 
   * @param columnFamilies
   *          Set of column families involved.
   * @param value
   *          the time.
   */
  public void updatePutMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, PUT_KEY, value);
  }

  /**
   * update metrics associated with a {@link Delete}
   * 
   * @param columnFamilies
   * @param value
   *          the time.
   */
  public void updateDeleteMetrics(Set<byte[]> columnFamilies, long value) {
    doUpdateTimeVarying(columnFamilies, DELETE_KEY, value);
  }
  
  /**
   * This deletes all old metrics this instance has ever created or updated.
   */
  public void closeMetrics() {
    RegionMetricsStorage.clear();
  }

  /**
   * Method to send updates for cf and region metrics. This is the normal method
   * used if the naming of stats and CF's are in line with put/delete/multiput.
   * 
   * @param columnFamilies
   *          the set of column families involved.
   * @param key
   *          the metric name.
   * @param value
   *          the time.
   */
  private void doUpdateTimeVarying(Set<byte[]> columnFamilies, String key, long value) {
    String cfPrefix = null;
    if (columnFamilies != null) {
      cfPrefix = SchemaMetrics.generateSchemaMetricsPrefix(tableName, columnFamilies);
    } else {
      cfPrefix = SchemaMetrics.generateSchemaMetricsPrefix(tableName, SchemaMetrics.UNKNOWN);
    }

    doSafeIncTimeVarying(cfPrefix, key, value);
    doSafeIncTimeVarying(this.regionMetrixPrefix, key, value);
  }

  private void doSafeIncTimeVarying(String prefix, String key, long value) {
    if (exposeTimes) {
      if (prefix != null && !prefix.isEmpty() && key != null && !key.isEmpty()) {
        String m = prefix + key;
        RegionMetricsStorage.incrTimeVaryingMetric(m, value);
      }
    }
  }

}
