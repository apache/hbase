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

package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;
import java.util.Map;

public class MetricsTableWrapperStub implements MetricsTableWrapperAggregate {

  private String tableName;

  public MetricsTableWrapperStub(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public long getReadRequestCount(String table) {
    return 10;
  }

  @Override
  public long getWriteRequestCount(String table) {
    return 20;
  }

  @Override
  public long getTotalRequestsCount(String table) {
    return 30;
  }

  @Override
  public long getFilteredReadRequestCount(String table) {
    return 40;
  }

  @Override
  public long getMemStoreSize(String table) {
    return 1000;
  }

  @Override
  public long getStoreFileSize(String table) {
    return 2000;
  }

  @Override
  public long getTableSize(String table) {
    return 3000;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public long getNumRegions(String table) {
    return 11;
  }

  @Override
  public long getNumStores(String table) {
    return 22;
  }

  @Override
  public long getNumStoreFiles(String table) {
    return 33;
  }

  @Override
  public long getMaxStoreFileAge(String table) {
    return 44;
  }

  @Override
  public long getMinStoreFileAge(String table) {
    return 55;
  }

  @Override
  public long getAvgStoreFileAge(String table) {
    return 66;
  }

  @Override
  public long getNumReferenceFiles(String table) {
    return 77;
  }

  @Override
  public long getAvgRegionSize(String table) {
    return 88;
  }

  @Override
  public Map<String, Long> getMemstoreOnlyRowReadsCount(String table) {
    Map<String, Long> map = new HashMap<String, Long>();
    map.put("table#info", 3L);
    return map;
  }

  @Override
  public Map<String, Long> getMixedRowReadsCount(String table) {
    Map<String, Long> map = new HashMap<String, Long>();
    map.put("table#info", 3L);
    return map;
  }
}
