/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.namespace;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;

import com.google.common.base.Joiner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * NamespaceTableAndRegionInfo is a helper class that contains information about current state of
 * tables and regions in a namespace.
 */
@InterfaceAudience.Private
class NamespaceTableAndRegionInfo {
  private String name;
  private Map<TableName, AtomicInteger> tableAndRegionInfo;

  public NamespaceTableAndRegionInfo(String namespace) {
    this.name = namespace;
    this.tableAndRegionInfo = new HashMap<TableName, AtomicInteger>();
  }

  /**
   * Gets the name of the namespace.
   * @return name of the namespace.
   */
  String getName() {
    return name;
  }

  /**
   * Gets the set of table names belonging to namespace.
   * @return A set of table names.
   */
  synchronized Set<TableName> getTables() {
    return this.tableAndRegionInfo.keySet();
  }

  /**
   * Gets the total number of regions in namespace.
   * @return the region count
   */
  synchronized int getRegionCount() {
    int regionCount = 0;
    for (Entry<TableName, AtomicInteger> entry : this.tableAndRegionInfo.entrySet()) {
      regionCount = regionCount + entry.getValue().get();
    }
    return regionCount;
  }

  synchronized int getRegionCountOfTable(TableName tableName) {
    if (tableAndRegionInfo.containsKey(tableName)) {
      return this.tableAndRegionInfo.get(tableName).get();
    } else {
      return -1;
    }
  }

  synchronized boolean containsTable(TableName tableName) {
    return this.tableAndRegionInfo.containsKey(tableName);
  }

  synchronized void addTable(TableName tableName, int regionCount) {
    if (!name.equalsIgnoreCase(tableName.getNamespaceAsString())) {
      throw new IllegalStateException("Table : " + tableName + " does not belong to namespace "
          + name);
    }
    if (!tableAndRegionInfo.containsKey(tableName)) {
      tableAndRegionInfo.put(tableName, new AtomicInteger(regionCount));
    } else {
      throw new IllegalStateException("Table already in the cache " + tableName);
    }
  }

  synchronized void removeTable(TableName tableName) {
    tableAndRegionInfo.remove(tableName);
  }

  synchronized int incRegionCountForTable(TableName tableName, int count) {
    return tableAndRegionInfo.get(tableName).addAndGet(count);
  }

  synchronized int decrementRegionCountForTable(TableName tableName, int count) {
    return tableAndRegionInfo.get(tableName).decrementAndGet();
  }

  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
    return "NamespaceTableAndRegionInfo [name=" + name + ", tableAndRegionInfo="
        + mapJoiner.join(tableAndRegionInfo) + "]";
  }
}
