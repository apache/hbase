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
package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Performs Append operations on a single row.
 * <p>
 * Note that this operation does not appear atomic to readers. Appends are done
 * under a single row lock, so write operations to a row are synchronized, but
 * readers do not take row locks so get and scan operations can see this
 * operation partially completed.
 * <p>
 * To append to a set of columns of a row, instantiate an Append object with the
 * row to append to. At least one column to append must be specified using the
 * {@link #add(byte[], byte[], byte[])} method.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Append extends Mutation {
  /**
   * @param returnResults
   *          True (default) if the append operation should return the results.
   *          A client that is not interested in the result can save network
   *          bandwidth setting this to false.
   */
  public Append setReturnResults(boolean returnResults) {
    super.setReturnResults(returnResults);
    return this;
  }

  /**
   * @return current setting for returnResults
   */
  // This method makes public the superclasses's protected method.
  public boolean isReturnResults() {
    return super.isReturnResults();
  }

  /**
   * Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param row row key; makes a local copy of passed in array.
   */
  public Append(byte[] row) {
    this(row, 0, row.length);
  }
  /**
   * Copy constructor
   * @param a
   */
  public Append(Append a) {
    this.row = a.getRow();
    this.ts = a.getTimeStamp();
    this.familyMap.putAll(a.getFamilyCellMap());
    for (Map.Entry<String, byte[]> entry : a.getAttributesMap().entrySet()) {
      this.setAttribute(entry.getKey(), entry.getValue());
    }
  }

  /** Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param rowArray Makes a copy out of this buffer.
   * @param rowOffset
   * @param rowLength
   */
  public Append(final byte [] rowArray, final int rowOffset, final int rowLength) {
    checkRow(rowArray, rowOffset, rowLength);
    this.row = Bytes.copy(rowArray, rowOffset, rowLength);
  }

  /**
   * Add the specified column and value to this Append operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value value to append to specified column
   * @return this
   */
  public Append add(byte [] family, byte [] qualifier, byte [] value) {
    KeyValue kv = new KeyValue(this.row, family, qualifier, this.ts, KeyValue.Type.Put, value);
    return add(kv);
  }

  /**
   * Add column and value to this Append operation.
   * @param cell
   * @return This instance
   */
  @SuppressWarnings("unchecked")
  public Append add(final Cell cell) {
    // Presume it is KeyValue for now.
    byte [] family = CellUtil.cloneFamily(cell);
    List<Cell> list = this.familyMap.get(family);
    if (list == null) {
      list  = new ArrayList<Cell>();
    }
    // find where the new entry should be placed in the List
    list.add(cell);
    this.familyMap.put(family, list);
    return this;
  }

  @Override
  public Append setAttribute(String name, byte[] value) {
    return (Append) super.setAttribute(name, value);
  }

  @Override
  public Append setId(String id) {
    return (Append) super.setId(id);
  }

  @Override
  public Append setDurability(Durability d) {
    return (Append) super.setDurability(d);
  }

  @Override
  public Append setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
    return (Append) super.setFamilyCellMap(map);
  }

  @Override
  public Append setClusterIds(List<UUID> clusterIds) {
    return (Append) super.setClusterIds(clusterIds);
  }

  @Override
  public Append setCellVisibility(CellVisibility expression) {
    return (Append) super.setCellVisibility(expression);
  }

  @Override
  public Append setACL(String user, Permission perms) {
    return (Append) super.setACL(user, perms);
  }

  @Override
  public Append setACL(Map<String, Permission> perms) {
    return (Append) super.setACL(perms);
  }

  @Override
  public Append setTTL(long ttl) {
    return (Append) super.setTTL(ttl);
  }
}
