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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RegionInfoBuilder {

  /** A non-capture group so that this can be embedded. */
  public static final String ENCODED_REGION_NAME_REGEX = "(?:[a-f0-9]+)";

  //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
  public static final String NO_HASH = null;

  public static final RegionInfo UNDEFINED =
    RegionInfoBuilder.newBuilder(TableName.valueOf("__UNDEFINED__")).build();

  /**
   * RegionInfo for first meta region
   * You cannot use this builder to make an instance of the {@link #FIRST_META_REGIONINFO}.
   * Just refer to this instance. Also, while the instance is actually a MutableRI, its type is
   * just RI so the mutable methods are not available (unless you go casting); it appears
   * as immutable (I tried adding Immutable type but it just makes a mess).
   */
  // TODO: How come Meta regions still do not have encoded region names? Fix.
  // hbase:meta,,1.1588230740 should be the hbase:meta first region name.
  public static final RegionInfo FIRST_META_REGIONINFO =
    new MutableRegionInfo(1L, TableName.META_TABLE_NAME, RegionInfo.DEFAULT_REPLICA_ID);

  private final TableName tableName;
  private byte[] startKey = HConstants.EMPTY_START_ROW;
  private byte[] endKey = HConstants.EMPTY_END_ROW;
  private long regionId = System.currentTimeMillis();
  private int replicaId = RegionInfo.DEFAULT_REPLICA_ID;
  private boolean offLine = false;
  private boolean split = false;

  public static RegionInfoBuilder newBuilder(TableName tableName) {
    return new RegionInfoBuilder(tableName);
  }

  public static RegionInfoBuilder newBuilder(RegionInfo regionInfo) {
    return new RegionInfoBuilder(regionInfo);
  }

  private RegionInfoBuilder(TableName tableName) {
    this.tableName = tableName;
  }

  private RegionInfoBuilder(RegionInfo regionInfo) {
    this.tableName = regionInfo.getTable();
    this.startKey = regionInfo.getStartKey();
    this.endKey = regionInfo.getEndKey();
    this.offLine = regionInfo.isOffline();
    this.split = regionInfo.isSplit();
    this.regionId = regionInfo.getRegionId();
    this.replicaId = regionInfo.getReplicaId();
  }

  public RegionInfoBuilder setStartKey(byte[] startKey) {
    this.startKey = startKey;
    return this;
  }

  public RegionInfoBuilder setEndKey(byte[] endKey) {
    this.endKey = endKey;
    return this;
  }

  public RegionInfoBuilder setRegionId(long regionId) {
    this.regionId = regionId;
    return this;
  }

  public RegionInfoBuilder setReplicaId(int replicaId) {
    this.replicaId = replicaId;
    return this;
  }

  public RegionInfoBuilder setSplit(boolean split) {
    this.split = split;
    return this;
  }

  @Deprecated
  public RegionInfoBuilder setOffline(boolean offLine) {
    this.offLine = offLine;
    return this;
  }

  public RegionInfo build() {
    return new MutableRegionInfo(tableName, startKey, endKey, split,
        regionId, replicaId, offLine);
  }

}
