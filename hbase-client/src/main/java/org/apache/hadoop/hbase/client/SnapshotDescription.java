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

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * The POJO equivalent of HBaseProtos.SnapshotDescription
 */
@InterfaceAudience.Public
public class SnapshotDescription {
  private final String name;
  private final TableName table;
  private final SnapshotType snapShotType;
  private final String owner;
  private final long creationTime;
  private final long ttl;
  private final int version;

  private final long maxFileSize;

  public SnapshotDescription(String name) {
    this(name, null);
  }

  public SnapshotDescription(String name, TableName table) {
    this(name, table, SnapshotType.DISABLED, null, -1, -1, null);
  }

  public SnapshotDescription(String name, TableName table, SnapshotType type) {
    this(name, table, type, null, -1, -1, null);
  }

  public SnapshotDescription(String name, TableName table, SnapshotType type, String owner) {
    this(name, table, type, owner, -1, -1, null);
  }

  /**
   * SnapshotDescription Parameterized Constructor
   *
   * @param name Name of the snapshot
   * @param table TableName associated with the snapshot
   * @param type Type of the snapshot - enum SnapshotType
   * @param owner Snapshot Owner
   * @param creationTime Creation time for Snapshot
   * @param version Snapshot Version
   * @deprecated since 2.3.0 and will be removed in 4.0.0. Use
   *   {@link #SnapshotDescription(String, TableName, SnapshotType, String, long, int, Map)}
   */
  @Deprecated
  public SnapshotDescription(String name, TableName table, SnapshotType type, String owner,
      long creationTime, int version) {
    this(name, table, type, owner, creationTime, version, null);
  }

  /**
   * SnapshotDescription Parameterized Constructor
   *
   * @param name          Name of the snapshot
   * @param table         TableName associated with the snapshot
   * @param type          Type of the snapshot - enum SnapshotType
   * @param owner         Snapshot Owner
   * @param creationTime  Creation time for Snapshot
   * @param version       Snapshot Version
   * @param snapshotProps Additional properties for snapshot e.g. TTL
   */
  public SnapshotDescription(String name, TableName table, SnapshotType type, String owner,
      long creationTime, int version, Map<String, Object> snapshotProps) {
    this.name = name;
    this.table = table;
    this.snapShotType = type;
    this.owner = owner;
    this.creationTime = creationTime;
    this.ttl = getLongFromSnapshotProps(snapshotProps, "TTL");
    this.version = version;
    this.maxFileSize = getLongFromSnapshotProps(snapshotProps, TableDescriptorBuilder.MAX_FILESIZE);
  }

  private long getLongFromSnapshotProps(Map<String, Object> snapshotProps, String property) {
    return MapUtils.getLongValue(snapshotProps, property, -1);
  }



  /**
   * SnapshotDescription Parameterized Constructor
   *
   * @param snapshotName  Name of the snapshot
   * @param tableName     TableName associated with the snapshot
   * @param type          Type of the snapshot - enum SnapshotType
   * @param snapshotProps Additional properties for snapshot e.g. TTL
   */
  public SnapshotDescription(String snapshotName, TableName tableName, SnapshotType type,
                             Map<String, Object> snapshotProps) {
    this(snapshotName, tableName, type, null, -1, -1, snapshotProps);
  }

  public String getName() {
    return this.name;
  }

  public String getTableNameAsString() {
    return this.table.getNameAsString();
  }

  public TableName getTableName() {
    return this.table;
  }

  public SnapshotType getType() {
    return this.snapShotType;
  }

  public String getOwner() {
    return this.owner;
  }

  public long getCreationTime() {
    return this.creationTime;
  }

  // get snapshot ttl in sec
  public long getTtl() {
    return ttl;
  }

  public int getVersion() {
    return this.version;
  }

  public long getMaxFileSize() { return maxFileSize; }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("name", name)
      .append("table", table)
      .append("snapShotType", snapShotType)
      .append("owner", owner)
      .append("creationTime", creationTime)
      .append("ttl", ttl)
      .append("version", version)
      .append("maxFileSize", maxFileSize)
      .toString();
  }
}
