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
package org.apache.hadoop.hbase.client;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The POJO equivalent of HBaseProtos.SnapshotDescription
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SnapshotDescription {
  private String name;
  private String table;
  private SnapshotType snapShotType = SnapshotType.DISABLED;
  private String owner;
  private long creationTime = -1L;
  private int version = -1;

  public SnapshotDescription(String name) {
    this(name, null);
  }

  public SnapshotDescription(String name, String table) {
    this(name, table, SnapshotType.DISABLED, null);
  }

  public SnapshotDescription(String name, String table, SnapshotType type) {
    this(name, table, type, null);
  }

  public SnapshotDescription(String name, String table, SnapshotType type, String owner) {
    this(name, table, type, owner, -1, -1);
  }

  public SnapshotDescription(String name, String table, SnapshotType type, String owner,
      long creationTime, int version) {
    this.name = name;
    this.table = table;
    this.snapShotType = type;
    this.owner = owner;
    this.creationTime = creationTime;
    this.version = version;
  }

  public String getName() {
    return this.name;
  }

  public String getTable() {
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

  public int getVersion() {
    return this.version;
  }
}
