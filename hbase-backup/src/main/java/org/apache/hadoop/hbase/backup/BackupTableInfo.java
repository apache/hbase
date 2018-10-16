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

package org.apache.hadoop.hbase.backup;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;

/**
 * Backup related information encapsulated for a table. At this moment only target directory,
 * snapshot name and table name are encapsulated here.
 */

@InterfaceAudience.Private
public class BackupTableInfo  {
  /*
   *  Table name for backup
   */
  private TableName table;

  /*
   *  Snapshot name for offline/online snapshot
   */
  private String snapshotName = null;

  public BackupTableInfo() {
  }

  public BackupTableInfo(TableName table, String targetRootDir, String backupId) {
    this.table = table;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public void setSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
  }

  public TableName getTable() {
    return table;
  }

  public static BackupTableInfo convert(BackupProtos.BackupTableInfo proto) {
    BackupTableInfo bs = new BackupTableInfo();
    bs.table = ProtobufUtil.toTableName(proto.getTableName());
    if (proto.hasSnapshotName()) {
      bs.snapshotName = proto.getSnapshotName();
    }
    return bs;
  }

  public BackupProtos.BackupTableInfo toProto() {
    BackupProtos.BackupTableInfo.Builder builder = BackupProtos.BackupTableInfo.newBuilder();
    if (snapshotName != null) {
      builder.setSnapshotName(snapshotName);
    }
    builder.setTableName(ProtobufUtil.toProtoTableName(table));
    return builder.build();
  }
}
