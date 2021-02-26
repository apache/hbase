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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class TableQueue extends Queue<TableName> {
  private final LockStatus namespaceLockStatus;

  public TableQueue(TableName tableName, int priority, LockStatus tableLock,
      LockStatus namespaceLockStatus) {
    super(tableName, priority, tableLock);
    this.namespaceLockStatus = namespaceLockStatus;
  }

  @Override
  public boolean isAvailable() {
    return !isEmpty() && !namespaceLockStatus.hasExclusiveLock();
  }

  @Override
  public boolean requireExclusiveLock(Procedure<?> proc) {
    return requireTableExclusiveLock((TableProcedureInterface) proc);
  }

  /**
   * @param proc must not be null
   */
  private static boolean requireTableExclusiveLock(TableProcedureInterface proc) {
    switch (proc.getTableOperationType()) {
      case CREATE:
      case DELETE:
      case DISABLE:
      case ENABLE:
        return true;
      case EDIT:
        // we allow concurrent edit on the NS table
        return !proc.getTableName().equals(TableName.NAMESPACE_TABLE_NAME);
      case READ:
        return false;
      // region operations are using the shared-lock on the table
      // and then they will grab an xlock on the region.
      case REGION_SPLIT:
      case REGION_MERGE:
      case REGION_ASSIGN:
      case REGION_UNASSIGN:
      case REGION_EDIT:
      case REGION_GC:
      case MERGED_REGIONS_GC:
        return false;
      default:
        break;
    }
    throw new UnsupportedOperationException("unexpected type " + proc.getTableOperationType());
  }
}
