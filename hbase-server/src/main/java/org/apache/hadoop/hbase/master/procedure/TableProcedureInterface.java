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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Procedures that operates on a specific Table (e.g. create, delete, snapshot, ...)
 * must implement this interface to allow the system handle the lock/concurrency problems.
 */
@InterfaceAudience.Private
public interface TableProcedureInterface {

  /**
   * Used for acquire/release lock for namespace related operations, just a place holder as we do
   * not have namespace table any more.
   */
  public static final TableName DUMMY_NAMESPACE_TABLE_NAME = TableName.NAMESPACE_TABLE_NAME;

  public enum TableOperationType {
    CREATE, DELETE, DISABLE, EDIT, ENABLE, READ,
    REGION_EDIT, REGION_SPLIT, REGION_MERGE, REGION_ASSIGN, REGION_UNASSIGN,
      REGION_GC, MERGED_REGIONS_GC/* region operations */
  }

  /**
   * @return the name of the table the procedure is operating on
   */
  TableName getTableName();

  /**
   * Given an operation type we can take decisions about what to do with pending operations.
   * e.g. if we get a delete and we have some table operation pending (e.g. add column)
   * we can abort those operations.
   * @return the operation type that the procedure is executing.
   */
  TableOperationType getTableOperationType();
}
