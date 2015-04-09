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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.TableName;

/**
 * Procedures that operates on a specific Table (e.g. create, delete, snapshot, ...)
 * must implement this interface to allow the system handle the lock/concurrency problems.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TableProcedureInterface {
  public enum TableOperationType { CREATE, DELETE, EDIT, READ };

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
