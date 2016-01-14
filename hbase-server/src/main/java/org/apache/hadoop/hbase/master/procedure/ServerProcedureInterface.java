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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Procedures that handle servers -- e.g. server crash -- must implement this Interface.
 * It is used by the procedure runner to figure locking and what queuing.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ServerProcedureInterface {
  public enum ServerOperationType {
    CRASH_HANDLER
  };

  /**
   * @return Name of this server instance.
   */
  ServerName getServerName();

  /**
   * @return True if this server has an hbase:meta table region.
   */
  boolean hasMetaTableRegion();

  /**
   * Given an operation type we can take decisions about what to do with pending operations.
   * e.g. if we get a crash handler and we have some assignment operation pending
   * we can abort those operations.
   * @return the operation type that the procedure is executing.
   */
  ServerOperationType getServerOperationType();
}
