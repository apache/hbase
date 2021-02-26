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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class MasterProcedureConstants {
  private MasterProcedureConstants() {}

  /** Number of threads used by the procedure executor */
  public static final String MASTER_PROCEDURE_THREADS = "hbase.master.procedure.threads";
  public static final int DEFAULT_MIN_MASTER_PROCEDURE_THREADS = 16;

  /**
   * Procedure replay sanity check. In case a WAL is missing or unreadable we
   * may lose information about pending/running procedures.
   * Set this to true in case you want the Master failing on load if a corrupted
   * procedure is encountred.
   * (Default is off, because we prefer having the Master up and running and
   * fix the "in transition" state "by hand")
   */
  public static final String EXECUTOR_ABORT_ON_CORRUPTION = "hbase.procedure.abort.on.corruption";
  public static final boolean DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION = false;
}
