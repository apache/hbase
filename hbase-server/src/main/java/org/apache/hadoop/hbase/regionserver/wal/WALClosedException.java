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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when {@link LogRoller} try to roll writer but the WAL already was closed. This may
 * happened when peer's sync replication state was transited from
 * {@link SyncReplicationState#ACTIVE} to {@link SyncReplicationState#DOWNGRADE_ACTIVE} and the
 * region's WAL was changed to a new one. But the old WAL was still left in {@link LogRoller}.
 */
@InterfaceAudience.Private
public class WALClosedException extends IOException {

  private static final long serialVersionUID = -3183198896865290678L;

  public WALClosedException() {
    super();
  }

  /**
   * @param msg exception message
   */
  public WALClosedException(String msg) {
    super(msg);
  }
}