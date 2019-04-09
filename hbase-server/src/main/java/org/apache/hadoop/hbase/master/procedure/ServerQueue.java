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
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class ServerQueue extends Queue<ServerName> {

  public ServerQueue(ServerName serverName, int priority, LockStatus serverLock) {
    super(serverName, priority, serverLock);
  }

  @Override
  public boolean requireExclusiveLock(Procedure<?> proc) {
    ServerProcedureInterface spi = (ServerProcedureInterface) proc;
    switch (spi.getServerOperationType()) {
      case CRASH_HANDLER:
        return true;
      case SWITCH_RPC_THROTTLE:
      case SPLIT_WAL:
      case SPLIT_WAL_REMOTE:
        return false;
      default:
        break;
    }
    throw new UnsupportedOperationException("unexpected type " + spi.getServerOperationType());
  }
}
