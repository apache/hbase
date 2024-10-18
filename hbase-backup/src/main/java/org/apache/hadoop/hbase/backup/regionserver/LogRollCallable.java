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
package org.apache.hadoop.hbase.backup.regionserver;

import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.RSLogRollParameter;

@InterfaceAudience.Private
public class LogRollCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(LogRollCallable.class);

  private String backupRoot;

  @Override
  protected void doCall() throws Exception {
    long highest = rs.getWALs().stream().mapToLong(wal -> ((AbstractFSWAL<?>) wal).getFilenum())
      .max().orElse(-1L);
    AbstractFSWAL<?> fsWAL = (AbstractFSWAL<?>) rs.getWAL(null);

    long filenum = fsWAL.getFilenum();
    LOG.info("Trying to roll log. Current log number={}, highest={}", filenum, highest);
    rs.getWalRoller().requestRollAll();
    rs.getWalRoller().waitUntilWalRollFinished();
    LOG.info("After roll log, Current log number={}", fsWAL.getFilenum());

    final String server = rs.getServerName().getAddress().toString();
    final BackupSystemTable table = new BackupSystemTable(rs.getConnection());
    long sts = table.getRegionServerLastLogRollResult(server, backupRoot);
    if (sts > highest) {
      LOG.warn("Won't update server's last roll log result: current={}, new={}", sts, highest);
    } else {
      table.writeRegionServerLastLogRollResult(server, highest, backupRoot);
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    this.backupRoot = RSLogRollParameter.parseFrom(parameter).getBackupRoot();
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_LOG_ROLL;
  }
}
