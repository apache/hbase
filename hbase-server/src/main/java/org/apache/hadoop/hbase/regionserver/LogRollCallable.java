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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.LogRollRemoteProcedureResult;

@InterfaceAudience.Private
public class LogRollCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(LogRollCallable.class);

  private int maxRollRetry;

  @Override
  protected byte[] doCall() throws Exception {
    for (int nAttempt = 0; nAttempt < maxRollRetry; nAttempt++) {
      try {
        Pair<Long, Long> filenumPairBefore = getFilenumPair();

        rs.getWalRoller().requestRollAll();
        rs.getWalRoller().waitUntilWalRollFinished();

        Pair<Long, Long> filenumPairAfter = getFilenumPair();
        LOG.info(
          "Before rolling log, highest filenum = {} default WAL filenum = {}, After "
            + "rolling log, highest filenum = {} default WAL filenum = {}",
          filenumPairBefore.getFirst(), filenumPairBefore.getSecond(), filenumPairAfter.getFirst(),
          filenumPairAfter.getSecond());
        return LogRollRemoteProcedureResult.newBuilder()
          .setServerName(ProtobufUtil.toServerName(rs.getServerName()))
          .setLastHighestWalFilenum(filenumPairBefore.getFirst()).build().toByteArray();
      } catch (Exception e) {
        LOG.warn("Failed rolling log on attempt={}", nAttempt, e);
        if (nAttempt == maxRollRetry - 1) {
          throw e;
        }
      }
    }
    return null;
  }

  private Pair<Long, Long> getFilenumPair() throws IOException {
    long highestFilenum = rs.getWALs().stream()
      .mapToLong(wal -> ((AbstractFSWAL<?>) wal).getFilenum()).max().orElse(-1L);
    long defaultWALFilenum = ((AbstractFSWAL<?>) rs.getWAL(null)).getFilenum();
    return Pair.newPair(highestFilenum, defaultWALFilenum);
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    this.maxRollRetry = rs.getConfiguration().getInt(AbstractWALRoller.WAL_ROLL_RETRIES, 1);
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_LOG_ROLL;
  }
}
