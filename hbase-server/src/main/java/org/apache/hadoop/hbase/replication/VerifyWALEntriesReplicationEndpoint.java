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
package org.apache.hadoop.hbase.replication;

import java.util.UUID;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A dummy {@link ReplicationEndpoint} that replicates nothing.
 * <p/>
 * Mainly used by ITBLL to check whether all the entries in WAL files are fine, since for normal
 * case, we will only read the WAL files when there are region servers crash and we need to split
 * the log, but for replication we will read all the entries and pass them to the
 * {@link ReplicationEndpoint}, so setting up a replication peer can help finding out whether there
 * are broken entries in WAL files.
 */
@InterfaceAudience.Private
public class VerifyWALEntriesReplicationEndpoint extends BaseReplicationEndpoint {

  @Override
  public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override
  public UUID getPeerUUID() {
    return ctx.getClusterId();
  }

  @Override
  public WALEntryFilter getWALEntryfilter() {
    return null;
  }

  private void checkCell(Cell cell) {
    // check whether all the fields are fine
    CellUtil.cloneRow(cell);
    CellUtil.cloneFamily(cell);
    CellUtil.cloneQualifier(cell);
    CellUtil.cloneValue(cell);
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    replicateContext.entries.stream().map(WAL.Entry::getEdit).flatMap(e -> e.getCells().stream())
      .forEach(this::checkCell);
    return true;
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }
}
