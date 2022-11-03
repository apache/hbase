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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.WALEventTrackerPayload;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class WALEventTrackerListener implements WALActionsListener {
  private final Configuration conf;
  private final NamedQueueRecorder namedQueueRecorder;
  private final String serverName;

  public enum WalState {
    ROLLING,
    ROLLED,
    ACTIVE
  }

  public WALEventTrackerListener(Configuration conf, NamedQueueRecorder namedQueueRecorder,
    ServerName serverName) {
    this.conf = conf;
    this.namedQueueRecorder = namedQueueRecorder;
    this.serverName = serverName.getHostname();
  }

  @Override
  public void preLogRoll(Path oldPath, Path newPath) {
    if (oldPath != null) {
      // oldPath can be null for first wal
      // Just persist the last component of path not the whole walName which includes filesystem
      // scheme, walDir.
      WALEventTrackerPayload payloadForOldPath =
        getPayload(oldPath.getName(), WalState.ROLLING.name(), 0L);
      this.namedQueueRecorder.addRecord(payloadForOldPath);
    }
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) {
    // Create 2 entries entry in RingBuffer.
    // 1. Change state to Rolled for oldPath
    // 2. Change state to Active for newPath.
    if (oldPath != null) {
      // oldPath can be null for first wal
      // Just persist the last component of path not the whole walName which includes filesystem
      // scheme, walDir.

      long fileLength = 0L;
      try {
        FileSystem fs = oldPath.getFileSystem(this.conf);
        fileLength = fs.getFileStatus(oldPath).getLen();
      } catch (IOException ioe) {
        // Saving wal length is best effort. In case of any exception just ignore.
      }
      WALEventTrackerPayload payloadForOldPath =
        getPayload(oldPath.getName(), WalState.ROLLED.name(), fileLength);
      this.namedQueueRecorder.addRecord(payloadForOldPath);
    }

    WALEventTrackerPayload payloadForNewPath =
      getPayload(newPath.getName(), WalState.ACTIVE.name(), 0L);
    this.namedQueueRecorder.addRecord(payloadForNewPath);
  }

  private WALEventTrackerPayload getPayload(String path, String state, long walLength) {
    long timestamp = EnvironmentEdgeManager.currentTime();
    WALEventTrackerPayload payload =
      new WALEventTrackerPayload(serverName, path, timestamp, state, walLength);
    return payload;
  }
}
