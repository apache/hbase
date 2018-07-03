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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ReplicationStatus {
  private final String peerId;
  private final String walGroup;
  private final Path currentPath;
  private final int queueSize;
  private final long ageOfLastShippedOp;
  private final long replicationDelay;
  private final long currentPosition;
  private final long fileSize;

  private ReplicationStatus(ReplicationStatusBuilder builder) {
    this.peerId = builder.peerId;
    this.walGroup = builder.walGroup;
    this.currentPath = builder.currentPath;
    this.queueSize = builder.queueSize;
    this.ageOfLastShippedOp = builder.ageOfLastShippedOp;
    this.replicationDelay = builder.replicationDelay;
    this.currentPosition = builder.currentPosition;
    this.fileSize = builder.fileSize;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String getPeerId() {
    return peerId;
  }

  public String getWalGroup() {
    return walGroup;
  }

  public int getQueueSize() {
    return queueSize;
  }

  public long getAgeOfLastShippedOp() {
    return ageOfLastShippedOp;
  }

  public long getReplicationDelay() {
    return replicationDelay;
  }

  public Path getCurrentPath() {
    return currentPath;
  }

  public static ReplicationStatusBuilder newBuilder() {
    return new ReplicationStatusBuilder();
  }

  public static class ReplicationStatusBuilder {
    private String peerId = "UNKNOWN";
    private String walGroup = "UNKNOWN";
    private Path currentPath = new Path("UNKNOWN");
    private int queueSize = -1;
    private long ageOfLastShippedOp = -1;
    private long replicationDelay = -1;
    private long currentPosition = -1;
    private long fileSize = -1;

    public ReplicationStatusBuilder withPeerId(String peerId) {
      this.peerId = peerId;
      return this;
    }

    public ReplicationStatusBuilder withFileSize(long fileSize) {
      this.fileSize = fileSize;
      return this;
    }

    public ReplicationStatusBuilder withWalGroup(String walGroup) {
      this.walGroup = walGroup;
      return this;
    }

    public ReplicationStatusBuilder withCurrentPath(Path currentPath) {
      this.currentPath = currentPath;
      return this;
    }

    public ReplicationStatusBuilder withQueueSize(int queueSize) {
      this.queueSize = queueSize;
      return this;
    }

    public ReplicationStatusBuilder withAgeOfLastShippedOp(long ageOfLastShippedOp) {
      this.ageOfLastShippedOp = ageOfLastShippedOp;
      return this;
    }

    public ReplicationStatusBuilder withReplicationDelay(long replicationDelay) {
      this.replicationDelay = replicationDelay;
      return this;
    }

    public ReplicationStatusBuilder withCurrentPosition(long currentPosition) {
      this.currentPosition = currentPosition;
      return this;
    }

    public ReplicationStatus build() {
      return new ReplicationStatus(this);
    }
  }
}
