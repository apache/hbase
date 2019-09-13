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

package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotCleanupProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;


/**
 * Tracks status of snapshot auto cleanup based on TTL
 */
@InterfaceAudience.Private
public class SnapshotCleanupTracker extends ZooKeeperNodeTracker {

  /**
   * Constructs a new ZK node tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher reference to the {@link ZooKeeperWatcher} which also contains configuration
   *   and constants
   * @param abortable used to abort if a fatal error occurs
   */
  public SnapshotCleanupTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.snapshotCleanupZNode, abortable);
  }

  /**
   * Returns the current state of the snapshot auto cleanup based on TTL
   *
   * @return <code>true</code> if the snapshot auto cleanup is enabled,
   *   <code>false</code> otherwise.
   */
  public boolean isSnapshotCleanupEnabled() {
    byte[] snapshotCleanupZNodeData = super.getData(false);
    try {
      // if data in ZK is null, use default of on.
      return snapshotCleanupZNodeData == null ||
          parseFrom(snapshotCleanupZNodeData).getSnapshotCleanupEnabled();
    } catch (DeserializationException dex) {
      LOG.error("ZK state for Snapshot Cleanup could not be parsed " +
          Bytes.toStringBinary(snapshotCleanupZNodeData), dex);
      // return false to be safe.
      return false;
    }
  }

  /**
   * Set snapshot auto clean on/off
   *
   * @param snapshotCleanupEnabled true if the snapshot auto cleanup should be on,
   *   false otherwise
   * @throws KeeperException if ZooKeeper operation fails
   */
  public void setSnapshotCleanupEnabled(final boolean snapshotCleanupEnabled)
      throws KeeperException {
    byte [] snapshotCleanupZNodeData = toByteArray(snapshotCleanupEnabled);
    try {
      ZKUtil.setData(watcher, watcher.snapshotCleanupZNode,
          snapshotCleanupZNodeData);
    } catch(KeeperException.NoNodeException nne) {
      ZKUtil.createAndWatch(watcher, watcher.snapshotCleanupZNode,
          snapshotCleanupZNodeData);
    }
    super.nodeDataChanged(watcher.snapshotCleanupZNode);
  }

  private byte[] toByteArray(final boolean isSnapshotCleanupEnabled) {
    SnapshotCleanupProtos.SnapshotCleanupState.Builder builder =
        SnapshotCleanupProtos.SnapshotCleanupState.newBuilder();
    builder.setSnapshotCleanupEnabled(isSnapshotCleanupEnabled);
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  private SnapshotCleanupProtos.SnapshotCleanupState parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(pbBytes);
    SnapshotCleanupProtos.SnapshotCleanupState.Builder builder =
        SnapshotCleanupProtos.SnapshotCleanupState.newBuilder();
    try {
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, pbBytes, magicLen, pbBytes.length - magicLen);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return builder.build();
  }

}
