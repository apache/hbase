/*
 *
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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReplicationStateImpl is responsible for maintaining the replication state
 * znode.
 */
public class ReplicationStateImpl extends ReplicationStateZKBase implements
    ReplicationStateInterface {

  private final ReplicationStateTracker stateTracker;
  private final AtomicBoolean replicating;

  private static final Log LOG = LogFactory.getLog(ReplicationStateImpl.class);

  public ReplicationStateImpl(final ZooKeeperWatcher zk, final Configuration conf,
      final Abortable abortable, final AtomicBoolean replicating) {
    super(zk, conf, abortable);
    this.replicating = replicating;

    // Set a tracker on replicationStateNode
    this.stateTracker =
        new ReplicationStateTracker(this.zookeeper, this.stateZNode, this.abortable);
  }

  public ReplicationStateImpl(final ZooKeeperWatcher zk, final Configuration conf,
      final Abortable abortable) {
    this(zk, conf, abortable, new AtomicBoolean());
  }

  @Override
  public void init() throws KeeperException {
    ZKUtil.createWithParents(this.zookeeper, this.stateZNode);
    stateTracker.start();
    readReplicationStateZnode();
  }

  @Override
  public boolean getState() throws KeeperException {
    return getReplication();
  }

  @Override
  public void setState(boolean newState) throws KeeperException {
    setReplicating(newState);
  }

  @Override
  public void close() throws IOException {
    if (stateTracker != null) stateTracker.stop();
  }

  /**
   * @param bytes
   * @return True if the passed in <code>bytes</code> are those of a pb
   *         serialized ENABLED state.
   * @throws DeserializationException
   */
  private boolean isStateEnabled(final byte[] bytes) throws DeserializationException {
    ZooKeeperProtos.ReplicationState.State state = parseStateFrom(bytes);
    return ZooKeeperProtos.ReplicationState.State.ENABLED == state;
  }

  /**
   * @param bytes Content of a state znode.
   * @return State parsed from the passed bytes.
   * @throws DeserializationException
   */
  private ZooKeeperProtos.ReplicationState.State parseStateFrom(final byte[] bytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.ReplicationState.Builder builder = ZooKeeperProtos.ReplicationState
        .newBuilder();
    ZooKeeperProtos.ReplicationState state;
    try {
      state = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
      return state.getState();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * Set the new replication state for this cluster
   * @param newState
   */
  private void setReplicating(boolean newState) throws KeeperException {
    ZKUtil.createWithParents(this.zookeeper, this.stateZNode);
    byte[] stateBytes = (newState == true) ? ENABLED_ZNODE_BYTES : DISABLED_ZNODE_BYTES;
    ZKUtil.setData(this.zookeeper, this.stateZNode, stateBytes);
  }

  /**
   * Get the replication status of this cluster. If the state znode doesn't
   * exist it will also create it and set it true.
   * @return returns true when it's enabled, else false
   * @throws KeeperException
   */
  private boolean getReplication() throws KeeperException {
    byte[] data = this.stateTracker.getData(false);
    if (data == null || data.length == 0) {
      setReplicating(true);
      return true;
    }
    try {
      return isStateEnabled(data);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }

  /**
   * This reads the state znode for replication and sets the atomic boolean
   */
  private void readReplicationStateZnode() {
    try {
      this.replicating.set(getReplication());
      LOG.info("Replication is now " + (this.replicating.get() ? "started" : "stopped"));
    } catch (KeeperException e) {
      this.abortable.abort("Failed getting data on from " + this.stateZNode, e);
    }
  }

  /**
   * Tracker for status of the replication
   */
  private class ReplicationStateTracker extends ZooKeeperNodeTracker {
    public ReplicationStateTracker(ZooKeeperWatcher watcher, String stateZnode, Abortable abortable) {
      super(watcher, stateZnode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readReplicationStateZnode();
      }
    }
  }
}