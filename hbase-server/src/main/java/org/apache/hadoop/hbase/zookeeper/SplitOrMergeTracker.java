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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SwitchState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;


/**
 * Tracks the switch of split and merge states in ZK
 *
 */
@InterfaceAudience.Private
public class SplitOrMergeTracker {

  public static final String LOCK = "splitOrMergeLock";
  public static final String STATE = "splitOrMergeState";

  private String splitZnode;
  private String mergeZnode;
  private String splitOrMergeLock;
  private ZooKeeperWatcher watcher;

  private SwitchStateTracker splitStateTracker;
  private SwitchStateTracker mergeStateTracker;

  public SplitOrMergeTracker(ZooKeeperWatcher watcher, Configuration conf,
                             Abortable abortable) {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getSwitchZNode()) < 0) {
        ZKUtil.createAndFailSilent(watcher, watcher.getSwitchZNode());
      }
      if (ZKUtil.checkExists(watcher, watcher.getSwitchLockZNode()) < 0) {
        ZKUtil.createAndFailSilent(watcher, watcher.getSwitchLockZNode());
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
    splitZnode = ZKUtil.joinZNode(watcher.getSwitchZNode(),
      conf.get("zookeeper.znode.switch.split", "split"));
    mergeZnode = ZKUtil.joinZNode(watcher.getSwitchZNode(),
      conf.get("zookeeper.znode.switch.merge", "merge"));

    splitOrMergeLock = ZKUtil.joinZNode(watcher.getSwitchLockZNode(), LOCK);

    splitStateTracker = new SwitchStateTracker(watcher, splitZnode, abortable);
    mergeStateTracker = new SwitchStateTracker(watcher, mergeZnode, abortable);
    this.watcher = watcher;
  }

  public void start() {
    splitStateTracker.start();
    mergeStateTracker.start();
  }

  public boolean isSplitOrMergeEnabled(Admin.MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return splitStateTracker.isSwitchEnabled();
      case MERGE:
        return mergeStateTracker.isSwitchEnabled();
      default:
        break;
    }
    return false;
  }

  public void setSplitOrMergeEnabled(boolean enabled, Admin.MasterSwitchType switchType)
    throws KeeperException {
    switch (switchType) {
      case SPLIT:
        splitStateTracker.setSwitchEnabled(enabled);
        break;
      case MERGE:
        mergeStateTracker.setSwitchEnabled(enabled);
        break;
      default:
        break;
    }
  }

  /**
   *  rollback the original state and delete lock node.
   * */
  public void releaseLockAndRollback()
    throws KeeperException, DeserializationException, InterruptedException {
    if (ZKUtil.checkExists(watcher, splitOrMergeLock) != -1) {
      List<ZKUtil.ZKUtilOp> ops = new ArrayList<>();
      rollback(ops);
      ops.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(splitOrMergeLock));
      ZKUtil.multiOrSequential(watcher, ops, false);
    }
  }

  // If there is old states of switch on zk, do rollback
  private void rollback(List<ZKUtil.ZKUtilOp> ops) throws KeeperException, InterruptedException, DeserializationException {
    String splitOrMergeState = ZKUtil.joinZNode(watcher.getSwitchLockZNode(),
      SplitOrMergeTracker.STATE);
    if (ZKUtil.checkExists(watcher, splitOrMergeState) != -1) {
      byte[] bytes = ZKUtil.getData(watcher, splitOrMergeState);
      ProtobufUtil.expectPBMagicPrefix(bytes);
      ZooKeeperProtos.SplitAndMergeState.Builder builder =
        ZooKeeperProtos.SplitAndMergeState.newBuilder();
      try {
        int magicLen = ProtobufUtil.lengthOfPBMagic();
        ProtobufUtil.mergeFrom(builder, bytes, magicLen, bytes.length - magicLen);
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      ZooKeeperProtos.SplitAndMergeState splitAndMergeState =  builder.build();
      splitStateTracker.setSwitchEnabled(splitAndMergeState.hasSplitEnabled());
      mergeStateTracker.setSwitchEnabled(splitAndMergeState.hasMergeEnabled());
      ops.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(splitOrMergeState));
    }
  }

  /**
   *  If there is no lock, you could acquire the lock.
   *  After we create lock on zk, we save original splitOrMerge switches on zk.
   *  @param skipLock if true, it means we will skip the lock action
   *                  but we still need to check whether the lock exists or not.
   *  @return true, lock successfully.  otherwise, false
   * */
  public boolean lock(boolean skipLock) throws KeeperException {
    if (ZKUtil.checkExists(watcher, splitOrMergeLock) != -1) {
      return false;
    }
    if (skipLock) {
      return true;
    }
    ZKUtil.createAndFailSilent(watcher,  splitOrMergeLock);
    if (ZKUtil.checkExists(watcher, splitOrMergeLock) != -1) {
      saveOriginalState();
      return true;
    }
    return false;
  }

  private void saveOriginalState() throws KeeperException {
    boolean splitEnabled = isSplitOrMergeEnabled(Admin.MasterSwitchType.SPLIT);
    boolean mergeEnabled = isSplitOrMergeEnabled(Admin.MasterSwitchType.MERGE);
    String splitOrMergeStates = ZKUtil.joinZNode(watcher.getSwitchLockZNode(),
      SplitOrMergeTracker.STATE);
    ZooKeeperProtos.SplitAndMergeState.Builder builder
      = ZooKeeperProtos.SplitAndMergeState.newBuilder();
    builder.setSplitEnabled(splitEnabled);
    builder.setMergeEnabled(mergeEnabled);
    ZKUtil.createSetData(watcher,  splitOrMergeStates,
      ProtobufUtil.prependPBMagic(builder.build().toByteArray()));
  }

  private static class SwitchStateTracker extends ZooKeeperNodeTracker {

    public SwitchStateTracker(ZooKeeperWatcher watcher, String node, Abortable abortable) {
      super(watcher, node, abortable);
    }

    /**
     * Return true if the switch is on, false otherwise
     */
    public boolean isSwitchEnabled() {
      byte [] upData = super.getData(false);
      try {
        // if data in ZK is null, use default of on.
        return upData == null || parseFrom(upData).getEnabled();
      } catch (DeserializationException dex) {
        LOG.error("ZK state for LoadBalancer could not be parsed " + Bytes.toStringBinary(upData));
        // return false to be safe.
        return false;
      }
    }

    /**
     * Set the switch on/off
     * @param enabled switch enabled or not?
     * @throws KeeperException keepException will be thrown out
     */
    public void setSwitchEnabled(boolean enabled) throws KeeperException {
      byte [] upData = toByteArray(enabled);
      try {
        ZKUtil.setData(watcher, node, upData);
      } catch(KeeperException.NoNodeException nne) {
        ZKUtil.createAndWatch(watcher, node, upData);
      }
      super.nodeDataChanged(node);
    }

    private byte [] toByteArray(boolean enabled) {
      SwitchState.Builder builder = SwitchState.newBuilder();
      builder.setEnabled(enabled);
      return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
    }

    private SwitchState parseFrom(byte [] bytes)
      throws DeserializationException {
      ProtobufUtil.expectPBMagicPrefix(bytes);
      SwitchState.Builder builder = SwitchState.newBuilder();
      try {
        int magicLen = ProtobufUtil.lengthOfPBMagic();
        ProtobufUtil.mergeFrom(builder, bytes, magicLen, bytes.length - magicLen);
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return builder.build();
    }
  }


}
