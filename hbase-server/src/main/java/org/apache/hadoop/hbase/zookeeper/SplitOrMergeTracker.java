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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SwitchState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;


/**
 * Tracks the switch of split and merge states in ZK
 *
 */
@InterfaceAudience.Private
public class SplitOrMergeTracker {

  private String splitZnode;
  private String mergeZnode;

  private SwitchStateTracker splitStateTracker;
  private SwitchStateTracker mergeStateTracker;

  public SplitOrMergeTracker(ZooKeeperWatcher watcher, Configuration conf,
                             Abortable abortable) {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getSwitchZNode()) < 0) {
        ZKUtil.createAndFailSilent(watcher, watcher.getSwitchZNode());
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
    splitZnode = ZKUtil.joinZNode(watcher.getSwitchZNode(),
      conf.get("zookeeper.znode.switch.split", "split"));
    mergeZnode = ZKUtil.joinZNode(watcher.getSwitchZNode(),
      conf.get("zookeeper.znode.switch.merge", "merge"));
    splitStateTracker = new SwitchStateTracker(watcher, splitZnode, abortable);
    mergeStateTracker = new SwitchStateTracker(watcher, mergeZnode, abortable);
  }

  public void start() {
    splitStateTracker.start();
    mergeStateTracker.start();
  }

  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
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

  public void setSplitOrMergeEnabled(boolean enabled, MasterSwitchType switchType)
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
