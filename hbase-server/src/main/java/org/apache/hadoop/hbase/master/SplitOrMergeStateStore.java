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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Tracks the switch of split and merge states.
 */
@InterfaceAudience.Private
public class SplitOrMergeStateStore {

  private static final String SPLIT_STATE_NAME = "split_enabled";

  private static final String MERGE_STATE_NAME = "merge_enabled";

  private SwitchStateStore splitStateStore;
  private SwitchStateStore mergeStateStore;

  public SplitOrMergeStateStore(MasterRegion masterRegion, ZKWatcher watcher, Configuration conf)
    throws IOException, KeeperException, DeserializationException {
    @SuppressWarnings("deprecation")
    String splitZnode = ZNodePaths.joinZNode(watcher.getZNodePaths().switchZNode,
      conf.get("zookeeper.znode.switch.split", "split"));
    @SuppressWarnings("deprecation")
    String mergeZnode = ZNodePaths.joinZNode(watcher.getZNodePaths().switchZNode,
      conf.get("zookeeper.znode.switch.merge", "merge"));
    splitStateStore = new SwitchStateStore(masterRegion, SPLIT_STATE_NAME, watcher, splitZnode);
    mergeStateStore = new SwitchStateStore(masterRegion, MERGE_STATE_NAME, watcher, mergeZnode);
  }

  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return splitStateStore.get();
      case MERGE:
        return mergeStateStore.get();
      default:
        break;
    }
    return false;
  }

  public void setSplitOrMergeEnabled(boolean enabled, MasterSwitchType switchType)
    throws IOException {
    switch (switchType) {
      case SPLIT:
        splitStateStore.set(enabled);
        break;
      case MERGE:
        mergeStateStore.set(enabled);
        break;
      default:
        break;
    }
  }

  private static final class SwitchStateStore extends BooleanStateStore {

    public SwitchStateStore(MasterRegion masterRegion, String stateName, ZKWatcher watcher,
      String zkPath) throws IOException, KeeperException, DeserializationException {
      super(masterRegion, stateName, watcher, zkPath);
    }

    @Override
    protected byte[] toByteArray(boolean enabled) {
      ZooKeeperProtos.SwitchState.Builder builder = ZooKeeperProtos.SwitchState.newBuilder();
      builder.setEnabled(enabled);
      return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
    }

    @Override
    protected boolean parseFrom(byte[] bytes) throws DeserializationException {
      ProtobufUtil.expectPBMagicPrefix(bytes);
      ZooKeeperProtos.SwitchState.Builder builder = ZooKeeperProtos.SwitchState.newBuilder();
      try {
        int magicLen = ProtobufUtil.lengthOfPBMagic();
        ProtobufUtil.mergeFrom(builder, bytes, magicLen, bytes.length - magicLen);
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return builder.build().getEnabled();
    }
  }
}
