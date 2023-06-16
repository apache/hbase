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
package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.BooleanStateStore;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LoadBalancerProtos;

/**
 * Store the balancer state.
 */
@InterfaceAudience.Private
public class LoadBalancerStateStore extends BooleanStateStore {

  public static final String STATE_NAME = "load_balancer_on";

  @SuppressWarnings("deprecation")
  public LoadBalancerStateStore(MasterRegion masterRegion, ZKWatcher watcher)
    throws IOException, KeeperException, DeserializationException {
    super(masterRegion, STATE_NAME, watcher, watcher.getZNodePaths().balancerZNode);
  }

  @Override
  protected byte[] toByteArray(boolean isBalancerOn) {
    LoadBalancerProtos.LoadBalancerState.Builder builder =
      LoadBalancerProtos.LoadBalancerState.newBuilder();
    builder.setBalancerOn(isBalancerOn);
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  @Override
  protected boolean parseFrom(byte[] pbBytes) throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(pbBytes);
    LoadBalancerProtos.LoadBalancerState.Builder builder =
      LoadBalancerProtos.LoadBalancerState.newBuilder();
    try {
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, pbBytes, magicLen, pbBytes.length - magicLen);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return builder.build().getBalancerOn();
  }
}
