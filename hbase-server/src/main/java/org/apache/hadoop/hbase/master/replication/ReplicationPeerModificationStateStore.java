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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.BooleanStateStore;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * Store the peer modification state.
 */
@InterfaceAudience.Private
public class ReplicationPeerModificationStateStore extends BooleanStateStore {

  public static final String STATE_NAME = "replication_peer_modification_on";

  public ReplicationPeerModificationStateStore(MasterRegion masterRegion)
    throws DeserializationException, IOException, KeeperException {
    super(masterRegion, STATE_NAME, null, null);
  }

  @Override
  protected byte[] toByteArray(boolean on) {
    ReplicationProtos.ReplicationPeerModificationState.Builder builder =
      ReplicationProtos.ReplicationPeerModificationState.newBuilder();
    builder.setOn(on);
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  @Override
  protected boolean parseFrom(byte[] bytes) throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    ReplicationProtos.ReplicationPeerModificationState.Builder builder =
      ReplicationProtos.ReplicationPeerModificationState.newBuilder();
    try {
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, bytes, magicLen, bytes.length - magicLen);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return builder.build().getOn();
  }
}
