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

import java.util.Arrays;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * Used by synchronous replication. Indicate the state of the current cluster in a synchronous
 * replication peer. The state may be one of {@link SyncReplicationState#ACTIVE},
 * {@link SyncReplicationState#DOWNGRADE_ACTIVE} or {@link SyncReplicationState#STANDBY}.
 * <p>
 * For asynchronous replication, the state is {@link SyncReplicationState#NONE}.
 */
@InterfaceAudience.Public
public enum SyncReplicationState {
  NONE(0), ACTIVE(1), DOWNGRADE_ACTIVE(2), STANDBY(3);

  private final byte value;

  private SyncReplicationState(int value) {
    this.value = (byte) value;
  }

  public static SyncReplicationState valueOf(int value) {
    switch (value) {
      case 0:
        return NONE;
      case 1:
        return ACTIVE;
      case 2:
        return DOWNGRADE_ACTIVE;
      case 3:
        return STANDBY;
      default:
        throw new IllegalArgumentException("Unknown synchronous replication state " + value);
    }
  }

  public int value() {
    return value & 0xFF;
  }

  public static byte[] toByteArray(SyncReplicationState state) {
    return ProtobufUtil
      .prependPBMagic(ReplicationPeerConfigUtil.toSyncReplicationState(state).toByteArray());
  }

  public static SyncReplicationState parseFrom(byte[] bytes) throws InvalidProtocolBufferException {
    if (bytes == null) {
      return SyncReplicationState.NONE;
    }
    return ReplicationPeerConfigUtil.toSyncReplicationState(ReplicationProtos.SyncReplicationState
        .parseFrom(Arrays.copyOfRange(bytes, ProtobufUtil.lengthOfPBMagic(), bytes.length)));
  }
}
