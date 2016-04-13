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

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Stable
public final class ReplicationSerDeHelper {
  private static final Log LOG = LogFactory.getLog(ReplicationSerDeHelper.class);

  private ReplicationSerDeHelper() {}

  /**
   * @param bytes Content of a peer znode.
   * @return ClusterKey parsed from the passed bytes.
   * @throws DeserializationException
   */
  public static ReplicationPeerConfig parsePeerFrom(final byte[] bytes)
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationPeer.Builder builder =
          ZooKeeperProtos.ReplicationPeer.newBuilder();
      ZooKeeperProtos.ReplicationPeer peer;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        peer = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return convert(peer);
    } else {
      if (bytes.length > 0) {
        return new ReplicationPeerConfig().setClusterKey(Bytes.toString(bytes));
      }
      return new ReplicationPeerConfig().setClusterKey("");
    }
  }

  private static ReplicationPeerConfig convert(ZooKeeperProtos.ReplicationPeer peer) {
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    if (peer.hasClusterkey()) {
      peerConfig.setClusterKey(peer.getClusterkey());
    }
    if (peer.hasReplicationEndpointImpl()) {
      peerConfig.setReplicationEndpointImpl(peer.getReplicationEndpointImpl());
    }

    for (HBaseProtos.BytesBytesPair pair : peer.getDataList()) {
      peerConfig.getPeerData().put(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
    }

    for (HBaseProtos.NameStringPair pair : peer.getConfigurationList()) {
      peerConfig.getConfiguration().put(pair.getName(), pair.getValue());
    }
    return peerConfig;
  }

  /**
   * @param peerConfig
   * @return Serialized protobuf of <code>peerConfig</code> with pb magic prefix prepended suitable
   *         for use as content of a this.peersZNode; i.e. the content of PEER_ID znode under
   *         /hbase/replication/peers/PEER_ID
   */
  public static byte[] toByteArray(final ReplicationPeerConfig peerConfig) {
    byte[] bytes = convert(peerConfig).toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  private static ZooKeeperProtos.ReplicationPeer convert(ReplicationPeerConfig  peerConfig) {
    ZooKeeperProtos.ReplicationPeer.Builder builder = ZooKeeperProtos.ReplicationPeer.newBuilder();
    if (peerConfig.getClusterKey() != null) {
      builder.setClusterkey(peerConfig.getClusterKey());
    }
    if (peerConfig.getReplicationEndpointImpl() != null) {
      builder.setReplicationEndpointImpl(peerConfig.getReplicationEndpointImpl());
    }

    for (Map.Entry<byte[], byte[]> entry : peerConfig.getPeerData().entrySet()) {
      builder.addData(HBaseProtos.BytesBytesPair.newBuilder()
          .setFirst(ByteString.copyFrom(entry.getKey()))
          .setSecond(ByteString.copyFrom(entry.getValue()))
          .build());
    }

    for (Map.Entry<String, String> entry : peerConfig.getConfiguration().entrySet()) {
      builder.addConfiguration(HBaseProtos.NameStringPair.newBuilder()
          .setName(entry.getKey())
          .setValue(entry.getValue())
          .build());
    }

    return builder.build();
  }
}
