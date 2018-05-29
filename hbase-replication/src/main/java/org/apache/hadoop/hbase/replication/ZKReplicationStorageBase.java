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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * This is a base class for maintaining replication related data,for example, peer, queue, etc, in
 * zookeeper.
 */
@InterfaceAudience.Private
public class ZKReplicationStorageBase {

  public static final String REPLICATION_ZNODE = "zookeeper.znode.replication";
  public static final String REPLICATION_ZNODE_DEFAULT = "replication";

  /** The name of the base znode that contains all replication state. */
  protected final String replicationZNode;

  protected final ZKWatcher zookeeper;
  protected final Configuration conf;

  protected ZKReplicationStorageBase(ZKWatcher zookeeper, Configuration conf) {
    this.zookeeper = zookeeper;
    this.conf = conf;

    this.replicationZNode = ZNodePaths.joinZNode(this.zookeeper.getZNodePaths().baseZNode,
      conf.get(REPLICATION_ZNODE, REPLICATION_ZNODE_DEFAULT));
  }

  /**
   * Serialized protobuf of <code>state</code> with pb magic prefix prepended suitable for use as
   * content of a peer-state znode under a peer cluster id as in
   * /hbase/replication/peers/PEER_ID/peer-state.
   */
  protected static byte[] toByteArray(final ReplicationProtos.ReplicationState.State state) {
    ReplicationProtos.ReplicationState msg =
        ReplicationProtos.ReplicationState.newBuilder().setState(state).build();
    // There is no toByteArray on this pb Message?
    // 32 bytes is default which seems fair enough here.
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      CodedOutputStream cos = CodedOutputStream.newInstance(baos, 16);
      msg.writeTo(cos);
      cos.flush();
      baos.flush();
      return ProtobufUtil.prependPBMagic(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
