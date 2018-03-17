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

import static org.apache.hadoop.hbase.replication.ReplicationUtils.PEER_STATE_DISABLED_BYTES;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.PEER_STATE_ENABLED_BYTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Table based replication peer storage.
 */
@InterfaceAudience.Private
public class TableReplicationPeerStorage extends TableReplicationStorageBase
    implements ReplicationPeerStorage {

  public TableReplicationPeerStorage(ZKWatcher zookeeper, Configuration conf) throws IOException {
    super(zookeeper, conf);
  }

  @Override
  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Put put = new Put(Bytes.toBytes(peerId));
      put.addColumn(FAMILY_PEER, QUALIFIER_PEER_CONFIG,
        ReplicationPeerConfigUtil.toByteArray(peerConfig));
      put.addColumn(FAMILY_PEER, QUALIFIER_PEER_STATE,
        enabled ? PEER_STATE_ENABLED_BYTES : PEER_STATE_DISABLED_BYTES);
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException("Failed to add peer " + peerId, e);
    }
  }

  @Override
  public void removePeer(String peerId) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Delete delete = new Delete(Bytes.toBytes(peerId));
      table.delete(delete);
    } catch (IOException e) {
      throw new ReplicationException("Failed to remove peer " + peerId, e);
    }
  }

  // TODO make it to be a checkExistAndMutate operation.
  private boolean peerExist(String peerId, Table table) throws IOException {
    Get get = new Get(Bytes.toBytes(peerId));
    get.addColumn(FAMILY_PEER, QUALIFIER_PEER_STATE);
    return table.exists(get);
  }

  @Override
  public void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      if (!peerExist(peerId, table)) {
        throw new ReplicationException("Peer " + peerId + " does not exist.");
      }
      Put put = new Put(Bytes.toBytes(peerId));
      put.addColumn(FAMILY_PEER, QUALIFIER_PEER_STATE,
        enabled ? PEER_STATE_ENABLED_BYTES : PEER_STATE_DISABLED_BYTES);
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to set peer state, peerId=" + peerId + ", state=" + enabled, e);
    }
  }

  @Override
  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      if (!peerExist(peerId, table)) {
        throw new ReplicationException("Peer " + peerId + " does not exist.");
      }
      Put put = new Put(Bytes.toBytes(peerId));
      put.addColumn(FAMILY_PEER, QUALIFIER_PEER_CONFIG,
        ReplicationPeerConfigUtil.toByteArray(peerConfig));
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException("Failed to update peer configuration, peerId=" + peerId, e);
    }
  }

  @Override
  public List<String> listPeerIds() throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Scan scan = new Scan().addColumn(FAMILY_PEER, QUALIFIER_PEER_STATE);
      try (ResultScanner scanner = table.getScanner(scan)) {
        List<String> peerIds = new ArrayList<>();
        for (Result r : scanner) {
          peerIds.add(Bytes.toString(r.getRow()));
        }
        return peerIds;
      }
    } catch (IOException e) {
      throw new ReplicationException("Failed to list peers", e);
    }
  }

  @Override
  public boolean isPeerEnabled(String peerId) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Get get = new Get(Bytes.toBytes(peerId)).addColumn(FAMILY_PEER, QUALIFIER_PEER_STATE);
      Result r = table.get(get);
      if (r == null) {
        throw new ReplicationException("Peer " + peerId + " does not found");
      }
      return Arrays.equals(PEER_STATE_ENABLED_BYTES, r.getValue(FAMILY_PEER, QUALIFIER_PEER_STATE));
    } catch (IOException e) {
      throw new ReplicationException("Failed to read the peer state, peerId=" + peerId, e);
    }
  }

  @Override
  public ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException {
    try (Table table = getReplicationMetaTable()) {
      Get get = new Get(Bytes.toBytes(peerId)).addColumn(FAMILY_PEER, QUALIFIER_PEER_CONFIG);
      Result r = table.get(get);
      if (r == null) {
        throw new ReplicationException("Peer " + peerId + " does not found");
      }
      byte[] data = r.getValue(FAMILY_PEER, QUALIFIER_PEER_CONFIG);
      if (data == null || data.length == 0) {
        throw new ReplicationException(
            "Replication peer config data shouldn't be empty, peerId=" + peerId);
      }
      try {
        return ReplicationPeerConfigUtil.parsePeerFrom(data);
      } catch (DeserializationException e) {
        throw new ReplicationException(
            "Failed to parse replication peer config for peer with id=" + peerId, e);
      }
    } catch (IOException e) {
      throw new ReplicationException(
          "Failed to read the peer configuration in hbase:replication, peerId=" + peerId, e);
    }
  }
}
