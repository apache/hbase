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
package org.apache.hadoop.hbase.replication;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RotateFile;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filesystem based replication peer storage. The implementation does not require atomic rename so
 * you can use it on cloud OSS.
 * <p/>
 * FileSystem layout:
 *
 * <pre>
 * hbase
 *   |
 *   --peers
 *       |
 *       --&lt;peer_id&gt;
 *           |
 *           --peer_config
 *           |
 *           --disabled
 *           |
 *           --sync-rep-state
 * </pre>
 *
 * Notice that, if the peer is enabled, we will not have a disabled file.
 * <p/>
 * And for other files, to avoid depending on atomic rename, we will use two files for storing the
 * content. When loading, we will try to read both the files and load the newer one. And when
 * writing, we will write to the older file.
 */
@InterfaceAudience.Private
public class FSReplicationPeerStorage implements ReplicationPeerStorage {

  private static final Logger LOG = LoggerFactory.getLogger(FSReplicationPeerStorage.class);

  public static final String PEERS_DIR = "hbase.replication.peers.directory";

  public static final String PEERS_DIR_DEFAULT = "peers";

  static final String PEER_CONFIG_FILE = "peer_config";

  static final String DISABLED_FILE = "disabled";

  static final String SYNC_REPLICATION_STATE_FILE = "sync-rep-state";

  static final String CREATE_TIME_FILE = "create_time";

  static final byte[] NONE_STATE_BYTES =
    SyncReplicationState.toByteArray(SyncReplicationState.NONE);

  private final FileSystem fs;

  private final Path dir;

  public FSReplicationPeerStorage(FileSystem fs, Configuration conf) throws IOException {
    this.fs = fs;
    this.dir = new Path(CommonFSUtils.getRootDir(conf), conf.get(PEERS_DIR, PEERS_DIR_DEFAULT));
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/FSReplicationPeerStorage.java|.*/src/test/.*")
  Path getPeerDir(String peerId) {
    return new Path(dir, peerId);
  }

  @Override
  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled,
    SyncReplicationState syncReplicationState) throws ReplicationException {
    Path peerDir = getPeerDir(peerId);
    try {
      if (fs.exists(peerDir)) {
        // check whether this is a valid peer, if so we should fail the add peer operation
        if (read(fs, peerDir, PEER_CONFIG_FILE) != null) {
          throw new ReplicationException("Could not add peer with id=" + peerId + ", peerConfig=>"
            + peerConfig + ", state=" + (enabled ? "ENABLED" : "DISABLED")
            + ", syncReplicationState=" + syncReplicationState + ", peer already exists");
        }
      }
      if (!enabled) {
        fs.createNewFile(new Path(peerDir, DISABLED_FILE));
      }
      fs.createNewFile(new Path(peerDir, CREATE_TIME_FILE));
      write(fs, peerDir, SYNC_REPLICATION_STATE_FILE,
        SyncReplicationState.toByteArray(syncReplicationState, SyncReplicationState.NONE));
      // write the peer config data at last, so when loading, if we can not load the peer_config, we
      // know that this is not a valid peer
      write(fs, peerDir, PEER_CONFIG_FILE, ReplicationPeerConfigUtil.toByteArray(peerConfig));
    } catch (IOException e) {
      throw new ReplicationException(
        "Could not add peer with id=" + peerId + ", peerConfig=>" + peerConfig + ", state="
          + (enabled ? "ENABLED" : "DISABLED") + ", syncReplicationState=" + syncReplicationState,
        e);
    }
  }

  @Override
  public void removePeer(String peerId) throws ReplicationException {
    // delete the peer config first, and then delete the directory
    // we will consider this is not a valid peer by reading the peer config file
    Path peerDir = getPeerDir(peerId);
    try {
      delete(fs, peerDir, PEER_CONFIG_FILE);
      if (!fs.delete(peerDir, true)) {
        throw new IOException("Can not delete " + peerDir);
      }
    } catch (IOException e) {
      throw new ReplicationException("Could not remove peer with id=" + peerId, e);
    }
  }

  @Override
  public void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    Path disabledFile = new Path(getPeerDir(peerId), DISABLED_FILE);
    try {
      if (enabled) {
        if (fs.exists(disabledFile) && !fs.delete(disabledFile, false)) {
          throw new IOException("Can not delete " + disabledFile);
        }
      } else {
        if (!fs.exists(disabledFile) && !fs.createNewFile(disabledFile)) {
          throw new IOException("Can not touch " + disabledFile);
        }
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "Unable to change state of the peer with id=" + peerId + " to " + enabled, e);
    }
  }

  @Override
  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
    throws ReplicationException {
    Path peerDir = getPeerDir(peerId);
    try {
      write(fs, peerDir, PEER_CONFIG_FILE, ReplicationPeerConfigUtil.toByteArray(peerConfig));
    } catch (IOException e) {
      throw new ReplicationException(
        "There was a problem trying to save changes to the " + "replication peer " + peerId, e);
    }
  }

  @Override
  public List<String> listPeerIds() throws ReplicationException {
    try {
      FileStatus[] statuses = fs.listStatus(dir);
      if (statuses == null || statuses.length == 0) {
        return Collections.emptyList();
      }
      List<String> peerIds = new ArrayList<>();
      for (FileStatus status : statuses) {
        String peerId = status.getPath().getName();
        Path peerDir = getPeerDir(peerId);
        // confirm that this is a valid peer
        byte[] peerConfigData = read(fs, peerDir, PEER_CONFIG_FILE);
        if (peerConfigData != null) {
          peerIds.add(peerId);
        }
      }
      return Collections.unmodifiableList(peerIds);
    } catch (FileNotFoundException e) {
      LOG.debug("Peer directory does not exist yet", e);
      return Collections.emptyList();
    } catch (IOException e) {
      throw new ReplicationException("Cannot get the list of peers", e);
    }
  }

  @Override
  public boolean isPeerEnabled(String peerId) throws ReplicationException {
    Path disabledFile = new Path(getPeerDir(peerId), DISABLED_FILE);
    try {
      return !fs.exists(disabledFile);
    } catch (IOException e) {
      throw new ReplicationException("Unable to get status of the peer with id=" + peerId, e);
    }
  }

  @Override
  public ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException {
    Path peerDir = getPeerDir(peerId);
    byte[] data;
    try {
      data = read(fs, peerDir, PEER_CONFIG_FILE);
    } catch (IOException e) {
      throw new ReplicationException("Error getting configuration for peer with id=" + peerId, e);
    }
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
  }

  @Override
  public long getPeerCreateTime(String peerId) {
    Path createTimeFile = new Path(getPeerDir(peerId), CREATE_TIME_FILE);
    try {
      if (fs.exists(createTimeFile)) {
        return fs.getFileStatus(createTimeFile).getModificationTime();
      }
    } catch (IOException e) {
      LOG.warn("Unable to get create time of the peer: " + peerId, e);
    }
    return NO_CREATE_TIME;
  }

  private Pair<SyncReplicationState, SyncReplicationState> getStateAndNewState(String peerId)
    throws IOException {
    Path peerDir = getPeerDir(peerId);
    if (!fs.exists(peerDir)) {
      throw new IOException("peer does not exists");
    }
    byte[] data = read(fs, peerDir, SYNC_REPLICATION_STATE_FILE);
    if (data == null) {
      // should be a peer from previous version, set the sync replication state for it.
      write(fs, peerDir, SYNC_REPLICATION_STATE_FILE,
        SyncReplicationState.toByteArray(SyncReplicationState.NONE, SyncReplicationState.NONE));
      return Pair.newPair(SyncReplicationState.NONE, SyncReplicationState.NONE);
    } else {
      return SyncReplicationState.parseStateAndNewStateFrom(data);
    }
  }

  @Override
  public void setPeerNewSyncReplicationState(String peerId, SyncReplicationState newState)
    throws ReplicationException {
    Path peerDir = getPeerDir(peerId);
    try {
      Pair<SyncReplicationState, SyncReplicationState> stateAndNewState =
        getStateAndNewState(peerId);
      write(fs, peerDir, SYNC_REPLICATION_STATE_FILE,
        SyncReplicationState.toByteArray(stateAndNewState.getFirst(), newState));
    } catch (IOException e) {
      throw new ReplicationException(
        "Unable to set the new sync replication state for peer with id=" + peerId + ", newState="
          + newState,
        e);
    }
  }

  @Override
  public void transitPeerSyncReplicationState(String peerId) throws ReplicationException {
    Path peerDir = getPeerDir(peerId);
    try {
      Pair<SyncReplicationState, SyncReplicationState> stateAndNewState =
        getStateAndNewState(peerId);
      write(fs, peerDir, SYNC_REPLICATION_STATE_FILE,
        SyncReplicationState.toByteArray(stateAndNewState.getSecond(), SyncReplicationState.NONE));
    } catch (IOException e) {
      throw new ReplicationException(
        "Error transiting sync replication state for peer with id=" + peerId, e);
    }
  }

  @Override
  public SyncReplicationState getPeerSyncReplicationState(String peerId)
    throws ReplicationException {
    try {
      return getStateAndNewState(peerId).getFirst();
    } catch (IOException e) {
      throw new ReplicationException(
        "Error getting sync replication state for peer with id=" + peerId, e);
    }
  }

  @Override
  public SyncReplicationState getPeerNewSyncReplicationState(String peerId)
    throws ReplicationException {
    try {
      return getStateAndNewState(peerId).getSecond();
    } catch (IOException e) {
      throw new ReplicationException(
        "Error getting new sync replication state for peer with id=" + peerId, e);
    }
  }

  // 16 MB is big enough for our usage here
  private static final long MAX_FILE_SIZE = 16 * 1024 * 1024;

  private static byte[] read(FileSystem fs, Path dir, String name) throws IOException {
    RotateFile file = new RotateFile(fs, dir, name, MAX_FILE_SIZE);
    return file.read();
  }

  private static void write(FileSystem fs, Path dir, String name, byte[] data) throws IOException {
    RotateFile file = new RotateFile(fs, dir, name, MAX_FILE_SIZE);
    // to initialize the nextFile index
    file.read();
    file.write(data);
  }

  private static void delete(FileSystem fs, Path dir, String name) throws IOException {
    RotateFile file = new RotateFile(fs, dir, name, MAX_FILE_SIZE);
    // to initialize the nextFile index
    file.read();
    file.delete();
  }
}
