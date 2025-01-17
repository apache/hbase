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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.HConstants.CLUSTER_KEY_FILE_PREFIX;

@InterfaceAudience.Private
public class ClusterKeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterKeyManager.class);

  private final MasterServices master;
  private Boolean pbeEnabled;

  public ClusterKeyManager(MasterServices master) throws IOException {
    this.master = master;
  }

  public void ensureClusterKeyInitialized() throws IOException {
    if (! isPBEEnabled()) {
      return;
    }
    List<Path> clusterKeys = getAllClusterKeys();
    if (clusterKeys.size() == 0) {
      LOG.info("Initializing Cluster Key for the first time");
      // Double check for cluster key as another HMaster might have succeeded.
      if (rotateClusterKey(null) == null && getAllClusterKeys().size() == 0) {
        throw new RuntimeException("Failed to generate or save Cluster Key");
      }
    }
    else if (rotateClusterKeyIfChanged() != null) {
      LOG.info("Cluster key has been rotated");
    }
    else {
      LOG.info("Cluster key is already initialized and unchanged");
    }
  }

  private boolean isPBEEnabled() {
    if (pbeEnabled == null) {
      pbeEnabled = master.getConfiguration().getBoolean(HConstants.CRYPTO_PBE_ENABLED_CONF_KEY,
        false);
    }
    return pbeEnabled;
  }

  public PBEKeyData rotateClusterKeyIfChanged() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    Path latestFile = getLatestClusterKeyFile();
    FSDataInputStream fin = master.getMasterFileSystem().getFileSystem().open(latestFile);
    try {
      String latestKeyMeata = fin.readUTF();
      PBEKeyData rotatedKey = rotateClusterKey(latestKeyMeata);
      if (rotatedKey != null && ! latestKeyMeata.equals(rotatedKey.getKeyMetadata())) {
        return rotatedKey;
      }
    }
    finally {
      fin.close();;
    }
    return null;
  }

  public PBEKeyData rotateClusterKey(String currentKeyMetadata) throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    PBEKeyProvider provider = getKeyProvider();
    PBEKeyData clusterKey = provider.getClusterKey(
      master.getMasterFileSystem().getClusterId().toString().getBytes());
    if (clusterKey != null && clusterKey.getKeyMetadata() != null &&
        ! clusterKey.getKeyMetadata().equals(currentKeyMetadata) &&
        saveLatestClusterKey(clusterKey.getKeyMetadata())) {
      return clusterKey;
    }
    return null;
  }

  public Path getLatestClusterKeyFile() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    int currentMaxSeqNum = findLatestKeySequence(getAllClusterKeys());
    return new Path(master.getMasterFileSystem().getClusterKeyDir(),
    CLUSTER_KEY_FILE_PREFIX + currentMaxSeqNum);
  }

  public List<Path> getAllClusterKeys() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    MasterFileSystem masterFS = master.getMasterFileSystem();
    Path clusterKeyDir = masterFS.getClusterKeyDir();
    FileSystem fs = masterFS.getFileSystem();
    List<Path> clusterKeys = new ArrayList<>();
    for (FileStatus st: fs.globStatus(new Path(clusterKeyDir, CLUSTER_KEY_FILE_PREFIX + "*"))) {
      Path keyPath = st.getPath();
      extractClusterKeySeqNum(keyPath); // Just check for validity.
      clusterKeys.add(keyPath);
    }
    return clusterKeys;
  }

  private int findLatestKeySequence(List<Path> clusterKeys) throws IOException {
    int maxKeySeq = -1;
    for (Path keyPath: clusterKeys) {
      if (keyPath.getName().startsWith(CLUSTER_KEY_FILE_PREFIX)) {
        int keySeq = Integer.valueOf(keyPath.getName().substring(CLUSTER_KEY_FILE_PREFIX.length()));
        if (keySeq > maxKeySeq) {
          maxKeySeq = keySeq;
        }
      }
    }
    return maxKeySeq;
  }

  private boolean saveLatestClusterKey(String keyMetadata) throws IOException {
    int nextClusterKeySeq = findLatestKeySequence(getAllClusterKeys()) + 1;
    LOG.info("Trying to save a new cluster key at seq: ", nextClusterKeySeq);
    MasterFileSystem masterFS = master.getMasterFileSystem();
    Path nextClusterKeyPath = new Path(masterFS.getClusterKeyDir(),
      CLUSTER_KEY_FILE_PREFIX + nextClusterKeySeq);
    Path tempClusterKeyFile  = new Path(masterFS.getTempDir(),
      nextClusterKeyPath.getName() + UUID.randomUUID().toString());
    FSDataOutputStream fsDataOutputStream = masterFS.getFileSystem().create(tempClusterKeyFile);
    try {
      fsDataOutputStream.writeUTF(keyMetadata);
      boolean succeeded = masterFS.getFileSystem().rename(tempClusterKeyFile, nextClusterKeyPath);
      if (succeeded) {
        LOG.info("Cluster key save succeeded for seq: {}", nextClusterKeySeq);
      }
      else {
        LOG.error("Cluster key save failed for seq: {}", nextClusterKeySeq);
      }
      return succeeded;
    }
    finally {
      fsDataOutputStream.close();
      masterFS.getFileSystem().delete(tempClusterKeyFile, false);
    }
  }

  private int extractClusterKeySeqNum(Path keyPath) throws IOException {
    if (keyPath.getName().startsWith(CLUSTER_KEY_FILE_PREFIX)) {
      try {
        return Integer.valueOf(keyPath.getName().substring(CLUSTER_KEY_FILE_PREFIX.length()));
      }
      catch (NumberFormatException e) {
        LOG.error("Invalid file name for a cluster key: {}", keyPath, e);
      }
    }
    throw new IOException("Couldn't parse key file name: " + keyPath.getName());
  }

  private PBEKeyProvider getKeyProvider() {
    KeyProvider provider = Encryption.getKeyProvider(master.getConfiguration());
    if (!(provider instanceof PBEKeyProvider)) {
      throw new RuntimeException("KeyProvider: " + provider.getClass().getName()
        + " expected to be of type PBEKeyProvider");
    }
    return (PBEKeyProvider) provider;
  }
}
