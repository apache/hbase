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
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.keymeta.PBEClusterKeyAccessor;
import org.apache.yetus.audience.InterfaceAudience;
import static org.apache.hadoop.hbase.HConstants.CLUSTER_KEY_FILE_PREFIX;

@InterfaceAudience.Private
public class PBEClusterKeyManager extends PBEClusterKeyAccessor {
  private final MasterServices master;

  public PBEClusterKeyManager(MasterServices master) throws IOException {
    super(master);
    this.master = master;
  }

  public void ensureClusterKeyInitialized() throws IOException {
    if (! isPBEEnabled()) {
      return;
    }
    List<Path> clusterKeys = getAllClusterKeyFiles();
    if (clusterKeys.isEmpty()) {
      LOG.info("Initializing Cluster Key for the first time");
      // Double check for cluster key as another HMaster might have succeeded.
      if (rotateClusterKey(null) == null && getAllClusterKeyFiles().isEmpty()) {
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

  public PBEKeyData rotateClusterKeyIfChanged() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    Path latestFile = getLatestClusterKeyFile();
    String latestKeyMetadata = loadKeyMetadata(latestFile);
    return rotateClusterKey(latestKeyMetadata);
  }

  private PBEKeyData rotateClusterKey(String currentKeyMetadata) throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    PBEKeyProvider provider = getKeyProvider();
    PBEKeyData clusterKey = provider.getClusterKey(
      master.getMasterFileSystem().getClusterId().toString().getBytes());
    if (clusterKey.getKeyStatus() != PBEKeyStatus.ACTIVE) {
      throw new IOException("Cluster key is expected to be ACTIVE but it is: " +
        clusterKey.getKeyStatus() + " for metadata: " + clusterKey.getKeyMetadata());
    }
    if (clusterKey != null && clusterKey.getKeyMetadata() != null &&
        ! clusterKey.getKeyMetadata().equals(currentKeyMetadata) &&
        saveLatestClusterKey(clusterKey.getKeyMetadata())) {
      return clusterKey;
    }
    return null;
  }

  private boolean saveLatestClusterKey(String keyMetadata) throws IOException {
    List<Path> allClusterKeyFiles = getAllClusterKeyFiles();
    int nextClusterKeySeq = (allClusterKeyFiles.isEmpty() ? -1
      : extractKeySequence(allClusterKeyFiles.get(0))) + 1;
    LOG.info("Trying to save a new cluster key at seq: {}", nextClusterKeySeq);
    MasterFileSystem masterFS = master.getMasterFileSystem();
    Path nextClusterKeyPath = new Path(clusterKeyDir,
      CLUSTER_KEY_FILE_PREFIX + nextClusterKeySeq);
    Path tempClusterKeyFile  = new Path(masterFS.getTempDir(),
      nextClusterKeyPath.getName() + UUID.randomUUID());
    try (FSDataOutputStream fsDataOutputStream = masterFS.getFileSystem()
      .create(tempClusterKeyFile)) {
      fsDataOutputStream.writeUTF(keyMetadata);
      boolean succeeded = masterFS.getFileSystem().rename(tempClusterKeyFile, nextClusterKeyPath);
      if (succeeded) {
        LOG.info("Cluster key save succeeded for seq: {}", nextClusterKeySeq);
      } else {
        LOG.error("Cluster key save failed for seq: {}", nextClusterKeySeq);
      }
      return succeeded;
    }
    finally {
      masterFS.getFileSystem().delete(tempClusterKeyFile, false);
    }
  }
}
