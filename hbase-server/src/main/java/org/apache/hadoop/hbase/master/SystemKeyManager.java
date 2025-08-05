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

import static org.apache.hadoop.hbase.HConstants.SYSTEM_KEY_FILE_PREFIX;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.keymeta.SystemKeyAccessor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SystemKeyManager extends SystemKeyAccessor {
  private final MasterServices master;

  public SystemKeyManager(MasterServices master) throws IOException {
    super(master);
    this.master = master;
  }

  public void ensureSystemKeyInitialized() throws IOException {
    if (! isKeyManagementEnabled()) {
      return;
    }
    List<Path> clusterKeys = getAllSystemKeyFiles();
    if (clusterKeys.isEmpty()) {
      LOG.info("Initializing System Key for the first time");
      // Double check for cluster key as another HMaster might have succeeded.
      if (rotateSystemKey(null, clusterKeys) == null &&
          getAllSystemKeyFiles().isEmpty()) {
        throw new RuntimeException("Failed to generate or save System Key");
      }
    }
    else if (rotateSystemKeyIfChanged() != null) {
      LOG.info("System key has been rotated");
    }
    else {
      LOG.info("System key is already initialized and unchanged");
    }
  }

  public ManagedKeyData rotateSystemKeyIfChanged() throws IOException {
    if (! isKeyManagementEnabled()) {
      return null;
    }
    Pair<Path, List<Path>> latestFileResult = getLatestSystemKeyFile();
    Path latestFile = getLatestSystemKeyFile().getFirst();
    String latestKeyMetadata = loadKeyMetadata(latestFile);
    return rotateSystemKey(latestKeyMetadata, latestFileResult.getSecond());
  }

  private ManagedKeyData rotateSystemKey(String currentKeyMetadata, List<Path> allSystemKeyFiles)
      throws IOException {
    ManagedKeyProvider provider = getKeyProvider();
    ManagedKeyData clusterKey = provider.getSystemKey(
      master.getMasterFileSystem().getClusterId().toString().getBytes());
    if (clusterKey == null) {
      throw new IOException("Failed to get system key for cluster id: " +
        master.getMasterFileSystem().getClusterId().toString());
    }
    if (clusterKey.getKeyState() != ManagedKeyState.ACTIVE) {
      throw new IOException("System key is expected to be ACTIVE but it is: " +
        clusterKey.getKeyState() + " for metadata: " + clusterKey.getKeyMetadata());
    }
    if (clusterKey.getKeyMetadata() == null) {
      throw new IOException("System key is expected to have metadata but it is null");
    }
    if (! clusterKey.getKeyMetadata().equals(currentKeyMetadata) &&
        saveLatestSystemKey(clusterKey.getKeyMetadata(), allSystemKeyFiles)) {
      return clusterKey;
    }
    return null;
  }

  private boolean saveLatestSystemKey(String keyMetadata, List<Path> allSystemKeyFiles)
      throws IOException {
    int nextSystemKeySeq = (allSystemKeyFiles.isEmpty() ? -1
      : SystemKeyAccessor.extractKeySequence(allSystemKeyFiles.get(0))) + 1;
    LOG.info("Trying to save a new cluster key at seq: {}", nextSystemKeySeq);
    MasterFileSystem masterFS = master.getMasterFileSystem();
    Path nextSystemKeyPath = new Path(systemKeyDir,
      SYSTEM_KEY_FILE_PREFIX + nextSystemKeySeq);
    Path tempSystemKeyFile  = new Path(masterFS.getTempDir(),
      nextSystemKeyPath.getName() + UUID.randomUUID());
    try (FSDataOutputStream fsDataOutputStream = masterFS.getFileSystem()
      .create(tempSystemKeyFile)) {
      fsDataOutputStream.writeUTF(keyMetadata);
      boolean succeeded = masterFS.getFileSystem().rename(tempSystemKeyFile, nextSystemKeyPath);
      if (succeeded) {
        LOG.info("System key save succeeded for seq: {}", nextSystemKeySeq);
      } else {
        LOG.error("System key save failed for seq: {}", nextSystemKeySeq);
      }
      return succeeded;
    }
    finally {
      masterFS.getFileSystem().delete(tempSystemKeyFile, false);
    }
  }
}
