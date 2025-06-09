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
package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static org.apache.hadoop.hbase.HConstants.SYSTEM_KEY_FILE_PREFIX;

@InterfaceAudience.Private
public class SystemKeyAccessor extends KeyManagementBase {
  protected final Path systemKeyDir;

  public SystemKeyAccessor(Server server) throws IOException {
    super(server);
    this.systemKeyDir = CommonFSUtils.getSystemKeyDir(server.getConfiguration());
  }

  public Pair<Path,List<Path>> getLatestSystemKeyFile() throws IOException {
    if (! isKeyManagementEnabled()) {
      return new Pair<>(null, null);
    }
    List<Path> allClusterKeyFiles = getAllSystemKeyFiles();
    if (allClusterKeyFiles.isEmpty()) {
      throw new RuntimeException("No cluster key initialized yet");
    }
    int currentMaxSeqNum = SystemKeyAccessor.extractKeySequence(allClusterKeyFiles.get(0));
    return new Pair<>(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX + currentMaxSeqNum),
      allClusterKeyFiles);
  }

  /**
   * Return all available cluster key files and return them in the order of latest to oldest.
   * If no cluster key files are available, then return an empty list. If key management is not
   * enabled, then return null.
   *
   * @return  a list of all available cluster key files
   * @throws IOException
   */
  public List<Path> getAllSystemKeyFiles() throws IOException {
    if (!isKeyManagementEnabled()) {
      return null;
    }
    FileSystem fs = getServer().getFileSystem();
    Map<Integer, Path> clusterKeys = new TreeMap<>(Comparator.reverseOrder());
    for (FileStatus st : fs.globStatus(new Path(systemKeyDir,
      SYSTEM_KEY_FILE_PREFIX + "*"))) {
      Path keyPath = st.getPath();
      int seqNum = extractSystemKeySeqNum(keyPath);
      clusterKeys.put(seqNum, keyPath);
    }

    return new ArrayList<>(clusterKeys.values());
  }

  public ManagedKeyData loadSystemKey(Path keyPath) throws IOException {
    ManagedKeyProvider provider = getKeyProvider();
    ManagedKeyData keyData = provider.unwrapKey(loadKeyMetadata(keyPath), null);
    if (keyData == null) {
      throw new RuntimeException("Failed to load system key from: " + keyPath);
    }
    return keyData;
  }

  @VisibleForTesting
  public static int extractSystemKeySeqNum(Path keyPath) throws IOException {
    if (keyPath.getName().startsWith(SYSTEM_KEY_FILE_PREFIX)) {
      try {
        return Integer.valueOf(keyPath.getName().substring(SYSTEM_KEY_FILE_PREFIX.length()));
      }
      catch (NumberFormatException e) {
        LOG.error("Invalid file name for a cluster key: {}", keyPath, e);
      }
    }
    throw new IOException("Couldn't parse key file name: " + keyPath.getName());
  }

  /**
   * Extract the key sequence number from the cluster key file name.
   * @param clusterKeyFile
   * @return The sequence or {@code -1} if not a valid sequence file.
   * @throws IOException
   */
  @VisibleForTesting
  public static int extractKeySequence(Path clusterKeyFile) throws IOException {
    int keySeq = -1;
    if (clusterKeyFile.getName().startsWith(SYSTEM_KEY_FILE_PREFIX)) {
      String seqStr = clusterKeyFile.getName().substring(SYSTEM_KEY_FILE_PREFIX.length());
      if (! seqStr.isEmpty()) {
        try {
          keySeq = Integer.valueOf(seqStr);
        } catch (NumberFormatException e) {
          throw new IOException("Invalid file name for a cluster key: " + clusterKeyFile, e);
        }
      }
    }
    return keySeq;
  }

  protected String loadKeyMetadata(Path keyPath) throws IOException {
    try (FSDataInputStream fin = getServer().getFileSystem().open(keyPath)) {
      return fin.readUTF();
    }
  }
}
