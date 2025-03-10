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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static org.apache.hadoop.hbase.HConstants.CLUSTER_KEY_FILE_PREFIX;

@InterfaceAudience.Private
public class PBEClusterKeyAccessor extends PBEKeyManager {
  protected final Path clusterKeyDir;

  public PBEClusterKeyAccessor(Server server) throws IOException {
    super(server);
    this.clusterKeyDir = CommonFSUtils.getClusterKeyDir(server.getConfiguration());
  }

  public Path getLatestClusterKeyFile() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    List<Path> allClusterKeyFiles = getAllClusterKeyFiles();
    if (allClusterKeyFiles.isEmpty()) {
      throw new RuntimeException("No cluster key initialized yet");
    }
    int currentMaxSeqNum = extractKeySequence(allClusterKeyFiles.get(0));
    return new Path(clusterKeyDir, CLUSTER_KEY_FILE_PREFIX + currentMaxSeqNum);
  }

  /**
   * Return all available cluster key files and return them in the order of latest to oldest.
   * If no cluster key files are available, then return an empty list. If PBE is not enabled,
   * then return null.
   *
   * @return  a list of all available cluster key files
   * @throws IOException
   */
  public List<Path> getAllClusterKeyFiles() throws IOException {
    if (!isPBEEnabled()) {
      return null;
    }
    FileSystem fs = server.getFileSystem();
    Map<Integer, Path> clusterKeys = new TreeMap<>(Comparator.reverseOrder());
    for (FileStatus st : fs.globStatus(new Path(clusterKeyDir, CLUSTER_KEY_FILE_PREFIX + "*"))) {
      Path keyPath = st.getPath();
      int seqNum = extractClusterKeySeqNum(keyPath);
      clusterKeys.put(seqNum, keyPath);
    }

    return new ArrayList<>(clusterKeys.values());
  }
  public PBEKeyData loadClusterKey(Path keyPath) throws IOException {
    PBEKeyProvider provider = getKeyProvider();
    return provider.unwrapKey(loadKeyMetadata(keyPath));
  }

  public int extractClusterKeySeqNum(Path keyPath) throws IOException {
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

  /**
   * Extract the key sequence number from the cluster key file name.
   * @param clusterKeyFile
   * @return The sequence or {@code -1} if not a valid sequence file.
   * @throws IOException
   */
  protected int extractKeySequence(Path clusterKeyFile) throws IOException {
    int keySeq = -1;
    if (clusterKeyFile.getName().startsWith(CLUSTER_KEY_FILE_PREFIX)) {
      keySeq = Integer.valueOf(clusterKeyFile.getName().substring(CLUSTER_KEY_FILE_PREFIX.length()));
    }
    return keySeq;
  }

  protected String loadKeyMetadata(Path keyPath) throws IOException {
    try (FSDataInputStream fin = server.getFileSystem().open(keyPath)) {
      return fin.readUTF();
    }
  }
}
