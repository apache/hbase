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
import java.util.List;
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
    int currentMaxSeqNum = findLatestKeySequence(getAllClusterKeys());
    return new Path(clusterKeyDir, CLUSTER_KEY_FILE_PREFIX + currentMaxSeqNum);
  }

  public List<Path> getAllClusterKeys() throws IOException {
    if (! isPBEEnabled()) {
      return null;
    }
    FileSystem fs = server.getFileSystem();
    List<Path> clusterKeys = new ArrayList<>();
    for (FileStatus st: fs.globStatus(new Path(clusterKeyDir, CLUSTER_KEY_FILE_PREFIX + "*"))) {
      Path keyPath = st.getPath();
      extractClusterKeySeqNum(keyPath); // Just check for validity.
      clusterKeys.add(keyPath);
    }
    return clusterKeys;
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

  protected int findLatestKeySequence(List<Path> clusterKeys) throws IOException {
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

  protected String loadKeyMetadata(Path keyPath) throws IOException {
    try (FSDataInputStream fin = server.getFileSystem().open(keyPath)) {
      return fin.readUTF();
    }
  }
}
