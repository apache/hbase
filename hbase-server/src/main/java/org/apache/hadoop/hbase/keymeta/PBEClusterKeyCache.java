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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@InterfaceAudience.Private
public class PBEClusterKeyCache {
  private static final Logger LOG = LoggerFactory.getLogger(PBEClusterKeyCache.class);

  private final PBEKeyData latestClusterKey;
  private final Map<Long, PBEKeyData> clusterKeys;

  /**
   * Construct the Cluster Key cache from the specified accessor.
   * @param accessor
   * @return the cache or {@code null} if no keys are found.
   * @throws IOException
   */
  public static PBEClusterKeyCache createCache(PBEClusterKeyAccessor accessor) throws IOException {
    List<Path> allClusterKeyFiles = accessor.getAllClusterKeyFiles();
    if (allClusterKeyFiles.isEmpty()) {
      LOG.warn("No cluster key files found, skipping cache creation");
      return null;
    }
    PBEKeyData latestClusterKey = null;
    Map<Long, PBEKeyData> clusterKeys = new TreeMap<>();
    for (Path keyPath: allClusterKeyFiles) {
      LOG.info("Loading cluster key from: {}", keyPath);
      PBEKeyData keyData = accessor.loadClusterKey(keyPath);
      if (latestClusterKey == null) {
        latestClusterKey = keyData;
      }
      clusterKeys.put(keyData.getKeyChecksum(), keyData);
    }
    return new PBEClusterKeyCache(clusterKeys, latestClusterKey);
  }

  private PBEClusterKeyCache(Map<Long, PBEKeyData> clusterKeys, PBEKeyData latestClusterKey) {
    this.clusterKeys = clusterKeys;
    this.latestClusterKey = latestClusterKey;
  }

  public PBEKeyData getLatestClusterKey() {
    return latestClusterKey;
  }

  public PBEKeyData getClusterKeyByChecksum(long checksum) {
    return clusterKeys.get(checksum);
  }
}
