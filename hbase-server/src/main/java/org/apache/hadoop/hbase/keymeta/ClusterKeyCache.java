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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
public class ClusterKeyCache {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterKeyCache.class);

  private final ClusterKeyAccessor accessor;
  private PBEKeyData latestClusterKey;
  private Map<Long, PBEKeyData> clusterKeys = new HashMap<>();

  public ClusterKeyCache(ClusterKeyAccessor accessor) throws IOException {
    this.accessor = accessor;

    List<Path> allClusterKeys = accessor.getAllClusterKeys();
    int latestKeySequence = accessor.findLatestKeySequence(allClusterKeys);
    for (Path keyPath: allClusterKeys) {
      LOG.info("Loading cluster key from: {}", keyPath);
      PBEKeyData keyData = accessor.loadClusterKey(keyPath);
      if (accessor.extractClusterKeySeqNum(keyPath) == latestKeySequence) {
        latestClusterKey = keyData;
      }
      clusterKeys.put(keyData.getKeyChecksum(), keyData);
    }
  }

  public PBEKeyData getLatestClusterKey() {
    return latestClusterKey;
  }

  public PBEKeyData getClusterKeyByChecksum(long checksum) {
    return clusterKeys.get(checksum);
  }
}
