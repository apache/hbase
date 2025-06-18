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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class SystemKeyCache {
  private static final Logger LOG = LoggerFactory.getLogger(SystemKeyCache.class);

  private final ManagedKeyData latestSystemKey;
  private final Map<Long, ManagedKeyData> systemKeys;

  /**
   * Construct the System Key cache from the specified accessor.
   * @param accessor the accessor to use to load the system keys
   * @return the cache or {@code null} if no keys are found.
   * @throws IOException if there is an error loading the system keys
   */
  public static SystemKeyCache createCache(SystemKeyAccessor accessor) throws IOException {
    List<Path> allSystemKeyFiles = accessor.getAllSystemKeyFiles();
    if (allSystemKeyFiles.isEmpty()) {
      LOG.warn("No system key files found, skipping cache creation");
      return null;
    }
    ManagedKeyData latestSystemKey = null;
    Map<Long, ManagedKeyData> systemKeys = new TreeMap<>();
    for (Path keyPath: allSystemKeyFiles) {
      LOG.info("Loading system key from: {}", keyPath);
      ManagedKeyData keyData = accessor.loadSystemKey(keyPath);
      if (latestSystemKey == null) {
        latestSystemKey = keyData;
      }
      systemKeys.put(keyData.getKeyChecksum(), keyData);
    }
    return new SystemKeyCache(systemKeys, latestSystemKey);
  }

  private SystemKeyCache(Map<Long, ManagedKeyData> systemKeys, ManagedKeyData latestSystemKey) {
    this.systemKeys = systemKeys;
    this.latestSystemKey = latestSystemKey;
  }

  public ManagedKeyData getLatestSystemKey() {
    return latestSystemKey;
  }

  public ManagedKeyData getSystemKeyByChecksum(long checksum) {
    return systemKeys.get(checksum);
  }
}
