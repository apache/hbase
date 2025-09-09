
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface KeyManagementService {
  class DefaultKeyManagementService implements KeyManagementService {
    private final Configuration configuration;
    private final ManagedKeyDataCache managedKeyDataCache;
    private final SystemKeyCache systemKeyCache;

    public DefaultKeyManagementService(Configuration configuration, FileSystem fs) {
      this.configuration = configuration;
      this.managedKeyDataCache = new ManagedKeyDataCache(configuration, null);
      try {
        this.systemKeyCache = SystemKeyCache.createCache(configuration, fs);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create system key cache", e);
      }
    }

    @Override
    public SystemKeyCache getSystemKeyCache() {
      return systemKeyCache;
    }

    @Override
    public ManagedKeyDataCache getManagedKeyDataCache() {
      return managedKeyDataCache;
    }

    @Override
    public KeymetaAdmin getKeymetaAdmin() {
      throw new UnsupportedOperationException("KeymetaAdmin is not supported");
    }

    @Override
    public Configuration getConfiguration() {
      return configuration;
    }
  }

  static KeyManagementService createDefault(Configuration configuration, FileSystem fs) {
    return new DefaultKeyManagementService(configuration, fs);
  }

  /**
   * @return the cache for cluster keys.
   */
  public SystemKeyCache getSystemKeyCache();

  /**
   * @return the cache for managed keys.
   */
  public ManagedKeyDataCache getManagedKeyDataCache();

  /**
   * @return the admin for keymeta.
   */
  public KeymetaAdmin getKeymetaAdmin();

  /**
   * @return the configuration.
   */
  public Configuration getConfiguration();
}