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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * STUB INTERFACE - Feature not yet complete. This interface will be fully implemented in
 * HBASE-29368 feature PR.
 */
@InterfaceAudience.Private
public interface KeyManagementService {

  /**
   * No-op implementation for precursor PR.
   */
  KeyManagementService NONE = new KeyManagementService() {
  };

  /**
   * Creates a default key management service. Returns NONE for precursor PR.
   */
  static KeyManagementService createDefault(Configuration conf, FileSystem fs) {
    return NONE;
  }

  /**
   * Returns the managed key data cache.
   * @return the managed key data cache, or null if not available
   */
  default ManagedKeyDataCache getManagedKeyDataCache() {
    return null;
  }

  /**
   * Returns the system key cache.
   * @return the system key cache, or null if not available
   */
  default SystemKeyCache getSystemKeyCache() {
    return null;
  }

  /**
   * Returns the keymeta admin.
   * @return the keymeta admin, or null if not available
   */
  default KeymetaAdmin getKeymetaAdmin() {
    return null;
  }
}
