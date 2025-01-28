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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.Server;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PBEKeyManager {
  protected static final Logger LOG = LoggerFactory.getLogger(PBEKeyManager.class);

  protected final Server server;

  private Boolean pbeEnabled;

  public PBEKeyManager(Server server) {
    this.server = server;
  }

  protected PBEKeyProvider getKeyProvider() {
    KeyProvider provider = Encryption.getKeyProvider(server.getConfiguration());
    if (!(provider instanceof PBEKeyProvider)) {
      throw new RuntimeException(
        "KeyProvider: " + provider.getClass().getName() + " expected to be of type PBEKeyProvider");
    }
    return (PBEKeyProvider) provider;
  }

  protected boolean isPBEEnabled() {
    if (pbeEnabled == null) {
      pbeEnabled = server.getConfiguration().getBoolean(HConstants.CRYPTO_PBE_ENABLED_CONF_KEY,
        false);
    }
    return pbeEnabled;
  }
}
