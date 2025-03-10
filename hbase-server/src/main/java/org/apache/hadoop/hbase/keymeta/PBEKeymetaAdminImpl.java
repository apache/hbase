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

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Base64;

@InterfaceAudience.Private
public class PBEKeymetaAdminImpl extends PBEKeymetaTableAccessor implements PBEKeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(PBEKeymetaAdminImpl.class);

  public PBEKeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public PBEKeyStatus enablePBE(String pbePrefix, String keyNamespace) throws IOException {
    if (! isPBEEnabled()) {
      throw new IOException("PBE is currently not enabled in HBase configuration");
    }
    LOG.info("Trying to enable PBE on key: {} for namespace: {}", pbePrefix, keyNamespace);
    byte[] pbe_prefix;
    try {
      pbe_prefix = Base64.getDecoder().decode(pbePrefix);
    }
    catch (IllegalArgumentException e) {
      throw new IOException("Failed to decode specified prefix as Base64 string: " + pbePrefix, e);
    }
    PBEKeyProvider provider = getKeyProvider();
    PBEKeyData pbeKey = provider.getPBEKey(pbe_prefix, keyNamespace);
    LOG.info("Got key data with status: {} and metadata: {} for prefix: {}", pbeKey.getKeyStatus(),
      pbeKey.getKeyMetadata(), pbePrefix);
    addKey(pbeKey);
    return pbeKey.getKeyStatus();
  }
}
