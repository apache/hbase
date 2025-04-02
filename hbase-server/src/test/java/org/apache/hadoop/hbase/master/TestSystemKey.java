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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockPBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.keymeta.SystemKeyAccessor;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.keymeta.PBETestBase;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.security.Key;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category({ MasterTests.class, MediumTests.class })
public class TestSystemKey extends PBETestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSystemKey.class);

  @Test
  public void testSystemKeyInitializationAndRotation() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeyProvider keyProvider = Encryption.getKeyProvider(master.getConfiguration());
    assertNotNull(keyProvider);
    assertTrue(keyProvider instanceof PBEKeyProvider);
    assertTrue(keyProvider instanceof MockPBEKeyProvider);
    MockPBEKeyProvider pbeKeyProvider = (MockPBEKeyProvider) keyProvider;
    PBEKeyData initialSystemKey = validateInitialState(master, pbeKeyProvider);

    restartSystem();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    validateInitialState(master, pbeKeyProvider);

    // Test rotation of cluster key by changing the key that the key provider provides and restart master.
    String newAlias = "new_cluster_key";
    pbeKeyProvider.setCluterKeyAlias(newAlias);
    Key newCluterKey = MockPBEKeyProvider.generateSecretKey();
    pbeKeyProvider.setKey(newAlias, newCluterKey);
    restartSystem();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    SystemKeyAccessor systemKeyAccessor = new SystemKeyAccessor(master);
    assertEquals(2, systemKeyAccessor.getAllSystemKeyFiles().size());
    SystemKeyCache systemKeyCache = master.getSystemKeyCache();
    assertEquals(0, Bytes.compareTo(newCluterKey.getEncoded(),
      systemKeyCache.getLatestSystemKey().getTheKey().getEncoded()));
    assertEquals(initialSystemKey,
      systemKeyAccessor.loadSystemKey(systemKeyAccessor.getAllSystemKeyFiles().get(1)));
    assertEquals(initialSystemKey,
      systemKeyCache.getSystemKeyByChecksum(initialSystemKey.getKeyChecksum()));
  }

  @Test
  public void testWithInvalidSystemKey() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeyProvider keyProvider = Encryption.getKeyProvider(master.getConfiguration());
    MockPBEKeyProvider pbeKeyProvider = (MockPBEKeyProvider) keyProvider;

    // Test startup failure when the cluster key is INACTIVE
    SystemKeyManager tmpCKM = new SystemKeyManager(master);
    tmpCKM.ensureSystemKeyInitialized();
    pbeKeyProvider.setKeyStatus(pbeKeyProvider.getSystemKeyAlias(), PBEKeyStatus.INACTIVE);
    assertThrows(IOException.class, tmpCKM::ensureSystemKeyInitialized);
  }

  private PBEKeyData validateInitialState(HMaster master, MockPBEKeyProvider pbeKeyProvider )
    throws IOException {
    SystemKeyAccessor systemKeyAccessor = new SystemKeyAccessor(master);
    assertEquals(1, systemKeyAccessor.getAllSystemKeyFiles().size());
    SystemKeyCache systemKeyCache = master.getSystemKeyCache();
    assertNotNull(systemKeyCache);
    PBEKeyData clusterKey = systemKeyCache.getLatestSystemKey();
    assertEquals(pbeKeyProvider.getSystemKey(master.getClusterId().getBytes()), clusterKey);
    assertEquals(clusterKey,
      systemKeyCache.getSystemKeyByChecksum(clusterKey.getKeyChecksum()));
    return clusterKey;
  }

  private void restartSystem() throws Exception {
    TEST_UTIL.shutdownMiniHBaseCluster();
    Thread.sleep(2000);
    TEST_UTIL.restartHBaseCluster(1);
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
  }
}
