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
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.io.crypto.MockPBEKeyProvider;
import org.apache.hadoop.hbase.keymeta.PBEClusterKeyCache;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.security.Key;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({ MasterTests.class, MediumTests.class })
public class TestPBEClusterKey {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPBEClusterKey.class);

  private HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  public static final String CLUSTER_KEY_ALIAS = "cluster-key";
  public static final byte[] CLUSTER_ID = CLUSTER_KEY_ALIAS.getBytes();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockPBEKeyProvider.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_PBE_ENABLED_CONF_KEY, "true");

    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClusterKeyInitializationAndRotation() throws Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    KeyProvider keyProvider = Encryption.getKeyProvider(master.getConfiguration());
    assertNotNull(keyProvider);
    assertTrue(keyProvider instanceof PBEKeyProvider);
    assertTrue(keyProvider instanceof MockPBEKeyProvider);
    MockPBEKeyProvider pbeKeyProvider = (MockPBEKeyProvider) keyProvider;
    PBEClusterKeyCache pbeClusterKeyCache = master.getPBEClusterKeyCache();
    assertNotNull(pbeClusterKeyCache);
    assertEquals(pbeKeyProvider.getClusterKey(master.getClusterId().getBytes()),
      pbeClusterKeyCache.getLatestClusterKey());

    // Test rotation of cluster key by changing the key that the key provider provides and restart master.
    Key newCluterKey = MockPBEKeyProvider.generateSecretKey();
    pbeKeyProvider.setKey(master.getClusterId().getBytes(), newCluterKey);
    TEST_UTIL.shutdownMiniHBaseCluster();
    Thread.sleep(2000);
    TEST_UTIL.restartHBaseCluster(1);
    TEST_UTIL.waitFor(60000, () -> TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    master = TEST_UTIL.getHBaseCluster().getMaster();
    assertEquals(newCluterKey, master.getPBEClusterKeyCache().getLatestClusterKey().getTheKey());
  }
}
