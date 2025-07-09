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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.when;

@Category({ MasterTests.class, SmallTests.class })
public class TestManagedKeyDataCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyDataCache.class);

  private static final String ALIAS = "cust1";
  private static final byte[] CUST_ID = ALIAS.getBytes();

  @Mock
  private Server server;

  private final MockManagedKeyProvider managedKeyProvider = new MockManagedKeyProvider();
  private ManagedKeyDataCache cache;
  protected Configuration conf = HBaseConfiguration.create();

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());

    // Configure the server mock to return the configuration
    when(server.getConfiguration()).thenReturn(conf);

    cache = new ManagedKeyDataCache(server);
    managedKeyProvider.initConfig(conf);
    managedKeyProvider.setMultikeyGenMode(true);
  }

  @Test
  public void testOperations() throws Exception {
    ManagedKeyData globalKey1 = managedKeyProvider.getManagedKey(CUST_ID,
      KEY_SPACE_GLOBAL);

    assertEquals(0, cache.getEntryCount());
    assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
    assertNull(cache.removeEntry(globalKey1.getKeyMetadata()));

    cache.addEntryForTesting(globalKey1);
    assertEntries(globalKey1);
    cache.addEntryForTesting(globalKey1);
    assertEntries(globalKey1);

    ManagedKeyData nsKey1 = managedKeyProvider.getManagedKey(CUST_ID,
      "namespace1");

    assertNull(cache.getEntry(CUST_ID, "namespace1", nsKey1.getKeyMetadata(), null));
    cache.addEntryForTesting(nsKey1);
    assertEquals(nsKey1, cache.getEntry(CUST_ID, "namespace1", nsKey1.getKeyMetadata(), null));
    assertEquals(globalKey1, cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
    assertEntries(nsKey1, globalKey1);

    ManagedKeyData globalKey2 = managedKeyProvider.getManagedKey(CUST_ID,
      KEY_SPACE_GLOBAL);
    assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey2.getKeyMetadata(), null));
    cache.addEntryForTesting(globalKey2);
    assertEntries(globalKey2, nsKey1, globalKey1);

    ManagedKeyData nsKey2 = managedKeyProvider.getManagedKey(CUST_ID,
      "namespace1");
    assertNull(cache.getEntry(CUST_ID, "namespace1", nsKey2.getKeyMetadata(), null));
    cache.addEntryForTesting(nsKey2);
    assertEntries(nsKey2, globalKey2, nsKey1, globalKey1);

    assertEquals(globalKey1, cache.removeEntry(globalKey1.getKeyMetadata()));
    assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey1.getKeyMetadata(), null));
    assertEntries(nsKey2, globalKey2, nsKey1);
    assertEquals(nsKey2, cache.removeEntry(nsKey2.getKeyMetadata()));
    assertNull(cache.getEntry(CUST_ID, "namespace1", nsKey2.getKeyMetadata(), null));
    assertEntries(globalKey2, nsKey1);
    assertEquals(nsKey1, cache.removeEntry(nsKey1.getKeyMetadata()));
    assertNull(cache.getEntry(CUST_ID, "namespace1", nsKey1.getKeyMetadata(), null));
    assertEntries(globalKey2);
    assertEquals(globalKey2, cache.removeEntry(globalKey2.getKeyMetadata()));
    assertNull(cache.getEntry(CUST_ID, KEY_SPACE_GLOBAL, globalKey2.getKeyMetadata(), null));
  }

  @Test
  public void testRandomKeyGet() throws Exception{
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));

    // Since getRandomEntry only looks at active keys cache, and we don't have a way to add directly to it,
    // we'll test that it returns null when no active keys are available
    List<ManagedKeyData> allKeys = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      ManagedKeyData keyData;
      keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      cache.addEntryForTesting(keyData);
      allKeys.add(keyData);
      keyData = managedKeyProvider.getManagedKey(CUST_ID, "namespace");
      cache.addEntryForTesting(keyData);
      allKeys.add(keyData);
    }

    // getRandomEntry should return null since no keys are in the active keys cache
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));

    for(ManagedKeyData key: allKeys) {
      assertEquals(key, cache.removeEntry(key.getKeyMetadata()));
    }
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
  }

  @Test
  public void testRandomKeyGetNoActive() throws Exception {
    managedKeyProvider.setMockedKeyState(ALIAS, FAILED);
    for (int i = 0; i < 20; ++i) {
      cache.addEntryForTesting(managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL));
    }
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
  }

  private void assertEntries(ManagedKeyData... keys) throws Exception {
    assertEquals(keys.length, cache.getEntryCount());
    for (ManagedKeyData key: keys) {
      assertEquals(key, cache.getEntry(key.getKeyCustodian(), key.getKeyNamespace(), key.getKeyMetadata(), null));
    }
  }
}
