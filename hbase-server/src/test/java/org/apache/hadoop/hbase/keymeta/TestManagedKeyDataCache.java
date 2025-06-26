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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestManagedKeyDataCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyDataCache.class);

  private static final String ALIAS = "cust1";
  private static final byte[] CUST_ID = ALIAS.getBytes();

  private final MockManagedKeyProvider managedKeyProvider = new MockManagedKeyProvider();
  private ManagedKeyDataCache cache;
  protected Configuration conf = HBaseConfiguration.create();

  @Before
  public void setUp() {
    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());

    cache = new ManagedKeyDataCache();
    managedKeyProvider.initConfig(conf);
    managedKeyProvider.setMultikeyGenMode(true);
  }

  @Test
  public void testOperations() throws Exception {
    ManagedKeyData globalKey1 = managedKeyProvider.getManagedKey(CUST_ID,
      KEY_SPACE_GLOBAL);

    assertEquals(0, cache.getEntryCount());
    assertNull(cache.getEntry(globalKey1.getKeyMetadata()));
    assertNull(cache.removeEntry(globalKey1.getKeyMetadata()));

    cache.addEntry(globalKey1);
    assertEntries(globalKey1);
    cache.addEntry(globalKey1);
    assertEntries(globalKey1);

    ManagedKeyData nsKey1 = managedKeyProvider.getManagedKey(CUST_ID,
      "namespace1");

    assertNull(cache.getEntry(nsKey1.getKeyMetadata()));
    cache.addEntry(nsKey1);
    assertEquals(nsKey1, cache.getEntry(nsKey1.getKeyMetadata()));
    assertEquals(globalKey1, cache.getEntry(globalKey1.getKeyMetadata()));
    assertEntries(nsKey1, globalKey1);

    ManagedKeyData globalKey2 = managedKeyProvider.getManagedKey(CUST_ID,
      KEY_SPACE_GLOBAL);
    assertNull(cache.getEntry(globalKey2.getKeyMetadata()));
    cache.addEntry(globalKey2);
    assertEntries(globalKey2, nsKey1, globalKey1);

    ManagedKeyData nsKey2 = managedKeyProvider.getManagedKey(CUST_ID,
      "namespace1");
    assertNull(cache.getEntry(nsKey2.getKeyMetadata()));
    cache.addEntry(nsKey2);
    assertEntries(nsKey2, globalKey2, nsKey1, globalKey1);

    assertEquals(globalKey1, cache.removeEntry(globalKey1.getKeyMetadata()));
    assertNull(cache.getEntry(globalKey1.getKeyMetadata()));
    assertEntries(nsKey2, globalKey2, nsKey1);
    assertEquals(nsKey2, cache.removeEntry(nsKey2.getKeyMetadata()));
    assertNull(cache.getEntry(nsKey2.getKeyMetadata()));
    assertEntries(globalKey2, nsKey1);
    assertEquals(nsKey1, cache.removeEntry(nsKey1.getKeyMetadata()));
    assertNull(cache.getEntry(nsKey1.getKeyMetadata()));
    assertEntries(globalKey2);
    assertEquals(globalKey2, cache.removeEntry(globalKey2.getKeyMetadata()));
    assertNull(cache.getEntry(globalKey2.getKeyMetadata()));
  }

  @Test
  public void testRandomKeyGet() throws Exception{
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
    List<ManagedKeyData> allKeys = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      ManagedKeyData keyData;
      keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
      cache.addEntry(keyData);
      allKeys.add(keyData);
      keyData = managedKeyProvider.getManagedKey(CUST_ID, "namespace");
      cache.addEntry(keyData);
      allKeys.add(keyData);
    }
    Set<ManagedKeyData> keys = new HashSet<>();
    for (int i = 0; i < 10; ++i) {
      keys.add(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
    }
    assertTrue(keys.size() > 1);
    assertTrue(keys.size() <= 10);
    for (ManagedKeyData key: keys) {
      assertEquals(KEY_SPACE_GLOBAL, key.getKeyNamespace());
    }

    for(ManagedKeyData key: allKeys) {
      assertEquals(key, cache.removeEntry(key.getKeyMetadata()));
    }
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
  }

  @Test
  public void testRandomKeyGetNoActive() throws Exception {
    managedKeyProvider.setMockedKeyState(ALIAS, FAILED);
    for (int i = 0; i < 20; ++i) {
      cache.addEntry(managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL));
    }
    assertNull(cache.getRandomEntry(CUST_ID, KEY_SPACE_GLOBAL));
  }

  private void assertEntries(ManagedKeyData... keys) {
    assertEquals(keys.length, cache.getEntryCount());
    for (ManagedKeyData key: keys) {
      assertEquals(key, cache.getEntry(key.getKeyMetadata()));
    }
  }
}
