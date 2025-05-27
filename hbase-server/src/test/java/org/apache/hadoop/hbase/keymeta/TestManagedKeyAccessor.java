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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.ArrayList;
import java.util.Arrays;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category({ MasterTests.class, SmallTests.class })
public class TestManagedKeyAccessor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestManagedKeyAccessor.class);

  private static final String ALIAS = "cust1";
  private static final byte[] CUST_ID = ALIAS.getBytes();

  @Mock
  private KeymetaTableAccessor keymetaAccessor;
  @Mock
  private ManagedKeyDataCache keyDataCache;
  @Mock
  private Server server;

  private ManagedKeyAccessor managedKeyAccessor;
  private AutoCloseable closeableMocks;
  private MockManagedKeyProvider managedKeyProvider;
  protected Configuration conf = HBaseConfiguration.create();

  @Before
  public void setUp() {
    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());

    closeableMocks = MockitoAnnotations.openMocks(this);
    managedKeyProvider = (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
    managedKeyProvider.initConfig(conf);
    when(server.getConfiguration()).thenReturn(conf);
    when(keymetaAccessor.getServer()).thenReturn(server);
    managedKeyAccessor = new ManagedKeyAccessor(keymetaAccessor, keyDataCache);
  }

  @After
  public void tearDown() throws Exception {
    closeableMocks.close();
  }

  @Test
  public void testGetKeyNonExisting() throws Exception {
    for (int i = 0; i < 2; ++i) {
      ManagedKeyData keyData = managedKeyAccessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, "abcd",
        null);
      verifyNonExisting(keyData);
    }
  }

  private void verifyNonExisting(ManagedKeyData keyData) throws Exception {
    assertNull(keyData);
    verify(keyDataCache).getEntry("abcd");
    verify(keymetaAccessor).getKey(CUST_ID, KEY_SPACE_GLOBAL, "abcd");
    verify(keymetaAccessor, never()).addKey(any());
    verify(keyDataCache, never()).addEntry(any());
    clearInvocations(keyDataCache, keymetaAccessor);
  }

  @Test
  public void testGetFromL1() throws Exception {
    ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    when(keyDataCache.getEntry(any())).thenReturn(keyData);

    ManagedKeyData result =
      managedKeyAccessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, keyData.getKeyMetadata(), null);

    assertEquals(keyData, result);
    verify(keyDataCache).getEntry(keyData.getKeyMetadata());
    verify(keymetaAccessor, never()).getKey(any(), any(), any(String.class));
    verify(keymetaAccessor, never()).addKey(any());
    verify(keyDataCache, never()).addEntry(keyData);
  }

  @Test
  public void testGetFromL2() throws Exception {
    ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    when(keymetaAccessor.getKey(any(), any(), any(String.class))).thenReturn(keyData);

    ManagedKeyData result =
      managedKeyAccessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, keyData.getKeyMetadata(), null);

    assertEquals(keyData, result);
    verify(keyDataCache).getEntry(keyData.getKeyMetadata());
    verify(keymetaAccessor).getKey(CUST_ID, KEY_SPACE_GLOBAL, keyData.getKeyMetadata());
    verify(keymetaAccessor, never()).addKey(any());
    verify(keyDataCache).addEntry(keyData);
  }

  @Test
  public void testGetFromProvider() throws Exception {
    ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);

    ManagedKeyData result =
      managedKeyAccessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, keyData.getKeyMetadata(), null);

    assertEquals(keyData, result);
    verify(keyDataCache).getEntry(keyData.getKeyMetadata());
    verify(keymetaAccessor).getKey(CUST_ID, KEY_SPACE_GLOBAL, keyData.getKeyMetadata());
    verify(keymetaAccessor).addKey(any());
    verify(keyDataCache).addEntry(keyData);
  }

  @Test
  public void testGetActiveKeyWhenMissing() throws Exception {
    ManagedKeyData result = managedKeyAccessor.getAnActiveKey(CUST_ID, KEY_SPACE_GLOBAL);

    assertNull(result);
    verify(keyDataCache).getRandomEntryForPrefix(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keymetaAccessor).getActiveKeys(CUST_ID, KEY_SPACE_GLOBAL);
  }

  @Test
  public void testGetActiveKeyFromL1() throws Exception {
    ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    when(keyDataCache.getRandomEntryForPrefix(any(), any())).thenReturn(keyData);

    ManagedKeyData result = managedKeyAccessor.getAnActiveKey(CUST_ID, KEY_SPACE_GLOBAL);

    assertEquals(keyData, result);
    verify(keyDataCache).getRandomEntryForPrefix(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keymetaAccessor, never()).getActiveKeys(any(), any());
  }

  @Test
  public void testGetActiveKeyFromL2WithNoResults() throws Exception {
    when(keymetaAccessor.getActiveKeys(any(), any())).thenReturn(new ArrayList<>());

    ManagedKeyData result = managedKeyAccessor.getAnActiveKey(CUST_ID, KEY_SPACE_GLOBAL);

    assertNull(result);
    verify(keyDataCache).getRandomEntryForPrefix(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keymetaAccessor).getActiveKeys(CUST_ID, KEY_SPACE_GLOBAL);
  }

  @Test
  public void testGetActiveKeyFromL2WithSingleResult() throws Exception {
    ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    when(keyDataCache.getRandomEntryForPrefix(any(), any())).thenReturn(null, keyData);
    when(keymetaAccessor.getActiveKeys(any(), any())).thenReturn(Arrays.asList(keyData));

    ManagedKeyData result = managedKeyAccessor.getAnActiveKey(CUST_ID, KEY_SPACE_GLOBAL);

    assertEquals(keyData, result);
    verify(keyDataCache, times(2)).getRandomEntryForPrefix(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keymetaAccessor).getActiveKeys(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keyDataCache).addEntry(keyData);
  }

  @Test
  public void testGetActiveKeyFromL2WithMultipleResults() throws Exception {
    managedKeyProvider.setMultikeyGenMode(true);
    ManagedKeyData keyData1 = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    ManagedKeyData keyData2 = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);
    when(keyDataCache.getRandomEntryForPrefix(any(), any())).thenReturn(null, keyData1);
    when(keymetaAccessor.getActiveKeys(any(), any())).thenReturn(Arrays.asList(keyData1, keyData2));

    ManagedKeyData result = managedKeyAccessor.getAnActiveKey(CUST_ID, KEY_SPACE_GLOBAL);

    assertEquals(keyData1, result);
    verify(keyDataCache, times(2)).getRandomEntryForPrefix(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keymetaAccessor).getActiveKeys(CUST_ID, KEY_SPACE_GLOBAL);
    verify(keyDataCache, times(2)).addEntry(any());
  }
}
