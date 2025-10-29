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
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_CHECKSUM_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_METADATA_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_WRAPPED_BY_STK_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.KEY_META_INFO_FAMILY;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.KEY_STATE_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.REFRESHED_TIMESTAMP_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.STK_CHECKSUM_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.constructRowKeyForCustNamespace;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.constructRowKeyForMetadata;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.parseFromResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Suite;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaTableAccessor.TestAdd.class,
  TestKeymetaTableAccessor.TestAddWithNullableFields.class,
  TestKeymetaTableAccessor.TestGet.class, })
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaTableAccessor {
  protected static final String ALIAS = "custId1";
  protected static final byte[] CUST_ID = ALIAS.getBytes();
  protected static final String KEY_NAMESPACE = "namespace";
  protected static String KEY_METADATA = "metadata1";

  @Mock
  protected MasterServices server;
  @Mock
  protected Connection connection;
  @Mock
  protected Table table;
  @Mock
  protected ResultScanner scanner;
  @Mock
  protected SystemKeyCache systemKeyCache;

  protected KeymetaTableAccessor accessor;
  protected Configuration conf = HBaseConfiguration.create();
  protected MockManagedKeyProvider managedKeyProvider;
  protected ManagedKeyData latestSystemKey;

  private AutoCloseable closeableMocks;

  @Before
  public void setUp() throws Exception {
    closeableMocks = MockitoAnnotations.openMocks(this);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
      MockManagedKeyProvider.class.getName());

    when(server.getConnection()).thenReturn(connection);
    when(connection.getTable(KeymetaTableAccessor.KEY_META_TABLE_NAME)).thenReturn(table);
    when(server.getSystemKeyCache()).thenReturn(systemKeyCache);
    when(server.getConfiguration()).thenReturn(conf);
    when(server.getKeyManagementService()).thenReturn(server);

    accessor = new KeymetaTableAccessor(server);
    managedKeyProvider = new MockManagedKeyProvider();
    managedKeyProvider.initConfig(conf, "");

    latestSystemKey = managedKeyProvider.getSystemKey("system-id".getBytes());
    when(systemKeyCache.getLatestSystemKey()).thenReturn(latestSystemKey);
    when(systemKeyCache.getSystemKeyByChecksum(anyLong())).thenReturn(latestSystemKey);
  }

  @After
  public void tearDown() throws Exception {
    closeableMocks.close();
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAdd extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAdd.class);

    @Parameter(0)
    public ManagedKeyState keyState;

    @Parameterized.Parameters(name = "{index},keyState={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { ACTIVE }, { FAILED }, { INACTIVE }, { DISABLED }, });
    }

    @Test
    public void testAddKey() throws Exception {
      managedKeyProvider.setMockedKeyState(ALIAS, keyState);
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_ID, KEY_SPACE_GLOBAL);

      accessor.addKey(keyData);

      ArgumentCaptor<List<Put>> putCaptor = ArgumentCaptor.forClass(ArrayList.class);
      verify(table).put(putCaptor.capture());
      List<Put> puts = putCaptor.getValue();
      assertEquals(keyState == ACTIVE ? 2 : 1, puts.size());
      if (keyState == ACTIVE) {
        assertPut(keyData, puts.get(0), constructRowKeyForCustNamespace(keyData));
        assertPut(keyData, puts.get(1), constructRowKeyForMetadata(keyData));
      } else {
        assertPut(keyData, puts.get(0), constructRowKeyForMetadata(keyData));
      }
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAddWithNullableFields extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAddWithNullableFields.class);

    @Test
    public void testAddKeyWithFailedStateAndNullMetadata() throws Exception {
      managedKeyProvider.setMockedKeyState(ALIAS, FAILED);
      ManagedKeyData keyData = new ManagedKeyData(CUST_ID, KEY_SPACE_GLOBAL, null, FAILED, null);

      accessor.addKey(keyData);

      ArgumentCaptor<List<Put>> putCaptor = ArgumentCaptor.forClass(ArrayList.class);
      verify(table).put(putCaptor.capture());
      List<Put> puts = putCaptor.getValue();
      assertEquals(1, puts.size());
      Put put = puts.get(0);

      // Verify the row key uses state value for metadata hash
      byte[] expectedRowKey =
        constructRowKeyForMetadata(CUST_ID, KEY_SPACE_GLOBAL, new byte[] { FAILED.getVal() });
      assertEquals(0, Bytes.compareTo(expectedRowKey, put.getRow()));

      Map<Bytes, Bytes> valueMap = getValueMap(put);

      // Verify key-related columns are not present
      assertNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));

      // Verify state is set correctly
      assertEquals(new Bytes(new byte[] { FAILED.getVal() }),
        valueMap.get(new Bytes(KEY_STATE_QUAL_BYTES)));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestGet extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestGet.class);

    @Mock
    private Result result1;
    @Mock
    private Result result2;

    private String keyMetadata2 = "metadata2";

    @Override
    public void setUp() throws Exception {
      super.setUp();

      when(result1.getValue(eq(KEY_META_INFO_FAMILY), eq(KEY_STATE_QUAL_BYTES)))
        .thenReturn(new byte[] { ACTIVE.getVal() });
      when(result2.getValue(eq(KEY_META_INFO_FAMILY), eq(KEY_STATE_QUAL_BYTES)))
        .thenReturn(new byte[] { FAILED.getVal() });
      for (Result result : Arrays.asList(result1, result2)) {
        when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(REFRESHED_TIMESTAMP_QUAL_BYTES)))
          .thenReturn(Bytes.toBytes(0L));
        when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(STK_CHECKSUM_QUAL_BYTES)))
          .thenReturn(Bytes.toBytes(0L));
      }
      when(result1.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_METADATA_QUAL_BYTES)))
        .thenReturn(KEY_METADATA.getBytes());
      when(result2.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_METADATA_QUAL_BYTES)))
        .thenReturn(keyMetadata2.getBytes());
    }

    @Test
    public void testParseEmptyResult() throws Exception {
      Result result = mock(Result.class);
      when(result.isEmpty()).thenReturn(true);

      assertNull(parseFromResult(server, CUST_ID, KEY_NAMESPACE, null));
      assertNull(parseFromResult(server, CUST_ID, KEY_NAMESPACE, result));
    }

    @Test
    public void testGetActiveKeyMissingWrappedKey() throws Exception {
      Result result = mock(Result.class);
      when(table.get(any(Get.class))).thenReturn(result);
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(KEY_STATE_QUAL_BYTES)))
        .thenReturn(new byte[] { ACTIVE.getVal() }, new byte[] { INACTIVE.getVal() });

      IOException ex;
      ex = assertThrows(IOException.class,
        () -> accessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, KEY_METADATA));
      assertEquals("ACTIVE key must have a wrapped key", ex.getMessage());
      ex = assertThrows(IOException.class,
        () -> accessor.getKey(CUST_ID, KEY_SPACE_GLOBAL, KEY_METADATA));
      assertEquals("INACTIVE key must have a wrapped key", ex.getMessage());
    }

    @Test
    public void testGetKeyMissingSTK() throws Exception {
      when(result1.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_WRAPPED_BY_STK_QUAL_BYTES)))
        .thenReturn(new byte[] { 0 });
      when(systemKeyCache.getSystemKeyByChecksum(anyLong())).thenReturn(null);
      when(table.get(any(Get.class))).thenReturn(result1);

      ManagedKeyData result = accessor.getKey(CUST_ID, KEY_NAMESPACE, KEY_METADATA);

      assertNull(result);
    }

    @Test
    public void testGetKeyWithWrappedKey() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      ManagedKeyData result = accessor.getKey(CUST_ID, KEY_NAMESPACE, KEY_METADATA);

      verify(table).get(any(Get.class));
      assertNotNull(result);
      assertEquals(0, Bytes.compareTo(CUST_ID, result.getKeyCustodian()));
      assertEquals(KEY_NAMESPACE, result.getKeyNamespace());
      assertEquals(keyData.getKeyMetadata(), result.getKeyMetadata());
      assertEquals(0,
        Bytes.compareTo(keyData.getTheKey().getEncoded(), result.getTheKey().getEncoded()));
      assertEquals(ACTIVE, result.getKeyState());

      // When DEK checksum doesn't match, we expect a null value.
      result = accessor.getKey(CUST_ID, KEY_NAMESPACE, KEY_METADATA);
      assertNull(result);
    }

    @Test
    public void testGetKeyWithoutWrappedKey() throws Exception {
      when(table.get(any(Get.class))).thenReturn(result2);

      ManagedKeyData result = accessor.getKey(CUST_ID, KEY_NAMESPACE, KEY_METADATA);

      verify(table).get(any(Get.class));
      assertNotNull(result);
      assertEquals(0, Bytes.compareTo(CUST_ID, result.getKeyCustodian()));
      assertEquals(KEY_NAMESPACE, result.getKeyNamespace());
      assertEquals(keyMetadata2, result.getKeyMetadata());
      assertNull(result.getTheKey());
      assertEquals(FAILED, result.getKeyState());
    }

    @Test
    public void testGetAllKeys() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      when(scanner.iterator()).thenReturn(List.of(result1, result2).iterator());
      when(table.getScanner(any(Scan.class))).thenReturn(scanner);

      List<ManagedKeyData> allKeys = accessor.getAllKeys(CUST_ID, KEY_NAMESPACE);

      assertEquals(2, allKeys.size());
      assertEquals(keyData.getKeyMetadata(), allKeys.get(0).getKeyMetadata());
      assertEquals(keyMetadata2, allKeys.get(1).getKeyMetadata());
      verify(table).getScanner(any(Scan.class));
    }

    @Test
    public void testGetActiveKey() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      when(scanner.iterator()).thenReturn(List.of(result1).iterator());
      when(table.get(any(Get.class))).thenReturn(result1);

      ManagedKeyData activeKey = accessor.getActiveKey(CUST_ID, KEY_NAMESPACE);

      assertNotNull(activeKey);
      assertEquals(keyData, activeKey);
      verify(table).get(any(Get.class));
    }

    private ManagedKeyData setupActiveKey(byte[] custId, Result result) throws Exception {
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(custId, KEY_NAMESPACE);
      byte[] dekWrappedBySTK =
        EncryptionUtil.wrapKey(conf, null, keyData.getTheKey(), latestSystemKey.getTheKey());
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_WRAPPED_BY_STK_QUAL_BYTES)))
        .thenReturn(dekWrappedBySTK);
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_CHECKSUM_QUAL_BYTES)))
        .thenReturn(Bytes.toBytes(keyData.getKeyChecksum()), Bytes.toBytes(0L));
      // Update the mock to return the correct metadata from the keyData
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_METADATA_QUAL_BYTES)))
        .thenReturn(keyData.getKeyMetadata().getBytes());
      when(table.get(any(Get.class))).thenReturn(result);
      return keyData;
    }
  }

  protected void assertPut(ManagedKeyData keyData, Put put, byte[] rowKey) {
    assertEquals(Durability.SKIP_WAL, put.getDurability());
    assertEquals(HConstants.SYSTEMTABLE_QOS, put.getPriority());
    assertTrue(Bytes.compareTo(rowKey, put.getRow()) == 0);

    Map<Bytes, Bytes> valueMap = getValueMap(put);

    if (keyData.getTheKey() != null) {
      assertNotNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNotNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertEquals(new Bytes(Bytes.toBytes(latestSystemKey.getKeyChecksum())),
        valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));
    } else {
      assertNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));
    }
    assertEquals(new Bytes(keyData.getKeyMetadata().getBytes()),
      valueMap.get(new Bytes(DEK_METADATA_QUAL_BYTES)));
    assertNotNull(valueMap.get(new Bytes(REFRESHED_TIMESTAMP_QUAL_BYTES)));
    assertEquals(new Bytes(new byte[] { keyData.getKeyState().getVal() }),
      valueMap.get(new Bytes(KEY_STATE_QUAL_BYTES)));
  }

  private static Map<Bytes, Bytes> getValueMap(Put put) {
    NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
    List<Cell> cells = familyCellMap.get(KEY_META_INFO_FAMILY);
    Map<Bytes, Bytes> valueMap = new HashMap<>();
    for (Cell cell : cells) {
      valueMap.put(
        new Bytes(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
        new Bytes(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }
    return valueMap;
  }
}
