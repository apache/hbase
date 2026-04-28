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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL_BYTES;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE_DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE_DISABLED;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_CHECKSUM_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_METADATA_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.DEK_WRAPPED_BY_STK_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.KEY_META_INFO_FAMILY;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.KEY_STATE_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.REFRESHED_TIMESTAMP_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.STK_CHECKSUM_QUAL_BYTES;
import static org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor.parseFromResult;
import static org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils.constructRowKeyForCustNamespace;
import static org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils.constructRowKeyForIdentity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
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
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaTableAccessor.TestAdd.class,
  TestKeymetaTableAccessor.TestAddWithNullableFields.class, TestKeymetaTableAccessor.TestGet.class,
  TestKeymetaTableAccessor.TestDisableKey.class,
  TestKeymetaTableAccessor.TestUpdateActiveState.class,
  TestKeymetaTableAccessor.TestCustodianNamespaceLength.class,
  TestKeymetaTableAccessor.TestRefreshTimestamp.class })
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaTableAccessor {
  protected static final String ALIAS = "custId1";
  protected static final byte[] CUST_ID = ALIAS.getBytes();
  protected static final Bytes CUST_ID_BYTES = new Bytes(CUST_ID);
  protected static final String KEY_NAMESPACE = "namespace";
  protected static final Bytes KEY_NAMESPACE_BYTES = new Bytes(KEY_NAMESPACE.getBytes());
  protected static String KEY_METADATA = "metadata1";
  protected static ManagedKeyIdentity KEY_IDENTITY_PREFIX =
    new KeyIdentityPrefixBytesBacked(CUST_ID_BYTES, KEY_NAMESPACE_BYTES);
  protected static ManagedKeyIdentity KEY_IDENTITY_FULL = ManagedKeyIdentityUtils
    .fullKeyIdentityFromMetadata(CUST_ID_BYTES, KEY_NAMESPACE_BYTES, KEY_METADATA);
  protected static ManagedKeyIdentity CUST_GLOBAL_ID =
    new KeyIdentityPrefixBytesBacked(CUST_ID_BYTES, KEY_SPACE_GLOBAL_BYTES);

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
  @Mock
  protected KeyManagementService keyManagementService;

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
    when(server.getKeyManagementService()).thenReturn(keyManagementService);
    when(keyManagementService.getConfiguration()).thenReturn(conf);
    when(keyManagementService.getSystemKeyCache()).thenReturn(systemKeyCache);

    accessor = new KeymetaTableAccessor(server);
    managedKeyProvider = new MockManagedKeyProvider();
    managedKeyProvider.initConfig(conf, "");

    latestSystemKey = managedKeyProvider.getSystemKey("system-id".getBytes());
    when(systemKeyCache.getLatestSystemKey()).thenReturn(latestSystemKey);
    when(systemKeyCache.getSystemKeyByIdentity(any(byte[].class))).thenReturn(latestSystemKey);
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

    @Captor
    private ArgumentCaptor<List<Put>> putCaptor;

    @Test
    public void testAddKey() throws Exception {
      managedKeyProvider.setMockedKeyState(ALIAS, keyState);
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(CUST_GLOBAL_ID);

      accessor.addKey(keyData);

      verify(table).put(putCaptor.capture());
      List<Put> puts = putCaptor.getValue();
      assertEquals(keyState == ACTIVE ? 2 : 1, puts.size());
      if (keyState == ACTIVE) {
        assertPut(keyData, puts.get(0), constructRowKeyForCustNamespace(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes()), ACTIVE);
        assertPut(keyData, puts.get(1), constructRowKeyForIdentity(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes(), keyData.getPartialIdentity()), ACTIVE);
      } else {
        assertPut(keyData, puts.get(0), constructRowKeyForIdentity(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes(), keyData.getPartialIdentity()), keyState);
      }
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAddWithNullableFields extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAddWithNullableFields.class);

    @Captor
    private ArgumentCaptor<List<Mutation>> batchCaptor;
    @Captor
    private ArgumentCaptor<List<Put>> putCaptor;

    @Test
    public void testAddKeyManagementStateMarker() throws Exception {
      managedKeyProvider.setMockedKeyState(ALIAS, FAILED);
      ManagedKeyData keyData = new ManagedKeyData(KEY_IDENTITY_PREFIX, FAILED);

      accessor.addKeyManagementStateMarker(keyData.getKeyIdentity(), keyData.getKeyState());

      verify(table).batch(batchCaptor.capture(), any());
      List<Mutation> mutations = batchCaptor.getValue();
      assertEquals(2, mutations.size());
      Mutation mutation1 = mutations.get(0);
      Mutation mutation2 = mutations.get(1);
      assertTrue(mutation1 instanceof Put);
      assertTrue(mutation2 instanceof Delete);
      Put put = (Put) mutation1;
      Delete delete = (Delete) mutation2;

      // Row key must match KEY_IDENTITY_PREFIX (custodian + namespace), not global key space
      byte[] expectedRowKey = constructRowKeyForCustNamespace(CUST_ID, KEY_NAMESPACE_BYTES.get());
      assertEquals(0, Bytes.compareTo(expectedRowKey, put.getRow()));

      Map<Bytes, Bytes> valueMap = getValueMap(put);

      // Verify key-related columns are not present
      assertNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));

      assertEquals(Durability.SKIP_WAL, put.getDurability());
      assertEquals(HConstants.SYSTEMTABLE_QOS, put.getPriority());

      // Verify state is set correctly
      assertEquals(new Bytes(new byte[] { FAILED.getVal() }),
        valueMap.get(new Bytes(KEY_STATE_QUAL_BYTES)));

      // Verify the delete operation properties
      assertEquals(Durability.SKIP_WAL, delete.getDurability());
      assertEquals(HConstants.SYSTEMTABLE_QOS, delete.getPriority());

      // Verify the row key is correct for a failure marker
      assertEquals(0, Bytes.compareTo(expectedRowKey, delete.getRow()));
      // Verify the key checksum, wrapped key, and STK checksum columns are deleted
      assertDeleteColumns(delete, true);
    }

    @Test
    public void testAddKeyWithoutMetadataSkipsMetadataColumn() throws Exception {
      ManagedKeyData keyData = new ManagedKeyData(KEY_IDENTITY_FULL, FAILED);

      accessor.addKey(keyData);

      verify(table).put(putCaptor.capture());
      List<Put> puts = putCaptor.getValue();
      assertEquals(1, puts.size());
      Map<Bytes, Bytes> valueMap = getValueMap(puts.get(0));
      assertNull(valueMap.get(new Bytes(DEK_METADATA_QUAL_BYTES)));
      assertEquals(new Bytes(new byte[] { FAILED.getVal() }),
        valueMap.get(new Bytes(KEY_STATE_QUAL_BYTES)));
      assertNotNull(valueMap.get(new Bytes(REFRESHED_TIMESTAMP_QUAL_BYTES)));
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

      when(result1.isEmpty()).thenReturn(false);
      when(result2.isEmpty()).thenReturn(false);
      when(result1.getRow())
        .thenReturn(KEY_IDENTITY_FULL.getFullIdentityView().copyBytesIfNecessary());
      when(result2.getRow())
        .thenReturn(KEY_IDENTITY_FULL.getFullIdentityView().copyBytesIfNecessary());
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
      assertNull(parseFromResult(server, KEY_IDENTITY_FULL, null));
      assertNull(parseFromResult(server, KEY_IDENTITY_FULL, Result.EMPTY_RESULT));
    }

    @Test
    public void testParseMarkerResultWithNonZeroPartialIdentityLength() throws Exception {
      byte[] row = KEY_IDENTITY_FULL.getFullIdentityView().copyBytesIfNecessary();
      Result result = Result.create(Arrays.asList(
        new KeyValue(row, KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES,
          new byte[] { FAILED.getVal() }),
        new KeyValue(row, KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES,
          Bytes.toBytes(0L))));

      IllegalArgumentException ex = null;
      try {
        parseFromResult(server, KEY_IDENTITY_FULL, result);
      } catch (IllegalArgumentException e) {
        ex = e;
      }
      assertNotNull(ex);
      assertTrue(ex.getMessage().contains("Partial identity length must be 0"));
      assertTrue(ex.getMessage().contains("got: " + KEY_IDENTITY_FULL.getPartialIdentityLength()));
    }

    @Test
    public void testGetActiveKeyMissingWrappedKey() throws Exception {
      when(table.get(any(Get.class))).thenReturn(result1);
      when(result1.getValue(eq(KEY_META_INFO_FAMILY), eq(KEY_STATE_QUAL_BYTES)))
        .thenReturn(new byte[] { ACTIVE.getVal() }, new byte[] { INACTIVE.getVal() });

      IOException ex;
      ex = assertThrows(IOException.class, () -> accessor.getKey(KEY_IDENTITY_FULL));
      assertEquals("ACTIVE key must have a wrapped key", ex.getMessage());
      ex = assertThrows(IOException.class, () -> accessor.getKey(KEY_IDENTITY_FULL));
      assertEquals("INACTIVE key must have a wrapped key", ex.getMessage());

    }

    @Test
    public void testGetKeyMissingSTK() throws Exception {
      when(result1.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_WRAPPED_BY_STK_QUAL_BYTES)))
        .thenReturn(new byte[] { 0 });
      when(systemKeyCache.getSystemKeyByIdentity(any(byte[].class))).thenReturn(null);
      when(table.get(any(Get.class))).thenReturn(result1);

      ManagedKeyData result = accessor.getKey(KEY_IDENTITY_FULL);

      assertNull(result);
    }

    @Test
    public void testGetKeyWithWrappedKey() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      ManagedKeyData result = accessor.getKey(KEY_IDENTITY_FULL);

      verify(table).get(any(Get.class));
      assertNotNull(result);
      assertEquals(0, Bytes.compareTo(CUST_ID, result.getKeyCustodian()));
      assertEquals(KEY_NAMESPACE, result.getKeyNamespace());
      assertEquals(keyData.getKeyMetadata(), result.getKeyMetadata());
      assertEquals(0,
        Bytes.compareTo(keyData.getTheKey().getEncoded(), result.getTheKey().getEncoded()));
      assertEquals(ACTIVE, result.getKeyState());

      // When DEK checksum doesn't match, we expect a null value.
      result = accessor.getKey(KEY_IDENTITY_FULL);
      assertNull(result);
    }

    @Test
    public void testGetKeyWithoutWrappedKey() throws Exception {
      when(table.get(any(Get.class))).thenReturn(result2);

      ManagedKeyData result = accessor.getKey(KEY_IDENTITY_FULL);

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

      when(scanner.iterator()).thenReturn(java.util.Arrays.asList(result1, result2).iterator());
      when(table.getScanner(any(Scan.class))).thenReturn(scanner);

      List<ManagedKeyData> allKeys = accessor.getAllKeys(KEY_IDENTITY_PREFIX, true);

      assertEquals(2, allKeys.size());
      assertEquals(keyData.getKeyMetadata(), allKeys.get(0).getKeyMetadata());
      assertEquals(keyMetadata2, allKeys.get(1).getKeyMetadata());
      verify(table).getScanner(any(Scan.class));
    }

    @Test
    public void testGetAllKeysExcludeMarkersWhenIncludeMarkersFalse() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);
      byte[] markerRow = KEY_IDENTITY_PREFIX.getIdentityPrefixView().copyBytesIfNecessary();
      Result markerResult = Result.create(Arrays.asList(
        new KeyValue(markerRow, KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES,
          new byte[] { FAILED.getVal() }),
        new KeyValue(markerRow, KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES,
          Bytes.toBytes(0L))));

      when(scanner.iterator())
        .thenReturn(java.util.Arrays.asList(result1, markerResult).iterator());
      when(table.getScanner(any(Scan.class))).thenReturn(scanner);

      List<ManagedKeyData> allKeys = accessor.getAllKeys(KEY_IDENTITY_PREFIX, false);

      assertEquals(1, allKeys.size());
      assertEquals(keyData.getKeyMetadata(), allKeys.get(0).getKeyMetadata());
      verify(table).getScanner(any(Scan.class));
    }

    @Test
    public void testGetAllKeysSkipsNullParsedResults() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);
      Result missingStkResult = Mockito.mock(Result.class);
      when(missingStkResult.isEmpty()).thenReturn(false);
      when(missingStkResult.getRow())
        .thenReturn(KEY_IDENTITY_FULL.getFullIdentityView().copyBytesIfNecessary());
      when(missingStkResult.getValue(eq(KEY_META_INFO_FAMILY), eq(KEY_STATE_QUAL_BYTES)))
        .thenReturn(new byte[] { ACTIVE.getVal() });
      when(missingStkResult.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_METADATA_QUAL_BYTES)))
        .thenReturn("metadata-missing-stk".getBytes());
      when(missingStkResult.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_WRAPPED_BY_STK_QUAL_BYTES)))
        .thenReturn(new byte[] { 1 });
      when(missingStkResult.getValue(eq(KEY_META_INFO_FAMILY), eq(STK_CHECKSUM_QUAL_BYTES)))
        .thenReturn(Bytes.toBytes(0L));
      when(missingStkResult.getValue(eq(KEY_META_INFO_FAMILY), eq(REFRESHED_TIMESTAMP_QUAL_BYTES)))
        .thenReturn(Bytes.toBytes(0L));
      when(systemKeyCache.getSystemKeyByIdentity(any(byte[].class))).thenReturn(latestSystemKey,
        (ManagedKeyData) null);

      when(scanner.iterator())
        .thenReturn(java.util.Arrays.asList(result1, missingStkResult).iterator());
      when(table.getScanner(any(Scan.class))).thenReturn(scanner);

      List<ManagedKeyData> allKeys = accessor.getAllKeys(KEY_IDENTITY_PREFIX, true);

      assertEquals(1, allKeys.size());
      assertEquals(keyData.getKeyMetadata(), allKeys.get(0).getKeyMetadata());
      verify(table).getScanner(any(Scan.class));
    }

    @Test
    public void testGetActiveKey() throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      when(scanner.iterator()).thenReturn(java.util.Arrays.asList(result1).iterator());
      when(table.get(any(Get.class))).thenReturn(result1);

      ManagedKeyData activeKey = accessor.getKeyManagementStateMarker(KEY_IDENTITY_PREFIX);

      assertNotNull(activeKey);
      assertEquals(keyData, activeKey);
      verify(table).get(any(Get.class));
    }

    @Test
    public void testGetKeyManagementStateMarkerWithFullIdentityCoversFalseBranch()
      throws Exception {
      ManagedKeyData keyData = setupActiveKey(CUST_ID, result1);

      when(table.get(any(Get.class))).thenReturn(result1);

      ManagedKeyData activeKey = accessor.getKeyManagementStateMarker(KEY_IDENTITY_FULL);

      assertNotNull(activeKey);
      assertEquals(keyData.getKeyMetadata(), activeKey.getKeyMetadata());
      assertEquals(ACTIVE, activeKey.getKeyState());
      verify(table).get(any(Get.class));
    }

    private ManagedKeyData setupActiveKey(byte[] custId, Result result) throws Exception {
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(KEY_IDENTITY_PREFIX);
      byte[] dekWrappedBySTK = SecurityUtil.wrapKey(conf, keyData.getTheKey(), latestSystemKey);
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_WRAPPED_BY_STK_QUAL_BYTES)))
        .thenReturn(dekWrappedBySTK);
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_CHECKSUM_QUAL_BYTES)))
        .thenReturn(Bytes.toBytes(keyData.getKeyChecksum()), Bytes.toBytes(0L));
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(STK_CHECKSUM_QUAL_BYTES))).thenReturn(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(latestSystemKey.getKeyCustodian(),
          latestSystemKey.getKeyNamespaceBytes(), latestSystemKey.getPartialIdentity()));
      // Update the mock to return the correct metadata from the keyData
      when(result.getValue(eq(KEY_META_INFO_FAMILY), eq(DEK_METADATA_QUAL_BYTES)))
        .thenReturn(keyData.getKeyMetadata().getBytes());
      when(table.get(any(Get.class))).thenReturn(result);
      return keyData;
    }
  }

  protected void assertPut(ManagedKeyData keyData, Put put, byte[] rowKey,
    ManagedKeyState targetState) {
    assertEquals(Durability.SKIP_WAL, put.getDurability());
    assertEquals(HConstants.SYSTEMTABLE_QOS, put.getPriority());
    assertTrue(Bytes.compareTo(rowKey, put.getRow()) == 0);

    Map<Bytes, Bytes> valueMap = getValueMap(put);

    if (keyData.getTheKey() != null) {
      assertNotNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNotNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertEquals(
        new Bytes(
          ManagedKeyIdentityUtils.constructRowKeyForIdentity(latestSystemKey.getKeyCustodian(),
            latestSystemKey.getKeyNamespaceBytes(), latestSystemKey.getPartialIdentity())),
        valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));
    } else {
      assertNull(valueMap.get(new Bytes(DEK_CHECKSUM_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(DEK_WRAPPED_BY_STK_QUAL_BYTES)));
      assertNull(valueMap.get(new Bytes(STK_CHECKSUM_QUAL_BYTES)));
    }
    assertEquals(new Bytes(keyData.getKeyMetadata().getBytes()),
      valueMap.get(new Bytes(DEK_METADATA_QUAL_BYTES)));
    assertNotNull(valueMap.get(new Bytes(REFRESHED_TIMESTAMP_QUAL_BYTES)));
    assertEquals(new Bytes(new byte[] { targetState.getVal() }),
      valueMap.get(new Bytes(KEY_STATE_QUAL_BYTES)));
  }

  // Verify the key checksum, wrapped key, and STK checksum columns are deleted
  private static void assertDeleteColumns(Delete delete, boolean includeMetadata) {
    Map<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
    assertTrue(familyCellMap.containsKey(KEY_META_INFO_FAMILY));

    List<Cell> cells = familyCellMap.get(KEY_META_INFO_FAMILY);
    assertEquals(includeMetadata ? 4 : 3, cells.size());

    // Verify each column is present in the delete
    Set<byte[]> qualifiers =
      cells.stream().map(CellUtil::cloneQualifier).collect(Collectors.toSet());

    assertTrue(qualifiers.stream().anyMatch(q -> Bytes.equals(q, DEK_CHECKSUM_QUAL_BYTES)));
    assertTrue(qualifiers.stream().anyMatch(q -> Bytes.equals(q, DEK_WRAPPED_BY_STK_QUAL_BYTES)));
    assertTrue(qualifiers.stream().anyMatch(q -> Bytes.equals(q, STK_CHECKSUM_QUAL_BYTES)));
    if (includeMetadata) {
      assertTrue(qualifiers.stream().anyMatch(q -> Bytes.equals(q, DEK_METADATA_QUAL_BYTES)));
    }
  }

  private static Map<Bytes, Bytes> getValueMap(Mutation mutation) {
    NavigableMap<byte[], List<Cell>> familyCellMap = mutation.getFamilyCellMap();
    List<Cell> cells = familyCellMap.get(KEY_META_INFO_FAMILY);
    Map<Bytes, Bytes> valueMap = new HashMap<>();
    for (Cell cell : cells) {
      valueMap.put(
        new Bytes(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
        new Bytes(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }
    return valueMap;
  }

  /**
   * Tests for disableKey() method.
   */
  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestDisableKey extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDisableKey.class);

    // Parameterize the key state
    @Parameter(0)
    public ManagedKeyState keyState;

    @Captor
    private ArgumentCaptor<List<Mutation>> mutationsCaptor;

    @Parameterized.Parameters(name = "{index},keyState={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { ACTIVE }, { INACTIVE }, { ACTIVE_DISABLED },
        { INACTIVE_DISABLED }, { FAILED }, });
    }

    @Test
    public void testDisableKey() throws Exception {
      ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(CUST_ID_BYTES, KEY_NAMESPACE_BYTES, "testMetadata");
      ManagedKeyData keyData =
        new ManagedKeyData(fullKeyIdentity, null, keyState, "testMetadata", 123L);

      accessor.disableKey(keyData);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      assertEquals(keyState == ACTIVE ? 3 : keyState == INACTIVE ? 2 : 1, mutations.size());
      int putIndex = 0;
      ManagedKeyState targetState = keyState == ACTIVE ? ACTIVE_DISABLED : INACTIVE_DISABLED;
      if (keyState == ACTIVE) {
        assertTrue(Bytes.compareTo(constructRowKeyForCustNamespace(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes()), mutations.get(0).getRow()) == 0);
        ++putIndex;
      }
      assertPut(keyData, (Put) mutations.get(putIndex),
        constructRowKeyForIdentity(keyData.getKeyCustodian(), keyData.getKeyNamespaceBytes(),
          keyData.getPartialIdentity()),
        targetState);
      if (keyState == INACTIVE) {
        assertTrue(Bytes.compareTo(constructRowKeyForIdentity(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes(), keyData.getPartialIdentity()),
          mutations.get(putIndex + 1).getRow()) == 0);
        // Verify the key checksum, wrapped key, and STK checksum columns are deleted
        assertDeleteColumns((Delete) mutations.get(putIndex + 1), false);
      }
    }

    @Test
    public void testDisableKeySkipCustNamespaceRowWhenActive() throws Exception {
      ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(CUST_ID_BYTES, KEY_NAMESPACE_BYTES, "testMetadata");
      ManagedKeyData keyData =
        new ManagedKeyData(fullKeyIdentity, null, ACTIVE, "testMetadata", 123L);

      accessor.disableKey(keyData, false);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      // When deleteCustNamespaceRow=false, ACTIVE key should not delete cust+namespace row.
      assertEquals(2, mutations.size());
      assertTrue(mutations.get(0) instanceof Put);
      assertTrue(mutations.get(1) instanceof Delete);
      assertTrue(Bytes
        .compareTo(constructRowKeyForIdentity(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes(), keyData.getPartialIdentity()), mutations.get(0).getRow())
          == 0);
      assertTrue(Bytes
        .compareTo(constructRowKeyForIdentity(keyData.getKeyCustodian(),
          keyData.getKeyNamespaceBytes(), keyData.getPartialIdentity()), mutations.get(1).getRow())
          == 0);
      assertDeleteColumns((Delete) mutations.get(1), false);
    }
  }

  /**
   * Tests for updateActiveState() method.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestUpdateActiveState extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUpdateActiveState.class);

    @Captor
    private ArgumentCaptor<List<Mutation>> mutationsCaptor;

    @Test
    public void testUpdateActiveStateFromInactiveToActive() throws Exception {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, INACTIVE, "metadata", 123L);
      ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(new Bytes(new byte[] { 1 }), KEY_NAMESPACE_BYTES, "syskey");
      ManagedKeyData systemKey = new ManagedKeyData(fullKeyIdentity, null, ACTIVE, "syskey", 100L);
      when(systemKeyCache.getLatestSystemKey()).thenReturn(systemKey);

      accessor.updateActiveState(keyData, ACTIVE, false);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      assertEquals(2, mutations.size());
    }

    @Test
    public void testUpdateActiveStateFromActiveToInactive() throws Exception {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, ACTIVE, "metadata", 123L);

      accessor.updateActiveState(keyData, INACTIVE, false);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      assertEquals(2, mutations.size());
    }

    @Test
    public void testUpdateActiveStateFromActiveToInactiveSkipDelete() throws Exception {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, ACTIVE, "metadata", 123L);

      accessor.updateActiveState(keyData, INACTIVE, true);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      assertEquals(1, mutations.size());
      assertTrue(mutations.get(0) instanceof Put);
    }

    @Test
    public void testUpdateActiveStateNoOp() throws Exception {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, ACTIVE, "metadata", 123L);

      accessor.updateActiveState(keyData, ACTIVE, false);

      verify(table, Mockito.never()).batch(any(), any());
    }

    @Test
    public void testUpdateActiveStateFromDisabledToActive() throws Exception {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, DISABLED, "metadata", 123L);
      ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(new Bytes(new byte[] { 1 }), KEY_NAMESPACE_BYTES, "syskey");
      ManagedKeyData systemKey = new ManagedKeyData(fullKeyIdentity, null, ACTIVE, "syskey", 100L);
      when(systemKeyCache.getLatestSystemKey()).thenReturn(systemKey);

      accessor.updateActiveState(keyData, ACTIVE, false);

      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      // Should have 2 mutations: add CustNamespace row and add all columns to Metadata row
      assertEquals(2, mutations.size());
    }

    @Test
    public void testUpdateActiveStateInvalidNewState() {
      ManagedKeyData keyData =
        new ManagedKeyData(KEY_IDENTITY_FULL, null, ACTIVE, "metadata", 123L);

      assertThrows(IllegalArgumentException.class,
        () -> accessor.updateActiveState(keyData, DISABLED, false));
    }
  }

  /**
   * Tests for custodian and namespace length limits (max 255 bytes / 255 chars) and row key format.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestCustodianNamespaceLength extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCustodianNamespaceLength.class);

    @Test
    public void testConstructRowKeyUsesSingleByteForCustodianLength() {
      byte[] keyCust = new byte[] { 1, 2, 3 };
      String keyNamespace = "ns";
      byte[] keyNamespaceBytes = Bytes.toBytes(keyNamespace);
      byte[] rowKey = constructRowKeyForCustNamespace(keyCust, keyNamespaceBytes);
      // Format: 1 byte custLen + custodian + 1 byte nsLen + namespace UTF-8 bytes
      int nsLen = keyNamespaceBytes.length;
      assertEquals(1 + keyCust.length + 1 + nsLen, rowKey.length);
      assertEquals(keyCust.length, rowKey[0] & 0xFF);
      assertTrue("Row key should contain custodian at offset 1",
        Bytes.equals(keyCust, Bytes.copy(rowKey, 1, keyCust.length)));
      int offsetAfterCust = 1 + keyCust.length;
      int storedNsLen = rowKey[offsetAfterCust] & 0xFF;
      assertEquals(keyNamespace, Bytes.toString(rowKey, offsetAfterCust + 1, storedNsLen));
    }

    @Test
    public void testMaxLengthCustodianAndNamespaceAllowed() throws Exception {
      byte[] maxCust = new byte[ManagedKeyData.MAX_UNSIGNED_BYTE];
      for (int i = 0; i < maxCust.length; i++) {
        maxCust[i] = (byte) ('a' + (i % 26));
      }
      StringBuilder nsSb = new StringBuilder(ManagedKeyData.MAX_UNSIGNED_BYTE);
      for (int i = 0; i < ManagedKeyData.MAX_UNSIGNED_BYTE; i++) {
        nsSb.append('n');
      }
      String maxNamespace = nsSb.toString();
      byte[] maxNamespaceBytes = Bytes.toBytes(maxNamespace);

      byte[] rowKey = constructRowKeyForCustNamespace(maxCust, maxNamespaceBytes);
      assertNotNull(rowKey);
      assertEquals(1 + maxCust.length + 1 + maxNamespaceBytes.length, rowKey.length);

      ManagedKeyIdentity maxCustIdentity = ManagedKeyIdentityUtils
        .fullKeyIdentityFromMetadata(new Bytes(maxCust), new Bytes(maxNamespaceBytes), "meta");
      ManagedKeyData keyData = new ManagedKeyData(maxCustIdentity, null, INACTIVE, "meta", 0L);
      accessor.addKey(keyData);
    }

    @Test
    public void testEmptyCustodianRejected() {
      byte[] emptyCust = new byte[0];
      byte[] keyNamespaceBytes = Bytes.toBytes("ns");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> constructRowKeyForCustNamespace(emptyCust, keyNamespaceBytes));
      assertTrue(ex.getMessage(), ex.getMessage().contains("custodian length must be 1-255"));
    }

    @Test
    public void testEmptyNamespaceRejected() {
      byte[] keyCust = new byte[] { 1 };
      byte[] emptyNamespaceBytes = Bytes.toBytes("");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> constructRowKeyForCustNamespace(keyCust, emptyNamespaceBytes));
      assertTrue(ex.getMessage(), ex.getMessage().contains("namespace length must be 1-255"));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestRefreshTimestamp extends TestKeymetaTableAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRefreshTimestamp.class);

    @Captor
    private ArgumentCaptor<List<Mutation>> mutationsCaptor;

    @Test
    public void testUpdateRefreshTimestamp_ActiveState() throws Exception {
      doTestUpdateRefreshTimestamp(ACTIVE);
    }

    @Test
    public void testUpdateRefreshTimestamp_InactiveState() throws Exception {
      doTestUpdateRefreshTimestamp(INACTIVE);
    }

    public void doTestUpdateRefreshTimestamp(ManagedKeyState state) throws Exception {
      ManagedKeyData keyData = new ManagedKeyData(KEY_IDENTITY_FULL, null, state, "metadata", 123L);

      // Inject timestamp via environment edge manager and verify that it is used in the mutation.
      long newTimestamp = System.currentTimeMillis();
      ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
      EnvironmentEdgeManager.injectEdge(edge);
      edge.setValue(newTimestamp);
      try {
        accessor.updateRefreshTimestamp(keyData);
      } finally {
        EnvironmentEdgeManager.reset();
      }

      // Verify that there are 2 mutations in the batch and both contain only refreshed timestamp
      // column.
      verify(table).batch(mutationsCaptor.capture(), any());
      List<Mutation> mutations = mutationsCaptor.getValue();
      assertEquals(state == ACTIVE ? 2 : 1, mutations.size());
      Map<Bytes, Bytes> valueMap0 = getValueMap(mutations.get(0));
      assertTrue(Bytes.compareTo(Bytes.toBytes(newTimestamp),
        valueMap0.get(new Bytes(REFRESHED_TIMESTAMP_QUAL_BYTES)).copyBytes()) == 0);
      if (state == ACTIVE) {
        Map<Bytes, Bytes> valueMap1 = getValueMap(mutations.get(1));
        assertTrue(Bytes.compareTo(Bytes.toBytes(newTimestamp),
          valueMap1.get(new Bytes(REFRESHED_TIMESTAMP_QUAL_BYTES)).copyBytes()) == 0);
      }
    }
  }
}
