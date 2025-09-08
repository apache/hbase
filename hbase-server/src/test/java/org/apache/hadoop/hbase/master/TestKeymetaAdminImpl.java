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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.DISABLED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.FAILED;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.keymeta.KeymetaAdminImpl;
import org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaAdminImpl.TestWhenDisabled.class,
  TestKeymetaAdminImpl.TestAdminImpl.class,
  TestKeymetaAdminImpl.TestForKeyProviderNullReturn.class,
})
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaAdminImpl {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final String CUST = "cust1";
  private static final String ENCODED_CUST = ManagedKeyProvider.encodeToStr(CUST.getBytes());


  @Rule
  public TestName name = new TestName();

  protected Configuration conf;
  protected Path testRootDir;
  protected FileSystem fs;

  protected FileSystem mockFileSystem = mock(FileSystem.class);
  protected MasterServices mockServer = mock(MasterServices.class);
  protected KeymetaAdminImplForTest keymetaAdmin;
  KeymetaTableAccessor keymetaAccessor = mock(KeymetaTableAccessor.class);

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    testRootDir = TEST_UTIL.getDataTestDir(name.getMethodName());
    fs = testRootDir.getFileSystem(conf);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());

    when(mockServer.getFileSystem()).thenReturn(mockFileSystem);
    when(mockServer.getConfiguration()).thenReturn(conf);
    keymetaAdmin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestWhenDisabled extends TestKeymetaAdminImpl {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWhenDisabled.class);

    @Override
    public void setUp() throws Exception {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");
    }

    @Test
    public void testDisabled() throws Exception {
      assertThrows(IOException.class,
        () -> keymetaAdmin.enableKeyManagement(ManagedKeyData.KEY_GLOBAL_CUSTODIAN,
          KEY_SPACE_GLOBAL));
      assertThrows(IOException.class,
        () -> keymetaAdmin.getManagedKeys(ManagedKeyData.KEY_GLOBAL_CUSTODIAN,
          KEY_SPACE_GLOBAL));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAdminImpl extends TestKeymetaAdminImpl {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAdminImpl.class);

    @Parameter(0)
    public String keySpace;
    @Parameter(1)
    public ManagedKeyState keyState;
    @Parameter(2)
    public boolean isNullKey;

    @Parameters(name = "{index},keySpace={1},keyState={2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
        new Object[][] {
          { KEY_SPACE_GLOBAL, ACTIVE, false },
          { "ns1", ACTIVE, false },
          { KEY_SPACE_GLOBAL, FAILED, true },
          { KEY_SPACE_GLOBAL, INACTIVE, false },
          { KEY_SPACE_GLOBAL, DISABLED, true },
        });
    }

    @Test
    public void testEnableAndGet() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
      managedKeyProvider.setMockedKeyState(CUST, keyState);
      when(keymetaAccessor.getActiveKey(CUST.getBytes(), keySpace)).thenReturn(
        managedKeyProvider.getManagedKey(CUST.getBytes(), keySpace));

      List<ManagedKeyData> managedKeys =
        keymetaAdmin.enableKeyManagement(ENCODED_CUST, keySpace);
      assertNotNull(managedKeys);
      assertEquals(1, managedKeys.size());
      assertEquals(keyState, managedKeys.get(0).getKeyState());
      verify(keymetaAccessor).getActiveKey(CUST.getBytes(), keySpace);

      keymetaAdmin.getManagedKeys(ENCODED_CUST, keySpace);
      verify(keymetaAccessor).getAllKeys(CUST.getBytes(), keySpace);
    }

    @Test
    public void testEnableKeyManagement() throws Exception {
      assumeTrue(keyState == ACTIVE);
      List<ManagedKeyData> keys = keymetaAdmin.enableKeyManagement(ENCODED_CUST, "namespace1");
      assertEquals(1, keys.size());
      assertEquals(ManagedKeyState.ACTIVE, keys.get(0).getKeyState());
      assertEquals(ENCODED_CUST, keys.get(0).getKeyCustodianEncoded());
      assertEquals("namespace1", keys.get(0).getKeyNamespace());

      // Second call should return the same keys since our mock key provider returns the same key
      List<ManagedKeyData> keys2 = keymetaAdmin.enableKeyManagement(ENCODED_CUST, "namespace1");
      assertEquals(1, keys2.size());
      assertEquals(keys.get(0), keys2.get(0));
    }

    @Test
    public void testEnableKeyManagementWithMultipleNamespaces() throws Exception {
      List<ManagedKeyData> keys = keymetaAdmin.enableKeyManagement(ENCODED_CUST, "namespace1");
      assertEquals(1, keys.size());
      assertEquals("namespace1", keys.get(0).getKeyNamespace());

      List<ManagedKeyData> keys2 = keymetaAdmin.enableKeyManagement(ENCODED_CUST, "namespace2");
      assertEquals(1, keys2.size());
      assertEquals("namespace2", keys2.get(0).getKeyNamespace());
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestForKeyProviderNullReturn extends TestKeymetaAdminImpl {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForKeyProviderNullReturn.class);

    @Parameter(0)
    public String keySpace;

    @Parameters(name = "{index},keySpace={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
        new Object[][] {
          { KEY_SPACE_GLOBAL },
          { "ns1" },
        });
    }

    @Test
    public void test() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
      String cust = "invalidcust1";
      String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
      managedKeyProvider.setMockedKey(cust, null, keySpace);
      IOException ex = assertThrows(IOException.class,
        () -> keymetaAdmin.enableKeyManagement(encodedCust, keySpace));
      assertEquals("Invalid null managed key received from key provider", ex.getMessage());
    }
  }

  private class KeymetaAdminImplForTest extends KeymetaAdminImpl {
    public KeymetaAdminImplForTest(MasterServices mockServer, KeymetaTableAccessor mockAccessor) {
      super(mockServer);
    }

    @Override
    public void addKey(ManagedKeyData keyData) throws IOException {
      keymetaAccessor.addKey(keyData);
    }

    @Override
    public List<ManagedKeyData> getAllKeys(byte[] key_cust, String keyNamespace)
      throws IOException, KeyException {
      return keymetaAccessor.getAllKeys(key_cust, keyNamespace);
    }

    @Override
    public ManagedKeyData getActiveKey(byte[] key_cust, String keyNamespace)
      throws IOException, KeyException {
      return keymetaAccessor.getActiveKey(key_cust, keyNamespace);
    }
  }

  protected boolean assertKeyData(ManagedKeyData keyData, ManagedKeyState expKeyState,
      Key expectedKey) {
    assertNotNull(keyData);
    assertEquals(expKeyState, keyData.getKeyState());
    if (expectedKey == null) {
      assertNull(keyData.getTheKey());
    }
    else {
      byte[] keyBytes = keyData.getTheKey().getEncoded();
      byte[] expectedKeyBytes = expectedKey.getEncoded();
      assertEquals(expectedKeyBytes.length, keyBytes.length);
      assertEquals(new Bytes(expectedKeyBytes), keyBytes);
    }
    return true;
  }
}
