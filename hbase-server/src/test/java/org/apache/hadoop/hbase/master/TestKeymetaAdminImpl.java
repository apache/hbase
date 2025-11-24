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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.keymeta.KeyManagementService;
import org.apache.hadoop.hbase.keymeta.KeymetaAdminImpl;
import org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaAdminImpl.TestWhenDisabled.class,
  TestKeymetaAdminImpl.TestAdminImpl.class, TestKeymetaAdminImpl.TestForKeyProviderNullReturn.class,
  TestKeymetaAdminImpl.TestMiscAPIs.class,
  TestKeymetaAdminImpl.TestNewKeyManagementAdminMethods.class })
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaAdminImpl {

  private static final String CUST = "cust1";
  private static final byte[] CUST_BYTES = CUST.getBytes();

  protected final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

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
    conf.set(HConstants.CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
      MockManagedKeyProvider.class.getName());

    when(mockServer.getKeyManagementService()).thenReturn(mockServer);
    when(mockServer.getFileSystem()).thenReturn(mockFileSystem);
    when(mockServer.getConfiguration()).thenReturn(conf);
    keymetaAdmin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);
  }

  @After
  public void tearDown() throws Exception {
    // Clear the provider cache to avoid test interference
    Encryption.clearKeyProviderCache();
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
      assertThrows(IOException.class, () -> keymetaAdmin
        .enableKeyManagement(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES, KEY_SPACE_GLOBAL));
      assertThrows(IOException.class, () -> keymetaAdmin
        .getManagedKeys(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES, KEY_SPACE_GLOBAL));
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

    @Parameters(name = "{index},keySpace={0},keyState={1}")
    public static Collection<Object[]> data() {
      return Arrays
        .asList(new Object[][] { { KEY_SPACE_GLOBAL, ACTIVE, false }, { "ns1", ACTIVE, false },
          { KEY_SPACE_GLOBAL, FAILED, true }, { KEY_SPACE_GLOBAL, DISABLED, true }, });
    }

    @Test
    public void testEnableAndGet() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
      managedKeyProvider.setMockedKeyState(CUST, keyState);
      when(keymetaAccessor.getKeyManagementStateMarker(CUST.getBytes(), keySpace))
        .thenReturn(managedKeyProvider.getManagedKey(CUST.getBytes(), keySpace));

      ManagedKeyData managedKey = keymetaAdmin.enableKeyManagement(CUST_BYTES, keySpace);
      assertNotNull(managedKey);
      assertEquals(keyState, managedKey.getKeyState());
      verify(keymetaAccessor).getKeyManagementStateMarker(CUST.getBytes(), keySpace);

      keymetaAdmin.getManagedKeys(CUST_BYTES, keySpace);
      verify(keymetaAccessor).getAllKeys(CUST.getBytes(), keySpace, false);
    }

    @Test
    public void testEnableKeyManagement() throws Exception {
      assumeTrue(keyState == ACTIVE);
      ManagedKeyData managedKey = keymetaAdmin.enableKeyManagement(CUST_BYTES, "namespace1");
      assertEquals(ManagedKeyState.ACTIVE, managedKey.getKeyState());
      assertEquals(ManagedKeyProvider.encodeToStr(CUST_BYTES), managedKey.getKeyCustodianEncoded());
      assertEquals("namespace1", managedKey.getKeyNamespace());

      // Second call should return the same keys since our mock key provider returns the same key
      ManagedKeyData managedKey2 = keymetaAdmin.enableKeyManagement(CUST_BYTES, "namespace1");
      assertEquals(managedKey, managedKey2);
    }

    @Test
    public void testEnableKeyManagementWithMultipleNamespaces() throws Exception {
      ManagedKeyData managedKey = keymetaAdmin.enableKeyManagement(CUST_BYTES, "namespace1");
      assertEquals("namespace1", managedKey.getKeyNamespace());

      ManagedKeyData managedKey2 = keymetaAdmin.enableKeyManagement(CUST_BYTES, "namespace2");
      assertEquals("namespace2", managedKey2.getKeyNamespace());
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
      return Arrays.asList(new Object[][] { { KEY_SPACE_GLOBAL }, { "ns1" }, });
    }

    @Test
    public void test() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
      String cust = "invalidcust1";
      byte[] custBytes = cust.getBytes();
      managedKeyProvider.setMockedKey(cust, null, keySpace);
      IOException ex = assertThrows(IOException.class,
        () -> keymetaAdmin.enableKeyManagement(custBytes, keySpace));
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
    public List<ManagedKeyData> getAllKeys(byte[] key_cust, String keyNamespace,
      boolean includeMarkers) throws IOException, KeyException {
      return keymetaAccessor.getAllKeys(key_cust, keyNamespace, includeMarkers);
    }

    @Override
    public ManagedKeyData getKeyManagementStateMarker(byte[] key_cust, String keyNamespace)
      throws IOException, KeyException {
      return keymetaAccessor.getKeyManagementStateMarker(key_cust, keyNamespace);
    }
  }

  protected boolean assertKeyData(ManagedKeyData keyData, ManagedKeyState expKeyState,
    Key expectedKey) {
    assertNotNull(keyData);
    assertEquals(expKeyState, keyData.getKeyState());
    if (expectedKey == null) {
      assertNull(keyData.getTheKey());
    } else {
      byte[] keyBytes = keyData.getTheKey().getEncoded();
      byte[] expectedKeyBytes = expectedKey.getEncoded();
      assertEquals(expectedKeyBytes.length, keyBytes.length);
      assertEquals(new Bytes(expectedKeyBytes), keyBytes);
    }
    return true;
  }

  /**
   * Test class for rotateSTK API
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestMiscAPIs extends TestKeymetaAdminImpl {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMiscAPIs.class);

    private ServerManager mockServerManager = mock(ServerManager.class);
    private AsyncClusterConnection mockConnection;
    private AsyncAdmin mockAsyncAdmin;

    @Override
    public void setUp() throws Exception {
      super.setUp();
      mockConnection = mock(AsyncClusterConnection.class);
      mockAsyncAdmin = mock(AsyncAdmin.class);
      when(mockServer.getServerManager()).thenReturn(mockServerManager);
      when(mockServer.getAsyncClusterConnection()).thenReturn(mockConnection);
      when(mockConnection.getAdmin()).thenReturn(mockAsyncAdmin);
    }

    @Test
    public void testEnableWithInactiveKey() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getManagedKeyProvider(conf);
      managedKeyProvider.setMockedKeyState(CUST, INACTIVE);
      when(keymetaAccessor.getKeyManagementStateMarker(CUST.getBytes(), KEY_SPACE_GLOBAL))
        .thenReturn(managedKeyProvider.getManagedKey(CUST.getBytes(), KEY_SPACE_GLOBAL));

      IOException exception = assertThrows(IOException.class,
        () -> keymetaAdmin.enableKeyManagement(CUST_BYTES, KEY_SPACE_GLOBAL));
      assertTrue(exception.getMessage(),
        exception.getMessage().contains("Expected key to be ACTIVE, but got an INACTIVE key"));
    }

    /**
     * Helper method to test that a method throws IOException when not called on master.
     * @param adminAction             the action to test, taking a KeymetaAdminImpl instance
     * @param expectedMessageFragment the expected fragment in the error message
     */
    private void assertNotOnMasterThrowsException(Consumer<KeymetaAdminImpl> adminAction,
      String expectedMessageFragment) {
      // Create a non-master server mock
      Server mockRegionServer = mock(Server.class);
      KeyManagementService mockKeyService = mock(KeyManagementService.class);
      when(mockRegionServer.getKeyManagementService()).thenReturn(mockKeyService);
      when(mockKeyService.getConfiguration()).thenReturn(conf);
      when(mockRegionServer.getConfiguration()).thenReturn(conf);
      when(mockRegionServer.getFileSystem()).thenReturn(mockFileSystem);

      KeymetaAdminImpl admin = new KeymetaAdminImpl(mockRegionServer) {
        @Override
        protected AsyncAdmin getAsyncAdmin(MasterServices master) {
          throw new RuntimeException("Shouldn't be called since we are not on master");
        }
      };

      RuntimeException runtimeEx =
        assertThrows(RuntimeException.class, () -> adminAction.accept(admin));
      assertTrue(runtimeEx.getCause() instanceof IOException);
      IOException ex = (IOException) runtimeEx.getCause();
      assertTrue(ex.getMessage().contains(expectedMessageFragment));
    }

    /**
     * Helper method to test that a method throws IOException when key management is disabled.
     * @param adminAction the action to test, taking a KeymetaAdminImpl instance
     */
    private void assertDisabledThrowsException(Consumer<KeymetaAdminImpl> adminAction) {
      TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");

      KeymetaAdminImpl admin = new KeymetaAdminImpl(mockServer) {
        @Override
        protected AsyncAdmin getAsyncAdmin(MasterServices master) {
          throw new RuntimeException("Shouldn't be called since we are disabled");
        }
      };

      RuntimeException runtimeEx =
        assertThrows(RuntimeException.class, () -> adminAction.accept(admin));
      assertTrue(runtimeEx.getCause() instanceof IOException);
      IOException ex = (IOException) runtimeEx.getCause();
      assertTrue("Exception message should contain 'not enabled', but was: " + ex.getMessage(),
        ex.getMessage().contains("not enabled"));
    }

    /**
     * Test rotateSTK when a new key is detected. Now that we can mock SystemKeyManager via
     * master.getSystemKeyManager(), we can properly test the success scenario: 1.
     * SystemKeyManager.rotateSystemKeyIfChanged() returns non-null (new key detected) 2. Master
     * gets list of online region servers 3. Master makes parallel RPC calls to all region servers
     * 4. All region servers successfully rebuild their system key cache 5. Method returns true
     */
    @Test
    public void testRotateSTKWithNewKey() throws Exception {
      // Setup mocks for MasterServices
      // Mock SystemKeyManager to return a new key (non-null)
      when(mockServer.rotateSystemKeyIfChanged()).thenReturn(true);

      when(mockAsyncAdmin.refreshSystemKeyCacheOnServers(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call rotateSTK - should return true since new key was detected
      boolean result = admin.rotateSTK();

      // Verify the result
      assertTrue("rotateSTK should return true when new key is detected", result);

      // Verify that rotateSystemKeyIfChanged was called
      verify(mockServer).rotateSystemKeyIfChanged();
      verify(mockAsyncAdmin).refreshSystemKeyCacheOnServers(any());
    }

    /**
     * Test rotateSTK when no key change is detected. Now that we can mock SystemKeyManager, we can
     * properly test the no-change scenario: 1. SystemKeyManager.rotateSystemKeyIfChanged() returns
     * null 2. Method returns false immediately without calling any region servers 3. No RPC calls
     * are made to region servers
     */
    @Test
    public void testRotateSTKNoChange() throws Exception {
      // Mock SystemKeyManager to return null (no key change)
      when(mockServer.rotateSystemKeyIfChanged()).thenReturn(false);

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call rotateSTK - should return false since no key change was detected
      boolean result = admin.rotateSTK();

      // Verify the result
      assertFalse("rotateSTK should return false when no key change is detected", result);

      // Verify that rotateSystemKeyIfChanged was called
      verify(mockServer).rotateSystemKeyIfChanged();

      // Verify that getOnlineServersList was never called (short-circuit behavior)
      verify(mockServerManager, never()).getOnlineServersList();
    }

    @Test
    public void testRotateSTKOnIOException() throws Exception {
      when(mockServer.rotateSystemKeyIfChanged()).thenThrow(new IOException("test"));

      KeymetaAdminImpl admin = new KeymetaAdminImpl(mockServer);
      IOException ex = assertThrows(IOException.class, () -> admin.rotateSTK());
      assertTrue("Exception message should contain 'test', but was: " + ex.getMessage(),
        ex.getMessage().equals("test"));
    }

    /**
     * Test rotateSTK when region server refresh fails.
     */
    @Test
    public void testRotateSTKWithFailedServerRefresh() throws Exception {
      // Setup mocks for MasterServices
      // Mock SystemKeyManager to return a new key (non-null)
      when(mockServer.rotateSystemKeyIfChanged()).thenReturn(true);

      CompletableFuture<Void> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new IOException("refresh failed"));
      when(mockAsyncAdmin.refreshSystemKeyCacheOnServers(any())).thenReturn(failedFuture);

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call rotateSTK and expect IOException
      IOException ex = assertThrows(IOException.class, () -> admin.rotateSTK());

      assertTrue(ex.getMessage()
        .contains("Failed to initiate System Key cache refresh on one or more region servers"));

      // Verify that rotateSystemKeyIfChanged was called
      verify(mockServer).rotateSystemKeyIfChanged();
      verify(mockAsyncAdmin).refreshSystemKeyCacheOnServers(any());
    }

    @Test
    public void testRotateSTKNotOnMaster() throws Exception {
      assertNotOnMasterThrowsException(admin -> {
        try {
          admin.rotateSTK();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, "rotateSTK can only be called on master");
    }

    @Test
    public void testEjectManagedKeyDataCacheEntryNotOnMaster() throws Exception {
      byte[] keyCustodian = Bytes.toBytes("testCustodian");
      String keyNamespace = "testNamespace";
      String keyMetadata = "testMetadata";

      assertNotOnMasterThrowsException(admin -> {
        try {
          admin.ejectManagedKeyDataCacheEntry(keyCustodian, keyNamespace, keyMetadata);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, "ejectManagedKeyDataCacheEntry can only be called on master");
    }

    @Test
    public void testClearManagedKeyDataCacheNotOnMaster() throws Exception {
      assertNotOnMasterThrowsException(admin -> {
        try {
          admin.clearManagedKeyDataCache();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, "clearManagedKeyDataCache can only be called on master");
    }

    @Test
    public void testRotateSTKWhenDisabled() throws Exception {
      assertDisabledThrowsException(admin -> {
        try {
          admin.rotateSTK();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    @Test
    public void testEjectManagedKeyDataCacheEntryWhenDisabled() throws Exception {
      byte[] keyCustodian = Bytes.toBytes("testCustodian");
      String keyNamespace = "testNamespace";
      String keyMetadata = "testMetadata";

      assertDisabledThrowsException(admin -> {
        try {
          admin.ejectManagedKeyDataCacheEntry(keyCustodian, keyNamespace, keyMetadata);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    @Test
    public void testClearManagedKeyDataCacheWhenDisabled() throws Exception {
      assertDisabledThrowsException(admin -> {
        try {
          admin.clearManagedKeyDataCache();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    /**
     * Test ejectManagedKeyDataCacheEntry API - verify it calls the AsyncAdmin method with correct
     * parameters
     */
    @Test
    public void testEjectManagedKeyDataCacheEntry() throws Exception {
      byte[] keyCustodian = Bytes.toBytes("testCustodian");
      String keyNamespace = "testNamespace";
      String keyMetadata = "testMetadata";

      when(mockAsyncAdmin.ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call the method
      admin.ejectManagedKeyDataCacheEntry(keyCustodian, keyNamespace, keyMetadata);

      // Verify the AsyncAdmin method was called
      verify(mockAsyncAdmin).ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any());
    }

    /**
     * Test ejectManagedKeyDataCacheEntry when it fails
     */
    @Test
    public void testEjectManagedKeyDataCacheEntryWithFailure() throws Exception {
      byte[] keyCustodian = Bytes.toBytes("testCustodian");
      String keyNamespace = "testNamespace";
      String keyMetadata = "testMetadata";

      CompletableFuture<Void> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new IOException("eject failed"));
      when(mockAsyncAdmin.ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any()))
        .thenReturn(failedFuture);

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call the method and expect IOException
      IOException ex = assertThrows(IOException.class,
        () -> admin.ejectManagedKeyDataCacheEntry(keyCustodian, keyNamespace, keyMetadata));

      assertTrue(ex.getMessage().contains("eject failed"));
      verify(mockAsyncAdmin).ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any());
    }

    /**
     * Test clearManagedKeyDataCache API - verify it calls the AsyncAdmin method
     */
    @Test
    public void testClearManagedKeyDataCache() throws Exception {
      when(mockAsyncAdmin.clearManagedKeyDataCacheOnServers(any()))
        .thenReturn(CompletableFuture.completedFuture(null));

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call the method
      admin.clearManagedKeyDataCache();

      // Verify the AsyncAdmin method was called
      verify(mockAsyncAdmin).clearManagedKeyDataCacheOnServers(any());
    }

    /**
     * Test clearManagedKeyDataCache when it fails
     */
    @Test
    public void testClearManagedKeyDataCacheWithFailure() throws Exception {
      CompletableFuture<Void> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new IOException("clear failed"));
      when(mockAsyncAdmin.clearManagedKeyDataCacheOnServers(any())).thenReturn(failedFuture);

      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockServer, keymetaAccessor);

      // Call the method and expect IOException
      IOException ex = assertThrows(IOException.class, () -> admin.clearManagedKeyDataCache());

      assertTrue(ex.getMessage().contains("clear failed"));
      verify(mockAsyncAdmin).clearManagedKeyDataCacheOnServers(any());
    }
  }

  /**
   * Tests for new key management admin methods.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestNewKeyManagementAdminMethods {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNewKeyManagementAdminMethods.class);

    @Mock
    private MasterServices mockMasterServices;
    @Mock
    private AsyncAdmin mockAsyncAdmin;
    @Mock
    private AsyncClusterConnection mockAsyncClusterConnection;
    @Mock
    private ServerManager mockServerManager;
    @Mock
    private KeymetaTableAccessor mockAccessor;
    @Mock
    private ManagedKeyProvider mockProvider;
    @Mock
    private KeyManagementService mockKeyManagementService;

    @Before
    public void setUp() throws Exception {
      MockitoAnnotations.openMocks(this);
      when(mockMasterServices.getAsyncClusterConnection()).thenReturn(mockAsyncClusterConnection);
      when(mockAsyncClusterConnection.getAdmin()).thenReturn(mockAsyncAdmin);
      when(mockMasterServices.getServerManager()).thenReturn(mockServerManager);
      when(mockServerManager.getOnlineServersList()).thenReturn(new ArrayList<>());

      // Setup KeyManagementService mock
      Configuration conf = HBaseConfiguration.create();
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
      when(mockKeyManagementService.getConfiguration()).thenReturn(conf);
      when(mockMasterServices.getKeyManagementService()).thenReturn(mockKeyManagementService);
    }

    @Test
    public void testDisableKeyManagement() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData activeKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      ManagedKeyData disabledMarker =
        new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, ManagedKeyState.DISABLED);

      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(activeKey)
        .thenReturn(disabledMarker);

      ManagedKeyData result = admin.disableKeyManagement(CUST_BYTES, KEY_SPACE_GLOBAL);

      assertNotNull(result);
      assertEquals(ManagedKeyState.DISABLED, result.getKeyState());
      verify(mockAccessor, times(2)).getKeyManagementStateMarker(CUST_BYTES, KEY_SPACE_GLOBAL);
      verify(mockAccessor).updateActiveState(activeKey, ManagedKeyState.INACTIVE);

      // Repeat the call for idempotency check.
      clearInvocations(mockAccessor);
      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(disabledMarker);
      result = admin.disableKeyManagement(CUST_BYTES, KEY_SPACE_GLOBAL);
      assertNotNull(result);
      assertEquals(ManagedKeyState.DISABLED, result.getKeyState());
      verify(mockAccessor, times(2)).getKeyManagementStateMarker(CUST_BYTES, KEY_SPACE_GLOBAL);
      verify(mockAccessor, never()).updateActiveState(any(), any());
    }

    @Test
    public void testDisableManagedKey() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData disabledKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.DISABLED, "metadata1", 123L);
      byte[] keyMetadataHash = ManagedKeyData.constructMetadataHash("metadata1");
      when(mockAccessor.getKey(any(), any(), any())).thenReturn(disabledKey);

      CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
      when(mockAsyncAdmin.ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any()))
        .thenReturn(successFuture);

      IOException exception = assertThrows(IOException.class,
        () -> admin.disableManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL, keyMetadataHash));
      assertTrue(exception.getMessage(),
        exception.getMessage().contains("Key is already disabled"));
      verify(mockAccessor, never()).disableKey(any(ManagedKeyData.class));
    }

    @Test
    public void testDisableManagedKeyNotFound() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      byte[] keyMetadataHash = ManagedKeyData.constructMetadataHash("metadata1");
      // Return null to simulate key not found
      when(mockAccessor.getKey(any(), any(), any())).thenReturn(null);

      IOException exception = assertThrows(IOException.class,
        () -> admin.disableManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL, keyMetadataHash));
      assertTrue(exception.getMessage(),
        exception.getMessage()
          .contains("Key not found for (custodian: Y3VzdDE=, namespace: *) with metadata hash: "
            + ManagedKeyProvider.encodeToStr(keyMetadataHash)));
      verify(mockAccessor).getKey(CUST_BYTES, KEY_SPACE_GLOBAL, keyMetadataHash);
    }

    @Test
    public void testRotateManagedKeyNoActiveKey() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      // Return null to simulate no active key exists
      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(null);

      IOException exception =
        assertThrows(IOException.class, () -> admin.rotateManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL));
      assertTrue(exception.getMessage().contains("No active key found"));
      verify(mockAccessor).getKeyManagementStateMarker(CUST_BYTES, KEY_SPACE_GLOBAL);
    }

    @Test
    public void testRotateManagedKey() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData currentKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      ManagedKeyData newKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata2", 124L);

      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(currentKey);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.getManagedKey(any(), any())).thenReturn(newKey);

      ManagedKeyData result = admin.rotateManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL);

      assertNotNull(result);
      assertEquals(newKey, result);
      verify(mockAccessor).addKey(newKey);
      verify(mockAccessor).updateActiveState(currentKey, ManagedKeyState.INACTIVE);
    }

    @Test
    public void testRefreshManagedKeysWithNoStateChange() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      List<ManagedKeyData> keys = new ArrayList<>();
      ManagedKeyData key1 = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      keys.add(key1);

      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.unwrapKey(any(), any())).thenReturn(key1);

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      verify(mockAccessor).getAllKeys(CUST_BYTES, KEY_SPACE_GLOBAL, false);
      verify(mockAccessor, never()).updateActiveState(any(), any());
      verify(mockAccessor, never()).disableKey(any());
    }

    @Test
    public void testRotateManagedKeyIgnoresFailedKey() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData currentKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      ManagedKeyData newKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.FAILED, "metadata1", 124L);

      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(currentKey);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.getManagedKey(any(), any())).thenReturn(newKey);
      // Mock the AsyncAdmin for ejectManagedKeyDataCacheEntry
      when(mockAsyncAdmin.ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

      ManagedKeyData result = admin.rotateManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL);

      assertNull(result);
      // Verify that the active key was not marked as inactive
      verify(mockAccessor, never()).addKey(any());
      verify(mockAccessor, never()).updateActiveState(any(), any());
    }

    @Test
    public void testRotateManagedKeyNoRotation() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      // Current and new keys have the same metadata hash, so no rotation should occur
      ManagedKeyData currentKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);

      when(mockAccessor.getKeyManagementStateMarker(any(), any())).thenReturn(currentKey);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.getManagedKey(any(), any())).thenReturn(currentKey);

      ManagedKeyData result = admin.rotateManagedKey(CUST_BYTES, KEY_SPACE_GLOBAL);

      assertNull(result);
      verify(mockAccessor, never()).updateActiveState(any(), any());
      verify(mockAccessor, never()).addKey(any());
      verify(mockAsyncAdmin, never()).ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(),
        any());
    }

    @Test
    public void testRefreshManagedKeysWithStateChange() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      List<ManagedKeyData> keys = new ArrayList<>();
      ManagedKeyData key1 = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      keys.add(key1);

      // Refreshed key has a different state (INACTIVE)
      ManagedKeyData refreshedKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.INACTIVE, "metadata1", 123L);

      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.unwrapKey(any(), any())).thenReturn(refreshedKey);

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      verify(mockAccessor).getAllKeys(CUST_BYTES, KEY_SPACE_GLOBAL, false);
      verify(mockAccessor).updateActiveState(key1, ManagedKeyState.INACTIVE);
    }

    @Test
    public void testRefreshManagedKeysWithDisabledState() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      List<ManagedKeyData> keys = new ArrayList<>();
      ManagedKeyData key1 = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      keys.add(key1);

      // Refreshed key is DISABLED
      ManagedKeyData disabledKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.DISABLED, "metadata1", 123L);

      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.unwrapKey(any(), any())).thenReturn(disabledKey);
      // Mock the ejectManagedKeyDataCacheEntry to cover line 263
      when(mockAsyncAdmin.ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      verify(mockAccessor).getAllKeys(CUST_BYTES, KEY_SPACE_GLOBAL, false);
      verify(mockAccessor).disableKey(key1);
      // Verify cache ejection was called (line 263)
      verify(mockAsyncAdmin).ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(), any());
    }

    @Test
    public void testRefreshManagedKeysWithException() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      List<ManagedKeyData> keys = new ArrayList<>();
      ManagedKeyData key1 = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      ManagedKeyData key2 = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata2", 124L);
      keys.add(key1);
      keys.add(key2);

      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      // First key throws IOException, second key should still be refreshed
      when(mockProvider.unwrapKey(key1.getKeyMetadata(), null))
        .thenThrow(new IOException("Simulated error"));
      when(mockProvider.unwrapKey(key2.getKeyMetadata(), null)).thenReturn(key2);

      // Should not throw exception, should continue refreshing other keys
      IOException exception = assertThrows(IOException.class,
        () -> admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL));

      assertTrue(exception.getCause() instanceof IOException);
      assertTrue(exception.getCause().getMessage(),
        exception.getCause().getMessage().contains("Simulated error"));
      verify(mockAccessor).getAllKeys(CUST_BYTES, KEY_SPACE_GLOBAL, false);
      verify(mockAccessor, never()).updateActiveState(any(), any());
      verify(mockAccessor, never()).disableKey(any());
      verify(mockProvider).unwrapKey(key1.getKeyMetadata(), null);
      verify(mockProvider).unwrapKey(key2.getKeyMetadata(), null);
      verify(mockAsyncAdmin, never()).ejectManagedKeyDataCacheEntryOnServers(any(), any(), any(),
        any());
    }

    @Test
    public void testRefreshKeyWithMetadataValidationFailure() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData originalKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      // Refreshed key has different metadata (which should not happen and indicates a serious
      // error)
      ManagedKeyData refreshedKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata2", 124L);

      List<ManagedKeyData> keys = Arrays.asList(originalKey);
      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.unwrapKey(originalKey.getKeyMetadata(), null)).thenReturn(refreshedKey);

      // The metadata mismatch triggers a KeyException which gets wrapped in an IOException
      IOException exception = assertThrows(IOException.class,
        () -> admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL));
      assertTrue(exception.getCause() instanceof KeyException);
      assertTrue(exception.getCause().getMessage(),
        exception.getCause().getMessage().contains("Key metadata changed during refresh"));
      verify(mockProvider).unwrapKey(originalKey.getKeyMetadata(), null);
      // No state updates should happen due to the exception
      verify(mockAccessor, never()).updateActiveState(any(), any());
      verify(mockAccessor, never()).disableKey(any());
    }

    @Test
    public void testRefreshKeyWithFailedStateIgnored() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      ManagedKeyData originalKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 123L);
      // Refreshed key is in FAILED state (provider issue) - using byte[] metadata hash constructor
      ManagedKeyData failedKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.FAILED, "metadata1", 124L);

      List<ManagedKeyData> keys = Arrays.asList(originalKey);
      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockProvider.unwrapKey(originalKey.getKeyMetadata(), null)).thenReturn(failedKey);

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      // Should not update state when refreshed key is FAILED
      verify(mockAccessor, never()).updateActiveState(any(), any());
      verify(mockAccessor, never()).disableKey(any());
      verify(mockProvider).unwrapKey(originalKey.getKeyMetadata(), null);
    }

    @Test
    public void testRefreshKeyRecoveryFromPriorEnableFailure() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      // FAILED key with null metadata (lines 119-135 in KeyManagementUtils)
      ManagedKeyData failedKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, FAILED, 123L);

      // Provider returns a recovered key
      ManagedKeyData recoveredKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, null,
        ManagedKeyState.ACTIVE, "metadata1", 124L);

      List<ManagedKeyData> keys = Arrays.asList(failedKey);
      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockAccessor.getKeyManagementStateMarker(CUST_BYTES, KEY_SPACE_GLOBAL))
        .thenReturn(failedKey);
      when(mockProvider.getManagedKey(failedKey.getKeyCustodian(), failedKey.getKeyNamespace()))
        .thenReturn(recoveredKey);

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      // Should call getManagedKey for FAILED key with null metadata (line 125)
      verify(mockProvider).getManagedKey(failedKey.getKeyCustodian(), failedKey.getKeyNamespace());
      // Should add recovered key (line 130)
      verify(mockAccessor).addKey(recoveredKey);
    }

    @Test
    public void testRefreshKeyNoRecoveryFromPriorEnableFailure() throws Exception {
      KeymetaAdminImplForTest admin = new KeymetaAdminImplForTest(mockMasterServices, mockAccessor);

      // FAILED key with null metadata
      ManagedKeyData failedKey = new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, FAILED, 123L);

      // Provider returns another FAILED key (recovery didn't work)
      ManagedKeyData stillFailedKey =
        new ManagedKeyData(CUST_BYTES, KEY_SPACE_GLOBAL, ManagedKeyState.FAILED, 124L);

      List<ManagedKeyData> keys = Arrays.asList(failedKey);
      when(mockAccessor.getAllKeys(any(), any(), anyBoolean())).thenReturn(keys);
      when(mockAccessor.getKeyProvider()).thenReturn(mockProvider);
      when(mockAccessor.getKeyManagementStateMarker(CUST_BYTES, KEY_SPACE_GLOBAL))
        .thenReturn(failedKey);
      when(mockProvider.getManagedKey(failedKey.getKeyCustodian(), failedKey.getKeyNamespace()))
        .thenReturn(stillFailedKey);

      admin.refreshManagedKeys(CUST_BYTES, KEY_SPACE_GLOBAL);

      // Should call getManagedKey for FAILED key with null metadata
      verify(mockProvider).getManagedKey(failedKey.getKeyCustodian(), failedKey.getKeyNamespace());
      verify(mockAccessor, never()).addKey(any());
    }

    private class KeymetaAdminImplForTest extends KeymetaAdminImpl {
      private final KeymetaTableAccessor accessor;

      public KeymetaAdminImplForTest(MasterServices server, KeymetaTableAccessor accessor)
        throws IOException {
        super(server);
        this.accessor = accessor;
      }

      @Override
      protected AsyncAdmin getAsyncAdmin(MasterServices master) {
        return mockAsyncAdmin;
      }

      @Override
      public List<ManagedKeyData> getAllKeys(byte[] keyCust, String keyNamespace,
        boolean includeMarkers) throws IOException, KeyException {
        return accessor.getAllKeys(keyCust, keyNamespace, includeMarkers);
      }

      @Override
      public ManagedKeyData getKey(byte[] keyCust, String keyNamespace, byte[] keyMetadataHash)
        throws IOException, KeyException {
        return accessor.getKey(keyCust, keyNamespace, keyMetadataHash);
      }

      @Override
      public void disableKey(ManagedKeyData keyData) throws IOException {
        accessor.disableKey(keyData);
      }

      @Override
      public ManagedKeyData getKeyManagementStateMarker(byte[] keyCust, String keyNamespace)
        throws IOException, KeyException {
        return accessor.getKeyManagementStateMarker(keyCust, keyNamespace);
      }

      @Override
      public void addKeyManagementStateMarker(byte[] keyCust, String keyNamespace,
        ManagedKeyState state) throws IOException {
        accessor.addKeyManagementStateMarker(keyCust, keyNamespace, state);
      }

      @Override
      public ManagedKeyProvider getKeyProvider() {
        return accessor.getKeyProvider();
      }

      @Override
      public void addKey(ManagedKeyData keyData) throws IOException {
        accessor.addKey(keyData);
      }

      @Override
      public void updateActiveState(ManagedKeyData keyData, ManagedKeyState newState)
        throws IOException {
        accessor.updateActiveState(keyData, newState);
      }
    }
  }
}
