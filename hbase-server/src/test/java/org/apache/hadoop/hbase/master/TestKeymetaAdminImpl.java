package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.keymeta.KeymetaAdminImpl;
import org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor;
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

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStatus.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaAdminImpl.TestWhenDisabled.class,
  TestKeymetaAdminImpl.TestAdminImpl.class,
  TestKeymetaAdminImpl.TestForKeyProviderNullReturn.class,
  TestKeymetaAdminImpl.TestMultiKeyGen.class,
  TestKeymetaAdminImpl.TestForInvalidKeyCountConfig.class,
})
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaAdminImpl {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  protected Configuration conf;
  protected Path testRootDir;
  protected FileSystem fs;

  protected FileSystem mockFileSystem = mock(FileSystem.class);
  protected Server mockServer = mock(Server.class);
  protected DummyKeymetaAdminImpl keymetaAdmin;
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
    keymetaAdmin = new DummyKeymetaAdminImpl(mockServer, keymetaAccessor);
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
    public int nKeys;
    @Parameter(1)
    public String keySpace;
    @Parameter(2)
    public ManagedKeyStatus keyStatus;
    @Parameter(3)
    public boolean isNullKey;

    @Parameters(name = "{index},nKeys={0},keySpace={1},keyStatus={2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
        new Object[][] {
          { 1, KEY_SPACE_GLOBAL, ACTIVE, false },
          { 1, "ns1", ACTIVE, false },
          { 1, KEY_SPACE_GLOBAL, FAILED, true },
          { 1, KEY_SPACE_GLOBAL, INACTIVE, false },
          { 1, KEY_SPACE_GLOBAL, DISABLED, true },
          { 2, KEY_SPACE_GLOBAL, ACTIVE, false },
        });
    }

    @Override
    public void setUp() throws Exception {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT,
        Integer.toString(nKeys));
    }

    @Test
    public void testEnableAndGet() throws Exception {
      MockManagedKeyProvider managedKeyProvider =
        (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
      String cust = "cust1";
      managedKeyProvider.setMockedKeyStatus(cust, keyStatus);
      String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
      List<ManagedKeyData> managedKeyStatuses =
        keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertNotNull(managedKeyStatuses);
      assertEquals(1, managedKeyStatuses.size());
      assertEquals(keyStatus, managedKeyStatuses.get(0).getKeyStatus());
      verify(keymetaAccessor).addKey(argThat(
        (ManagedKeyData keyData) -> assertKeyData(keyData, keyStatus,
          isNullKey ? null : managedKeyProvider.getMockedKey(cust,
            keySpace))));
      verify(keymetaAccessor).getAllKeys(cust.getBytes(), keySpace);
      reset(keymetaAccessor);

      keymetaAdmin.getManagedKeys(encodedCust, keySpace);
      verify(keymetaAccessor).getAllKeys(cust.getBytes(), keySpace);
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestMultiKeyGen extends TestKeymetaAdminImpl {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeymetaAdminImpl.TestMultiKeyGen.class);

    @Parameter(0)
    public String keySpace;

    private MockManagedKeyProvider managedKeyProvider;

    @Parameters(name = "{index},keySpace={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][] {
            { KEY_SPACE_GLOBAL },
            { "ns1" },
          });
    }

    @Override
    public void setUp() throws Exception {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT, "3");
      managedKeyProvider = (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
      managedKeyProvider.setMultikeyGenMode(true);
    }

    @Test
    public void testEnable() throws Exception {
      List<ManagedKeyData> managedKeyStatuses;
      String cust = "cust1";
      String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
      managedKeyStatuses = keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertKeys(managedKeyStatuses, 3);
      verify(keymetaAccessor).getAllKeys(cust.getBytes(), keySpace);
      verify(keymetaAccessor, times(3)).addKey(any());

      reset(keymetaAccessor);

      when(keymetaAccessor.getAllKeys(cust.getBytes(), keySpace)).thenReturn(managedKeyStatuses);
      managedKeyStatuses = keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertKeys(managedKeyStatuses, 3);
      verify(keymetaAccessor, times(0)).addKey(any());

      reset(keymetaAccessor);
      when(keymetaAccessor.getAllKeys(cust.getBytes(), keySpace)).thenReturn(managedKeyStatuses);
      keymetaAdmin.activeKeyCountOverride = 4;
      managedKeyStatuses = keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertKeys(managedKeyStatuses, 1);
      verify(keymetaAccessor, times(1)).addKey(any());

      reset(keymetaAccessor);
      when(keymetaAccessor.getAllKeys(cust.getBytes(), keySpace)).thenReturn(managedKeyStatuses);
      managedKeyProvider.setMultikeyGenMode(false);
      managedKeyStatuses = keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertKeys(managedKeyStatuses, 0);
      verify(keymetaAccessor, times(0)).addKey(any());

      //reset(keymetaAccessor);
      managedKeyProvider.setMockedKeyStatus(cust, FAILED);
      managedKeyStatuses = keymetaAdmin.enableKeyManagement(encodedCust, keySpace);
      assertNotNull(managedKeyStatuses);
      assertEquals(1, managedKeyStatuses.size());
      assertEquals(FAILED, managedKeyStatuses.get(0).getKeyStatus());
      verify(keymetaAccessor, times(1)).addKey(any());
      // NOTE: Reset as this instance is shared for more than 1 test.
      managedKeyProvider.setMockedKeyStatus(cust, ACTIVE);
    }

    private static void assertKeys(List<ManagedKeyData> managedKeyStatuses, int expectedCnt) {
      assertNotNull(managedKeyStatuses);
      assertEquals(expectedCnt, managedKeyStatuses.size());
      for (int i = 0; i < managedKeyStatuses.size(); ++i) {
        assertEquals(ACTIVE, managedKeyStatuses.get(i).getKeyStatus());
      }
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

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestForInvalidKeyCountConfig extends TestKeymetaAdminImpl {
    @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForInvalidKeyCountConfig.class);

    @Parameter(0)
    public String keyCount;;
    @Parameter(1)
    public String keySpace;
    @Parameter(2)
    public Class expectedExType;
    @Parameters(name = "{index},keyCount={0},keySpace={1}expectedExType={2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
        { "0", KEY_SPACE_GLOBAL, IOException.class },
        { "-1", KEY_SPACE_GLOBAL, IOException.class },
        { "abc", KEY_SPACE_GLOBAL, NumberFormatException.class },
        { "0", "ns1", IOException.class },
        { "-1", "ns1", IOException.class },
        { "abc", "ns1", NumberFormatException.class },
      });
    }

    @Test
    public void test() throws Exception {
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT, keyCount);
      String cust = "cust1";
      String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
      assertThrows(expectedExType, () ->
        keymetaAdmin.enableKeyManagement(encodedCust, keySpace));
    }
  }

  private class DummyKeymetaAdminImpl extends KeymetaAdminImpl {
    public DummyKeymetaAdminImpl(Server mockServer, KeymetaTableAccessor mockAccessor) {
      super(mockServer);
    }

    public Integer activeKeyCountOverride;

    @Override
    protected int getPerCustodianNamespaceActiveKeyConfCount() throws IOException {
      if (activeKeyCountOverride != null) {
        return activeKeyCountOverride;
      }
      return super.getPerCustodianNamespaceActiveKeyConfCount();
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
  }

  protected boolean assertKeyData(ManagedKeyData keyData, ManagedKeyStatus expKeyStatus,
      Key expectedKey) {
    assertNotNull(keyData);
    assertEquals(expKeyStatus, keyData.getKeyStatus());
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
