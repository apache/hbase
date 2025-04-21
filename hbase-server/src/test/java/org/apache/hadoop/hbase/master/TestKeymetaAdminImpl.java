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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestKeymetaAdminImpl.TestWhenDisabled.class,
  TestKeymetaAdminImpl.TestAdminImpl.class, TestKeymetaAdminImpl.TestForInvalid.class })
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaAdminImpl {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule public TestName name = new TestName();

  protected Configuration conf;
  protected Path testRootDir;
  protected FileSystem fs;

  protected FileSystem mockFileSystem = mock(FileSystem.class);
  protected Server mockServer = mock(Server.class);
  protected KeymetaAdminImpl keymetaAdmin;
  KeymetaTableAccessor mockAccessor = mock(KeymetaTableAccessor.class);

  @Before public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    testRootDir = TEST_UTIL.getDataTestDir(name.getMethodName());
    fs = testRootDir.getFileSystem(conf);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());

    when(mockServer.getFileSystem()).thenReturn(mockFileSystem);
    when(mockServer.getConfiguration()).thenReturn(conf);
    keymetaAdmin = new DummyKeymetaAdminImpl(mockServer, mockAccessor);
  }

  @RunWith(BlockJUnit4ClassRunner.class) @Category({ MasterTests.class, SmallTests.class })
  public static class TestWhenDisabled extends TestKeymetaAdminImpl {
    @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWhenDisabled.class);

    @Override public void setUp() throws IOException {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");
    }

    @Test public void testDisabled() throws Exception {
      assertThrows(IOException.class,
        () -> keymetaAdmin.enableKeyManagement(ManagedKeyData.KEY_GLOBAL_CUSTODIAN,
          ManagedKeyData.KEY_SPACE_GLOBAL));
      assertThrows(IOException.class,
        () -> keymetaAdmin.getManagedKeys(ManagedKeyData.KEY_GLOBAL_CUSTODIAN,
          ManagedKeyData.KEY_SPACE_GLOBAL));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAdminImpl extends TestKeymetaAdminImpl {
    @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAdminImpl.class);

    @Parameter(0)
    public ManagedKeyStatus keyStatus;
    @Parameter(1)
    public boolean isNullKey;

    @Parameters(name = "{index},keyStatus={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
        { ManagedKeyStatus.ACTIVE, false },
        { ManagedKeyStatus.FAILED, true },
        { ManagedKeyStatus.INACTIVE, false },
        { ManagedKeyStatus.DISABLED, true },
      });
    }

    @Test
    public void testEnable() throws Exception {
      MockManagedKeyProvider managedKeyProvider = (MockManagedKeyProvider)
        Encryption.getKeyProvider(conf);
      String cust = "cust1";
      managedKeyProvider.setMockedKeyStatus(cust, keyStatus);
      String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
      List<ManagedKeyData> managedKeyStatuses =
        keymetaAdmin.enableKeyManagement(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(managedKeyStatuses);
      assertEquals(1, managedKeyStatuses.size());
      assertEquals(keyStatus, managedKeyStatuses.get(0).getKeyStatus());
      verify(mockAccessor)
        .addKey(argThat((ManagedKeyData keyData) -> assertKeyData(keyData, keyStatus,
          isNullKey ? null : managedKeyProvider.getMockedKey(cust,
            ManagedKeyData.KEY_SPACE_GLOBAL))));

      keymetaAdmin.getManagedKeys(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
      verify(mockAccessor)
        .getAllKeys(argThat((arr) -> Bytes.compareTo(cust.getBytes(), arr) == 0),
          eq(ManagedKeyData.KEY_SPACE_GLOBAL));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestForInvalid extends TestKeymetaAdminImpl {
    @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForInvalid.class);

      @Test
      public void testForKeyProviderNullReturn() throws Exception {
        MockManagedKeyProvider managedKeyProvider = (MockManagedKeyProvider)
          Encryption.getKeyProvider(conf);
        String cust = "invalidcust1";
        String encodedCust = ManagedKeyProvider.encodeToStr(cust.getBytes());
        managedKeyProvider.setMockedKey(cust, null, ManagedKeyData.KEY_SPACE_GLOBAL);
        IOException ex = assertThrows(IOException.class,
          () -> keymetaAdmin.enableKeyManagement(encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL));
        assertEquals("Invalid null managed key received from key provider", ex.getMessage());
      }
    }


  private class DummyKeymetaAdminImpl extends KeymetaAdminImpl {
    public DummyKeymetaAdminImpl(Server mockServer, KeymetaTableAccessor mockAccessor) {
      super(mockServer);
    }

    @Override
    public void addKey(ManagedKeyData keyData) throws IOException {
      mockAccessor.addKey(keyData);
    }

    @Override
    public List<ManagedKeyData> getAllKeys(byte[] key_cust, String keyNamespace)
      throws IOException, KeyException {
      return mockAccessor.getAllKeys(key_cust, keyNamespace);
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
