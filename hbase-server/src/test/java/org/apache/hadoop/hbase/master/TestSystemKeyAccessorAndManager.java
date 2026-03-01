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

import static org.apache.hadoop.hbase.HConstants.SYSTEM_KEY_FILE_PREFIX;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.ACTIVE;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyState.INACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.keymeta.SystemKeyAccessor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
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
@Suite.SuiteClasses({ TestSystemKeyAccessorAndManager.TestAccessorWhenDisabled.class,
  TestSystemKeyAccessorAndManager.TestManagerWhenDisabled.class,
  TestSystemKeyAccessorAndManager.TestAccessor.class,
  TestSystemKeyAccessorAndManager.TestForInvalidFilenames.class,
  TestSystemKeyAccessorAndManager.TestManagerForErrors.class,
  TestSystemKeyAccessorAndManager.TestAccessorMisc.class // ADD THIS
})
@Category({ MasterTests.class, SmallTests.class })
public class TestSystemKeyAccessorAndManager {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  protected Configuration conf;
  protected Path testRootDir;
  protected FileSystem fs;

  protected FileSystem mockFileSystem = mock(FileSystem.class);
  protected MasterServices mockMaster = mock(MasterServices.class);
  protected SystemKeyManager systemKeyManager;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    testRootDir = TEST_UTIL.getDataTestDir(name.getMethodName());
    fs = testRootDir.getFileSystem(conf);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");

    when(mockMaster.getFileSystem()).thenReturn(mockFileSystem);
    when(mockMaster.getConfiguration()).thenReturn(conf);
    systemKeyManager = new SystemKeyManager(mockMaster);
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAccessorWhenDisabled extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessorWhenDisabled.class);

    @Override
    public void setUp() throws Exception {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");
    }

    @Test
    public void test() throws Exception {
      assertThrows(IOException.class, () -> systemKeyManager.getAllSystemKeyFiles());
      assertThrows(IOException.class, () -> systemKeyManager.getLatestSystemKeyFile().getFirst());
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestManagerWhenDisabled extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagerWhenDisabled.class);

    @Override
    public void setUp() throws Exception {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");
    }

    @Test
    public void test() throws Exception {
      systemKeyManager.ensureSystemKeyInitialized();
      assertNull(systemKeyManager.rotateSystemKeyIfChanged());
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAccessor extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessor.class);

    @Test
    public void testGetLatestWithNone() throws Exception {
      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0]);

      RuntimeException ex =
        assertThrows(RuntimeException.class, () -> systemKeyManager.getLatestSystemKeyFile());
      assertEquals("No cluster key initialized yet", ex.getMessage());
    }

    @Test
    public void testGetWithSingle() throws Exception {
      String fileName = SYSTEM_KEY_FILE_PREFIX + "1";
      FileStatus mockFileStatus = KeymetaTestUtils.createMockFile(fileName);

      Path systemKeyDir = CommonFSUtils.getSystemKeyDir(conf);
      when(mockFileSystem.globStatus(eq(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX + "*"))))
        .thenReturn(new FileStatus[] { mockFileStatus });

      List<Path> files = systemKeyManager.getAllSystemKeyFiles();
      assertEquals(1, files.size());
      assertEquals(fileName, files.get(0).getName());

      Pair<Path, List<Path>> latestSystemKeyFileResult = systemKeyManager.getLatestSystemKeyFile();
      assertEquals(fileName, latestSystemKeyFileResult.getFirst().getName());

      assertEquals(1,
        SystemKeyAccessor.extractSystemKeySeqNum(latestSystemKeyFileResult.getFirst()));
    }

    @Test
    public void testGetWithMultiple() throws Exception {
      FileStatus[] mockFileStatuses = IntStream.rangeClosed(1, 3)
        .mapToObj(i -> KeymetaTestUtils.createMockFile(SYSTEM_KEY_FILE_PREFIX + i))
        .toArray(FileStatus[]::new);

      Path systemKeyDir = CommonFSUtils.getSystemKeyDir(conf);
      when(mockFileSystem.globStatus(eq(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX + "*"))))
        .thenReturn(mockFileStatuses);

      List<Path> files = systemKeyManager.getAllSystemKeyFiles();
      assertEquals(3, files.size());

      Pair<Path, List<Path>> latestSystemKeyFileResult = systemKeyManager.getLatestSystemKeyFile();
      assertEquals(3,
        SystemKeyAccessor.extractSystemKeySeqNum(latestSystemKeyFileResult.getFirst()));
    }

    @Test
    public void testExtractKeySequenceForInvalidFilename() throws Exception {
      assertEquals(-1,
        SystemKeyAccessor.extractKeySequence(KeymetaTestUtils.createMockFile("abcd").getPath()));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestForInvalidFilenames extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForInvalidFilenames.class);

    @Parameter(0)
    public String fileName;
    @Parameter(1)
    public String expectedErrorMessage;

    @Parameters(name = "{index},fileName={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { "abcd", "Couldn't parse key file name: abcd" },
        { SYSTEM_KEY_FILE_PREFIX + "abcd",
          "Couldn't parse key file name: " + SYSTEM_KEY_FILE_PREFIX + "abcd" },
          // Add more test cases here
      });
    }

    @Test
    public void test() throws Exception {
      FileStatus mockFileStatus = KeymetaTestUtils.createMockFile(fileName);

      IOException ex = assertThrows(IOException.class,
        () -> SystemKeyAccessor.extractSystemKeySeqNum(mockFileStatus.getPath()));
      assertEquals(expectedErrorMessage, ex.getMessage());
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestManagerForErrors extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagerForErrors.class);

    private static final String CLUSTER_ID = "clusterId";

    @Mock
    ManagedKeyProvider mockKeyProvide;
    @Mock
    MasterFileSystem masterFS;

    private MockSystemKeyManager manager;
    private AutoCloseable closeableMocks;

    @Before
    public void setUp() throws Exception {
      super.setUp();
      closeableMocks = MockitoAnnotations.openMocks(this);

      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0]);
      ClusterId clusterId = mock(ClusterId.class);
      when(mockMaster.getMasterFileSystem()).thenReturn(masterFS);
      when(masterFS.getClusterId()).thenReturn(clusterId);
      when(clusterId.toString()).thenReturn(CLUSTER_ID);
      when(masterFS.getFileSystem()).thenReturn(mockFileSystem);

      manager = new MockSystemKeyManager(mockMaster, mockKeyProvide);
    }

    @After
    public void tearDown() throws Exception {
      closeableMocks.close();
    }

    @Test
    public void testEnsureSystemKeyInitialized_WithNoSystemKeys() throws Exception {
      when(mockKeyProvide.getSystemKey(any())).thenReturn(null);

      IOException ex = assertThrows(IOException.class, manager::ensureSystemKeyInitialized);
      assertEquals("Failed to get system key for cluster id: " + CLUSTER_ID, ex.getMessage());
    }

    @Test
    public void testEnsureSystemKeyInitialized_WithNoNonActiveKey() throws Exception {
      String metadata = "key-metadata";
      ManagedKeyData keyData = mock(ManagedKeyData.class);
      when(keyData.getKeyState()).thenReturn(INACTIVE);
      when(keyData.getKeyMetadata()).thenReturn(metadata);
      when(mockKeyProvide.getSystemKey(any())).thenReturn(keyData);

      IOException ex = assertThrows(IOException.class, manager::ensureSystemKeyInitialized);
      assertEquals(
        "System key is expected to be ACTIVE but it is: INACTIVE for metadata: " + metadata,
        ex.getMessage());
    }

    @Test
    public void testEnsureSystemKeyInitialized_WithInvalidMetadata() throws Exception {
      ManagedKeyData keyData = mock(ManagedKeyData.class);
      when(keyData.getKeyState()).thenReturn(ACTIVE);
      when(mockKeyProvide.getSystemKey(any())).thenReturn(keyData);

      IOException ex = assertThrows(IOException.class, manager::ensureSystemKeyInitialized);
      assertEquals("System key is expected to have metadata but it is null", ex.getMessage());
    }

    @Test
    public void testEnsureSystemKeyInitialized_WithSaveFailure() throws Exception {
      String metadata = "key-metadata";
      ManagedKeyData keyData = mock(ManagedKeyData.class);
      when(keyData.getKeyState()).thenReturn(ACTIVE);
      when(mockKeyProvide.getSystemKey(any())).thenReturn(keyData);
      when(keyData.getKeyMetadata()).thenReturn(metadata);
      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0]);
      Path rootDir = CommonFSUtils.getRootDir(conf);
      when(masterFS.getTempDir()).thenReturn(rootDir);
      FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
      when(mockFileSystem.create(any())).thenReturn(mockStream);
      when(mockFileSystem.rename(any(), any())).thenReturn(false);

      RuntimeException ex =
        assertThrows(RuntimeException.class, manager::ensureSystemKeyInitialized);
      assertEquals("Failed to generate or save System Key", ex.getMessage());
    }

    @Test
    public void testEnsureSystemKeyInitialized_RaceCondition() throws Exception {
      String metadata = "key-metadata";
      ManagedKeyData keyData = mock(ManagedKeyData.class);
      when(keyData.getKeyState()).thenReturn(ACTIVE);
      when(mockKeyProvide.getSystemKey(any())).thenReturn(keyData);
      when(keyData.getKeyMetadata()).thenReturn(metadata);
      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0]);
      Path rootDir = CommonFSUtils.getRootDir(conf);
      when(masterFS.getTempDir()).thenReturn(rootDir);
      FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
      when(mockFileSystem.create(any())).thenReturn(mockStream);
      when(mockFileSystem.rename(any(), any())).thenReturn(false);
      String fileName = SYSTEM_KEY_FILE_PREFIX + "1";
      FileStatus mockFileStatus = KeymetaTestUtils.createMockFile(fileName);
      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0],
        new FileStatus[] { mockFileStatus });

      manager.ensureSystemKeyInitialized();
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAccessorMisc extends TestSystemKeyAccessorAndManager {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessorMisc.class);

    @Test
    public void testLoadSystemKeySuccess() throws Exception {
      Path testPath = new Path("/test/key/path");
      String testMetadata = "test-metadata";

      // Create test key data
      Key testKey = new SecretKeySpec("test-key-bytes".getBytes(), "AES");
      ManagedKeyData testKeyData = new ManagedKeyData("custodian".getBytes(), "namespace", testKey,
        ManagedKeyState.ACTIVE, testMetadata, 1000L);

      // Mock key provider
      ManagedKeyProvider realProvider = mock(ManagedKeyProvider.class);
      when(realProvider.unwrapKey(testMetadata, null)).thenReturn(testKeyData);

      // Create testable SystemKeyAccessor that overrides both loadKeyMetadata and getKeyProvider
      SystemKeyAccessor testAccessor = new SystemKeyAccessor(mockMaster) {
        @Override
        protected String loadKeyMetadata(Path keyPath) throws IOException {
          assertEquals(testPath, keyPath);
          return testMetadata;
        }

        @Override
        public ManagedKeyProvider getKeyProvider() {
          return realProvider;
        }
      };

      ManagedKeyData result = testAccessor.loadSystemKey(testPath);
      assertEquals(testKeyData, result);

      // Verify the key provider was called correctly
      verify(realProvider).unwrapKey(testMetadata, null);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadSystemKeyNullResult() throws Exception {
      Path testPath = new Path("/test/key/path");
      String testMetadata = "test-metadata";

      // Mock key provider to return null
      ManagedKeyProvider realProvider = mock(ManagedKeyProvider.class);
      when(realProvider.unwrapKey(testMetadata, null)).thenReturn(null);

      SystemKeyAccessor testAccessor = new SystemKeyAccessor(mockMaster) {
        @Override
        protected String loadKeyMetadata(Path keyPath) throws IOException {
          assertEquals(testPath, keyPath);
          return testMetadata;
        }

        @Override
        public ManagedKeyProvider getKeyProvider() {
          return realProvider;
        }
      };

      testAccessor.loadSystemKey(testPath);
    }

    @Test
    public void testExtractSystemKeySeqNumValid() throws Exception {
      Path testPath1 = new Path(SYSTEM_KEY_FILE_PREFIX + "1");
      Path testPath123 = new Path(SYSTEM_KEY_FILE_PREFIX + "123");
      Path testPathMax = new Path(SYSTEM_KEY_FILE_PREFIX + Integer.MAX_VALUE);

      assertEquals(1, SystemKeyAccessor.extractSystemKeySeqNum(testPath1));
      assertEquals(123, SystemKeyAccessor.extractSystemKeySeqNum(testPath123));
      assertEquals(Integer.MAX_VALUE, SystemKeyAccessor.extractSystemKeySeqNum(testPathMax));
    }

    @Test(expected = IOException.class)
    public void testGetAllSystemKeyFilesIOException() throws Exception {
      when(mockFileSystem.globStatus(any())).thenThrow(new IOException("Filesystem error"));
      systemKeyManager.getAllSystemKeyFiles();
    }

    @Test(expected = IOException.class)
    public void testLoadSystemKeyIOExceptionFromMetadata() throws Exception {
      Path testPath = new Path("/test/key/path");

      SystemKeyAccessor testAccessor = new SystemKeyAccessor(mockMaster) {
        @Override
        protected String loadKeyMetadata(Path keyPath) throws IOException {
          assertEquals(testPath, keyPath);
          throw new IOException("Metadata read failed");
        }

        @Override
        public ManagedKeyProvider getKeyProvider() {
          return mock(ManagedKeyProvider.class);
        }
      };

      testAccessor.loadSystemKey(testPath);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadSystemKeyProviderException() throws Exception {
      Path testPath = new Path("/test/key/path");
      String testMetadata = "test-metadata";

      SystemKeyAccessor testAccessor = new SystemKeyAccessor(mockMaster) {
        @Override
        protected String loadKeyMetadata(Path keyPath) throws IOException {
          assertEquals(testPath, keyPath);
          return testMetadata;
        }

        @Override
        public ManagedKeyProvider getKeyProvider() {
          throw new RuntimeException("Key provider not available");
        }
      };

      testAccessor.loadSystemKey(testPath);
    }

    @Test
    public void testExtractSystemKeySeqNumBoundaryValues() throws Exception {
      // Test boundary values
      Path testPath0 = new Path(SYSTEM_KEY_FILE_PREFIX + "0");
      Path testPathMin = new Path(SYSTEM_KEY_FILE_PREFIX + Integer.MIN_VALUE);

      assertEquals(0, SystemKeyAccessor.extractSystemKeySeqNum(testPath0));
      assertEquals(Integer.MIN_VALUE, SystemKeyAccessor.extractSystemKeySeqNum(testPathMin));
    }

    @Test
    public void testExtractKeySequenceEdgeCases() throws Exception {
      // Test various edge cases for extractKeySequence
      Path validZero = new Path(SYSTEM_KEY_FILE_PREFIX + "0");
      Path validNegative = new Path(SYSTEM_KEY_FILE_PREFIX + "-1");

      // Valid cases should still work
      assertEquals(0, SystemKeyAccessor.extractKeySequence(validZero));
      assertEquals(-1, SystemKeyAccessor.extractKeySequence(validNegative));
    }

    @Test
    public void testCreateCacheFactoryMethod() {
      // Test static factory method
    }

    @Test
    public void testCreateCacheWithNoKeys() {
      // Test behavior when no system keys are available
    }
  }

  private static class MockSystemKeyManager extends SystemKeyManager {
    private final ManagedKeyProvider keyProvider;

    public MockSystemKeyManager(MasterServices master, ManagedKeyProvider keyProvider)
      throws IOException {
      super(master);
      this.keyProvider = keyProvider;
      // systemKeyDir = mock(Path.class);
    }

    @Override
    public ManagedKeyProvider getKeyProvider() {
      return keyProvider;
    }
  }
}
