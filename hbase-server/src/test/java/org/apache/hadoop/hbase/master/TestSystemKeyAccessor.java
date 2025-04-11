package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.keymeta.SystemKeyAccessor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import static org.apache.hadoop.hbase.HConstants.SYSTEM_KEY_FILE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestSystemKeyAccessor.TestWhenDisabled.class,
  TestSystemKeyAccessor.TestAccessor.class,
  TestSystemKeyAccessor.TestForInvalidFileNames.class })
@Category({ MasterTests.class, SmallTests.class })
public class TestSystemKeyAccessor {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  protected Configuration conf;
  protected Path testRootDir;
  protected FileSystem fs;

  protected FileSystem mockFileSystem = mock(FileSystem.class);
  protected Server mockServer = mock(Server.class);
  protected SystemKeyAccessor systemKeyAccessor;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    testRootDir = TEST_UTIL.getDataTestDir(name.getMethodName());
    fs = testRootDir.getFileSystem(conf);

    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");

    when(mockServer.getFileSystem()).thenReturn(mockFileSystem);
    when(mockServer.getConfiguration()).thenReturn(conf);
    systemKeyAccessor = new SystemKeyAccessor(mockServer);
  }

  private static FileStatus createMockFile(String fileName) {
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn(fileName);
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getPath()).thenReturn(mockPath);
    return mockFileStatus;
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestWhenDisabled extends TestSystemKeyAccessor {
    @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWhenDisabled.class);

    @Override public void setUp() throws IOException {
      super.setUp();
      conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "false");
    }

    @Test public void testDisabled() throws Exception {
      assertNull(systemKeyAccessor.getAllSystemKeyFiles());
      assertNull(systemKeyAccessor.getLatestSystemKeyFile());
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestAccessor extends TestSystemKeyAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessor.class);

    @Test
    public void testGetLatestWithNone() throws Exception {
      when(mockFileSystem.globStatus(any())).thenReturn(new FileStatus[0]);

      RuntimeException ex = assertThrows(RuntimeException.class,
        () -> systemKeyAccessor.getLatestSystemKeyFile());
      assertEquals("No cluster key initialized yet", ex.getMessage());
    }

    @Test
    public void testGetWithSingle() throws Exception {
      String fileName = SYSTEM_KEY_FILE_PREFIX + "1";
      FileStatus mockFileStatus = createMockFile(fileName);

      Path systemKeyDir = CommonFSUtils.getSystemKeyDir(conf);
      when(mockFileSystem.globStatus(eq(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX+"*"))))
        .thenReturn(new FileStatus[] { mockFileStatus });

      List<Path> files = systemKeyAccessor.getAllSystemKeyFiles();
      assertEquals(1, files.size());
      assertEquals(fileName, files.get(0).getName());

      Path latestSystemKeyFile = systemKeyAccessor.getLatestSystemKeyFile();
      assertEquals(fileName, latestSystemKeyFile.getName());

      assertEquals(1, systemKeyAccessor.extractSystemKeySeqNum(latestSystemKeyFile));
    }

    @Test
    public void testGetWithMultiple() throws Exception {
      FileStatus[] mockFileStatuses = IntStream.rangeClosed(1, 3)
        .mapToObj(i -> createMockFile(SYSTEM_KEY_FILE_PREFIX + i))
        .toArray(FileStatus[]::new);

      Path systemKeyDir = CommonFSUtils.getSystemKeyDir(conf);
      when(mockFileSystem.globStatus(eq(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX+"*"))))
        .thenReturn( mockFileStatuses );

      List<Path> files = systemKeyAccessor.getAllSystemKeyFiles();
      assertEquals(3, files.size());

      Path latestSystemKeyFile = systemKeyAccessor.getLatestSystemKeyFile();
      assertEquals(3, systemKeyAccessor.extractSystemKeySeqNum(latestSystemKeyFile));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestForInvalidFileNames extends TestSystemKeyAccessor {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForInvalidFileNames.class);

    @Parameter(0)
    public String fileName;
    @Parameter(1)
    public String expectedErrorMessage;

    @Parameters(name = "{index},fileName={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
        { "abcd", "Couldn't parse key file name: abcd" },
        {SYSTEM_KEY_FILE_PREFIX+"abcd", "Couldn't parse key file name: "+
          SYSTEM_KEY_FILE_PREFIX+"abcd"},
        // Add more test cases here
      });
    }

    @Test
    public void testForInvalid() throws Exception {
      FileStatus mockFileStatus = createMockFile(fileName);

      IOException ex = assertThrows(IOException.class,
          () -> systemKeyAccessor.extractSystemKeySeqNum(mockFileStatus.getPath()));
      assertEquals(expectedErrorMessage, ex.getMessage());
    }
  }
}
