/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.FailedCloseWALAfterInitializedErrorException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test WAL Init ERROR
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestWALOpenError {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALOpenError.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALOpenError.class);

  protected static Configuration conf;
  private static MiniDFSCluster cluster;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Path hbaseDir;
  protected static Path hbaseWALDir;

  protected FileSystem fs;
  protected Path dir;
  protected WALFactory wals;
  private ServerName currentServername;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    fs = cluster.getFileSystem();
    dir = new Path(hbaseDir, currentTest.getMethodName());
    this.currentServername = ServerName.valueOf(currentTest.getMethodName(), 16010, 1);
    wals = new WALFactory(conf, this.currentServername.toString());
  }

  @After
  public void tearDown() throws Exception {
    // testAppendClose closes the FileSystem, which will prevent us from closing cleanly here.
    try {
      wals.close();
    } catch (IOException exception) {
      LOG.warn("Encountered exception while closing wal factory. If you have other errors, this" +
        " may be the cause. Message: " + exception);
      LOG.debug("Exception details for failure to close wal factory.", exception);
    }
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(3);
    conf = TEST_UTIL.getConfiguration();
    conf.set(WALFactory.WAL_PROVIDER, MyFSWalProvider.class.getName());
    conf.set(WALFactory.META_WAL_PROVIDER, MyFSWalProvider.class.getName());
    cluster = TEST_UTIL.getDFSCluster();

    hbaseDir = TEST_UTIL.createRootDir();
    hbaseWALDir = TEST_UTIL.createWALRootDir();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  private static MyFSLog myFSLogCreated;
  private static boolean throwExceptionWhenCloseFSLogClose = false;

  @Test
  public void testWALClosedIfOpenError() throws IOException {

    throwExceptionWhenCloseFSLogClose = false;

    boolean hasFakeInitException = false;
    try {
      wals.getWAL(HRegionInfo.FIRST_META_REGIONINFO);
    } catch (IOException ex) {
      hasFakeInitException = ex.getMessage().contains("Fake init exception");
    }
    Assert.assertTrue(hasFakeInitException);
    Assert.assertTrue(myFSLogCreated.closed);

    FileStatus[] fileStatuses = CommonFSUtils.listStatus(fs, myFSLogCreated.walDir);
    Assert.assertTrue(fileStatuses == null || fileStatuses.length == 0);
  }

  @Test
  public void testThrowFailedCloseWalException() throws IOException {
    throwExceptionWhenCloseFSLogClose = true;
    boolean failedCloseWalException = false;
    try {
      wals.getWAL(HRegionInfo.FIRST_META_REGIONINFO);
    } catch (FailedCloseWALAfterInitializedErrorException ex) {
      failedCloseWalException = true;
    }
    Assert.assertTrue(failedCloseWalException);
  }


  public static class MyFSWalProvider extends FSHLogProvider {

    @Override
    protected MyFSLog createWAL() throws IOException {
      MyFSLog myFSLog = new MyFSLog(CommonFSUtils.getWALFileSystem(conf),
        CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.getFactoryId()),
        getWALArchiveDirectoryName(conf, factory.getFactoryId()), conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);

      myFSLogCreated = myFSLog;

      return myFSLog;
    }
  }

  public static class MyFSLog extends FSHLog {
    public MyFSLog(final FileSystem fs, final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix) throws IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    }

    @Override
    public void init() throws IOException {
      super.init();
      throw new IOException("Fake init exception");
    }

    @Override
    public void close() throws IOException {
      if (throwExceptionWhenCloseFSLogClose) {
        throw new IOException("Fake close exception");
      }
      super.close();
    }
  }
}