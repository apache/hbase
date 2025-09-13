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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleanerBarrier;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ MasterTests.class, MediumTests.class })
public class TestLogsCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogsCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestLogsCleaner.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final Path OLD_WALS_DIR =
    new Path(TEST_UTIL.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME);

  private static final Path OLD_PROCEDURE_WALS_DIR = new Path(OLD_WALS_DIR, "masterProcedureWALs");

  private static Configuration conf;

  private static DirScanPool POOL;

  private static String peerId = "1";

  private MasterServices masterServices;

  private ReplicationQueueStorage queueStorage;

  @Rule
  public final TableNameTestRule tableNameRule = new TableNameTestRule();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    POOL = DirScanPool.getLogCleanerScanPool(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    POOL.shutdownNow();
  }

  @Before
  public void beforeTest() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();

    fs.delete(OLD_WALS_DIR, true);

    // root directory
    fs.mkdirs(OLD_WALS_DIR);

    TableName tableName = tableNameRule.getTableName();
    TableDescriptor td = ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName);
    TEST_UTIL.getAdmin().createTable(td);
    TEST_UTIL.waitTableAvailable(tableName);
    queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(TEST_UTIL.getConnection(),
      conf, tableName);

    masterServices = mock(MasterServices.class);
    when(masterServices.getConnection()).thenReturn(TEST_UTIL.getConnection());
    when(masterServices.getReplicationLogCleanerBarrier())
      .thenReturn(new ReplicationLogCleanerBarrier());
    AsyncClusterConnection asyncClusterConnection = mock(AsyncClusterConnection.class);
    when(masterServices.getAsyncClusterConnection()).thenReturn(asyncClusterConnection);
    when(asyncClusterConnection.isClosed()).thenReturn(false);
    ReplicationPeerManager rpm = mock(ReplicationPeerManager.class);
    when(masterServices.getReplicationPeerManager()).thenReturn(rpm);
    when(rpm.getQueueStorage()).thenReturn(queueStorage);
    when(rpm.listPeers(null)).thenReturn(new ArrayList<>());
    ServerManager sm = mock(ServerManager.class);
    when(masterServices.getServerManager()).thenReturn(sm);
    when(sm.getOnlineServersList()).thenReturn(Collections.emptyList());
    @SuppressWarnings("unchecked")
    ProcedureExecutor<MasterProcedureEnv> procExec = mock(ProcedureExecutor.class);
    when(masterServices.getMasterProcedureExecutor()).thenReturn(procExec);
    when(procExec.getProcedures()).thenReturn(Collections.emptyList());
  }

  /**
   * This tests verifies LogCleaner works correctly with WALs and Procedure WALs located in the same
   * oldWALs directory.
   * <p/>
   * Created files:
   * <ul>
   * <li>2 invalid files</li>
   * <li>5 old Procedure WALs</li>
   * <li>30 old WALs from which 3 are in replication</li>
   * <li>5 recent Procedure WALs</li>
   * <li>1 recent WAL</li>
   * <li>1 very new WAL (timestamp in future)</li>
   * <li>masterProcedureWALs subdirectory</li>
   * </ul>
   * Files which should stay:
   * <ul>
   * <li>3 replication WALs</li>
   * <li>2 new WALs</li>
   * <li>5 latest Procedure WALs</li>
   * <li>masterProcedureWALs subdirectory</li>
   * </ul>
   */
  @Test
  public void testLogCleaning() throws Exception {
    // set TTLs
    long ttlWAL = 2000;
    long ttlProcedureWAL = 4000;
    conf.setLong("hbase.master.logcleaner.ttl", ttlWAL);
    conf.setLong("hbase.master.procedurewalcleaner.ttl", ttlProcedureWAL);

    HMaster.decorateMasterConfiguration(conf);
    Server server = new DummyServer();
    String fakeMachineName =
      URLEncoder.encode(server.getServerName().toString(), StandardCharsets.UTF_8.name());

    final FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(OLD_PROCEDURE_WALS_DIR);

    final long now = EnvironmentEdgeManager.currentTime();

    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(OLD_WALS_DIR, "a"));
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + "a"));

    // Case 2: 5 Procedure WALs that are old which would be deleted
    for (int i = 1; i <= 5; i++) {
      final Path fileName = new Path(OLD_PROCEDURE_WALS_DIR, String.format("pv2-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get old procedure WALs
    Thread.sleep(ttlProcedureWAL - ttlWAL);

    // Case 3: old WALs which would be deletable
    for (int i = 1; i <= 30; i++) {
      Path fileName = new Path(OLD_WALS_DIR, fakeMachineName + "." + (now - i));
      fs.createNewFile(fileName);
    }
    // Case 4: the newest 3 WALs will be kept because they are beyond the replication offset
    masterServices.getReplicationPeerManager().listPeers(null)
      .add(new ReplicationPeerDescription(peerId, true, null, null));
    queueStorage.setOffset(new ReplicationQueueId(server.getServerName(), peerId), fakeMachineName,
      new ReplicationGroupOffset(fakeMachineName + "." + (now - 3), 0), Collections.emptyMap());
    // Case 5: 5 Procedure WALs that are new, will stay
    for (int i = 6; i <= 10; i++) {
      Path fileName = new Path(OLD_PROCEDURE_WALS_DIR, String.format("pv2-%020d.log", i));
      fs.createNewFile(fileName);
    }

    // Sleep for sometime to get newer modification time
    Thread.sleep(ttlWAL);
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + now));

    // Case 6: 1 newer WAL, not even deletable for TimeToLiveLogCleaner,
    // so we are not going down the chain
    fs.createNewFile(new Path(OLD_WALS_DIR, fakeMachineName + "." + (now + ttlWAL)));

    FileStatus[] status = fs.listStatus(OLD_WALS_DIR);
    LOG.info("File status: {}", Arrays.toString(status));

    // There should be 34 files and 1 masterProcedureWALs directory
    assertEquals(35, fs.listStatus(OLD_WALS_DIR).length);
    // 10 procedure WALs
    assertEquals(10, fs.listStatus(OLD_PROCEDURE_WALS_DIR).length);

    LogCleaner cleaner = new LogCleaner(1000, server, conf, fs, OLD_WALS_DIR, POOL,
      ImmutableMap.of(HMaster.MASTER, masterServices));
    cleaner.chore();

    // In oldWALs we end up with the current WAL, a newer WAL, the 3 old WALs which
    // are scheduled for replication and masterProcedureWALs directory
    TEST_UTIL.waitFor(1000,
      (Waiter.Predicate<Exception>) () -> 6 == fs.listStatus(OLD_WALS_DIR).length);
    // In masterProcedureWALs we end up with 5 newer Procedure WALs
    TEST_UTIL.waitFor(1000,
      (Waiter.Predicate<Exception>) () -> 5 == fs.listStatus(OLD_PROCEDURE_WALS_DIR).length);

    if (LOG.isDebugEnabled()) {
      FileStatus[] statusOldWALs = fs.listStatus(OLD_WALS_DIR);
      FileStatus[] statusProcedureWALs = fs.listStatus(OLD_PROCEDURE_WALS_DIR);
      LOG.debug("Kept log file for oldWALs: {}", Arrays.toString(statusOldWALs));
      LOG.debug("Kept log file for masterProcedureWALs: {}", Arrays.toString(statusProcedureWALs));
    }
  }

  @Test
  public void testOnConfigurationChange() throws Exception {
    // Prepare environments
    Server server = new DummyServer();

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    LogCleaner cleaner = new LogCleaner(3000, server, conf, fs, OLD_WALS_DIR, POOL,
      ImmutableMap.of(HMaster.MASTER, masterServices));
    int size = cleaner.getSizeOfCleaners();
    assertEquals(LogCleaner.DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
      cleaner.getCleanerThreadTimeoutMsec());
    // Create dir and files for test
    int numOfFiles = 10;
    createFiles(fs, OLD_WALS_DIR, numOfFiles);
    FileStatus[] status = fs.listStatus(OLD_WALS_DIR);
    assertEquals(numOfFiles, status.length);
    // Start cleaner chore
    Thread thread = new Thread(() -> cleaner.chore());
    thread.setDaemon(true);
    thread.start();
    // change size of cleaners dynamically
    int sizeToChange = 4;
    long threadTimeoutToChange = 30 * 1000L;
    conf.setInt(LogCleaner.OLD_WALS_CLEANER_THREAD_SIZE, size + sizeToChange);
    conf.setLong(LogCleaner.OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC, threadTimeoutToChange);
    cleaner.onConfigurationChange(conf);
    assertEquals(sizeToChange + size, cleaner.getSizeOfCleaners());
    assertEquals(threadTimeoutToChange, cleaner.getCleanerThreadTimeoutMsec());
    // Stop chore
    thread.join();
    status = fs.listStatus(OLD_WALS_DIR);
    assertEquals(0, status.length);
  }

  private void createFiles(FileSystem fs, Path parentDir, int numOfFiles) throws IOException {
    for (int i = 0; i < numOfFiles; i++) {
      // size of each file is 1M, 2M, or 3M
      int xMega = 1 + ThreadLocalRandom.current().nextInt(1, 4);
      byte[] M = new byte[Math.toIntExact(FileUtils.ONE_MB * xMega)];
      Bytes.random(M);
      try (FSDataOutputStream fsdos = fs.create(new Path(parentDir, "file-" + i))) {
        fsdos.write(M);
      }
    }
  }

  private static final class DummyServer extends MockServer {

    @Override
    public Configuration getConfiguration() {
      return TEST_UTIL.getConfiguration();
    }

    @Override
    public ZKWatcher getZooKeeper() {
      try {
        return new ZKWatcher(getConfiguration(), "dummy server", this);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}
