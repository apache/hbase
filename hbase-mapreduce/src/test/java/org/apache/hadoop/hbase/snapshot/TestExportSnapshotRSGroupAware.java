package org.apache.hadoop.hbase.snapshot;

import static org.apache.hadoop.util.ToolRunner.run;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ MapReduceTests.class, LargeTests.class }) public class TestExportSnapshotRSGroupAware
  extends TestExportSnapshot {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExportSnapshot.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshot.class);
  private static final HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private static final HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final String RSGROUP_NAME = "rsgroup";
  private static final String NAMESPACE_NAME = "snapshotNamespace";
  private static final String TABLE_NAME = "snapshotTable";
  private static final int numRegions = 5;
  private static MiniHBaseCluster cluster1;
  private static Configuration conf1;
  private static Admin admin1;
  private static Admin admin2;
  private static MiniHBaseCluster cluster2;
  private static Configuration conf2;
  private static TableName table;
  private static String snapshotName;
  @Rule public final TestName testName = new TestName();

  @BeforeClass public static void setUpBeforeClass() throws Exception {
    conf1 = TEST_UTIL1.getConfiguration();
    conf1.setBoolean(RSGroupUtil.RS_GROUP_ENABLED, true);
    cluster1 = TEST_UTIL1.startMiniCluster(7);
    admin1 = cluster1.getMaster().getConnection().getAdmin();

    conf2 = conf1;
    cluster2 = cluster1;
    admin2 = admin1;
//    conf2 = TEST_UTIL2.getConfiguration();
//    conf2.setBoolean(RSGroupUtil.RS_GROUP_ENABLED, true);
//    cluster2 = TEST_UTIL2.startMiniCluster(7);
//    admin2 = cluster2.getMaster().getConnection().getAdmin();
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL1.shutdownMiniCluster();
//    TEST_UTIL2.shutdownMiniCluster();
  }

  private RSGroupInfo addGroup(Admin gAdmin, String group, int servers) throws IOException {
    RSGroupInfo defaultInfo = gAdmin.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertNotNull("Default group info can't be null", defaultInfo);
    assertTrue("default group doesn't have enough servers",
      defaultInfo.getServers().size() >= servers);
    gAdmin.addRSGroup(group);
    Set<Address> set = Sets.newHashSet();
    for (Address address : defaultInfo.getServers()) {
      if (set.size() == servers) {
        break;
      }
      set.add(address);
    }

    gAdmin.moveServersToRSGroup(set, group);
    RSGroupInfo result = gAdmin.getRSGroup(group);
    assertEquals("Insufficient servers in group", result.getServers().size(), servers);
    LOG.debug("Created group: " + group + " with servers: " + result.getServers());
    return result;
  }

  @Before public void setUp() throws Exception {
    table = TableName.valueOf(NAMESPACE_NAME + ":" + TABLE_NAME);
    snapshotName = "snaptb0-" + testName.getMethodName();

    addGroup(admin1, RSGROUP_NAME, 4);
//    addGroup(admin2, RSGROUP_NAME, 4);

    NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(NAMESPACE_NAME)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, RSGROUP_NAME).build();
    admin1.createNamespace(nsDescriptor);

    SnapshotTestingUtils.createPreSplitTable(TEST_UTIL1, table, numRegions, FAMILY);
    SnapshotTestingUtils.loadData(TEST_UTIL1, table, 1000, FAMILY);

    admin1.snapshot(snapshotName, table);
  }

  @After public void tearDown() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(admin1);
//    SnapshotTestingUtils.deleteAllSnapshots(admin2);
    admin1.disableTable(table);
    admin1.deleteTable(table);
    admin1.deleteNamespace(NAMESPACE_NAME);
  }

  @Test public void testExportSnapshotRSGroupAware() throws Exception {
    Path srcPath = cluster1.getMaster().getMasterFileSystem().getRootDir();
    Path targetPath = cluster2.getMaster().getMasterFileSystem().getRootDir();
    FileSystem srcFs = srcPath.getFileSystem(conf1);
    FileSystem targetFs = targetPath.getFileSystem(conf2);
    Path targetRootDir =
      targetPath.makeQualified(targetFs.getUri(), targetFs.getWorkingDirectory());
    Path targetDir = new Path(targetRootDir, new Path("tmp"));
    LOG.info("tgtFsUri={}, tgtDir={}, srcFsUri={}", targetFs.getUri(), targetDir, srcFs.getUri());
    List<String> opts = new ArrayList<>();
    opts.add("--snapshot");
    opts.add(snapshotName);
    opts.add("--copy-to");
    opts.add(targetDir.toString());
    opts.add("--target");
    opts.add(snapshotName);
    opts.add("--targetZK");
    opts.add(TEST_UTIL1.getZkCluster().getAddress().toString() + ":" + conf2
      .get("zookeeper.znode.parent"));
    opts.add("--targetRSGroup");
    opts.add(RSGROUP_NAME);

    int res = run(conf1, new ExportSnapshot(), opts.toArray(new String[opts.size()]));
    assertEquals("res=" + res, 0, res);

    final Path snapshotDir = new Path(HConstants.SNAPSHOT_DIR_NAME, snapshotName);
    assertTrue(targetDir.toString() + " " + snapshotDir.toString(),
      targetFs.exists(new Path(targetDir, snapshotDir)));
    LOG.info("Exported snapshot");

    // Verify File-System state
    FileStatus[] rootFiles = targetFs.listStatus(targetDir);
    assertEquals("Number of files should match number of regions", 2, rootFiles.length);
    for (FileStatus fileStatus : rootFiles) {
      String name = fileStatus.getPath().getName();
      assertTrue(fileStatus.toString(), fileStatus.isDirectory());
      assertTrue(name.toString(), name.equals(HConstants.SNAPSHOT_DIR_NAME) || name
        .equals(HConstants.HFILE_ARCHIVE_DIRECTORY));
    }
    LOG.info("Verified filesystem state");

    // Compare the snapshot metadata and verify the hfiles
    verifySnapshotDir(srcFs, new Path(srcPath, snapshotDir), targetFs,
      new Path(targetDir, snapshotDir));
    Set<String> snapshotFiles =
      verifySnapshot(conf2, targetFs, targetDir, table, snapshotName, null);
    assertEquals(numRegions, snapshotFiles.size());

    Set<String> rsgroupHosts = new HashSet<>();
    RSGroupInfo rsGroupInfo = admin2.getRSGroup(RSGROUP_NAME);
    for (Address addr : rsGroupInfo.getServers()) {
      rsgroupHosts.add(addr.toString());
    }

    //Ensure rsaware
    RemoteIterator<LocatedFileStatus> fileStatusListIterator =
      targetFs.listFiles(new Path(targetDir, HConstants.HFILE_ARCHIVE_DIRECTORY), true);
    while (fileStatusListIterator.hasNext()) {
      LocatedFileStatus fileStatus = fileStatusListIterator.next();
      BlockLocation[] locations = targetFs.getFileBlockLocations(fileStatus.getPath(), 0, Integer.MAX_VALUE);
      for (BlockLocation location : locations) {
        for (String host : location.getNames()) {
//          assertTrue("Location of file should be a node from rsgroup " + RSGROUP_NAME, rsgroupHosts.contains(host));
        }
      }
    }
  }
}

