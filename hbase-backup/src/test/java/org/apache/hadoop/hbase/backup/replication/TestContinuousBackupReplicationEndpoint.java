package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_BULKLOAD_ENABLE_KEY;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupManager.CONF_BACKUP_MAX_WAL_SIZE;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupManager.CONF_BACKUP_ROOT_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_PEER_UUID;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupStagingManager.CONF_STAGED_WAL_FLUSH_INITIAL_DELAY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupStagingManager.CONF_STAGED_WAL_FLUSH_INTERVAL;
import static org.apache.hadoop.hbase.master.cleaner.HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestContinuousBackupReplicationEndpoint {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestContinuousBackupReplicationEndpoint.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static final byte[] QUALIFIER = Bytes.toBytes("my-qualifier");
  static FileSystem fs = null;
  Path root;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set the configuration properties as required
    conf.set(MASTER_HFILE_CLEANER_PLUGINS,
      "org.apache.hadoop.hbase.backup.replication.ContinuousBackupStagedHFileCleaner");
    conf.setLong(CONF_BACKUP_MAX_WAL_SIZE, 10240);
    conf.setInt(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, 10);
    conf.setInt(CONF_STAGED_WAL_FLUSH_INTERVAL, 10);
    conf.setBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf.set(REPLICATION_CLUSTER_ID, "clusterId1");

    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(1);
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (fs != null) {
      fs.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    root = TEST_UTIL.getDataTestDirOnTestFS();
  }

  @After
  public void cleanup() throws IOException {
    fs.delete(root, true);
  }

  @Test
  public void testWALAndBulkLoadFileBackup() throws IOException {
    String tableName = "usertable";
    String cfName = "cf";

    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cfName)).setScope(1).build();
    TableDescriptor tableDescriptor = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(tableName))
      .setColumnFamily(columnFamilyDescriptor)
      .build();

    Admin admin = TEST_UTIL.getAdmin();
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      admin.createTable(tableDescriptor);
    }

    String peerId = "peerId";
    Map<TableName, List<String>> tableMap = new HashMap<>();
    tableMap.put(TableName.valueOf(tableName), new ArrayList<>());
    String continuousBackupReplicationEndpoint =
      "org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint";

    Path backupRootDir = new Path(root, "testWALBackup");
    fs.mkdirs(backupRootDir);

    Map<String, String> additionalArgs = new HashMap<>();
    additionalArgs.put(CONF_PEER_UUID, "0c5672e3-0f96-4f5d-83d2-cceccebdfd42");
    additionalArgs.put(CONF_BACKUP_ROOT_DIR, backupRootDir.toString());

    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(continuousBackupReplicationEndpoint)
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableMap)
      .putAllConfiguration(additionalArgs)
      .build();

    admin.addReplicationPeer(peerId, peerConfig);

    try (Table table = TEST_UTIL.getConnection().getTable(TableName.valueOf(tableName))) {
      int rowSize = 32;
      int totalRows = 100;
      TEST_UTIL.loadRandomRows(table, Bytes.toBytes(cfName), rowSize, totalRows);

      Path dir = TEST_UTIL.getDataTestDirOnTestFS("testBulkLoadByFamily");
      dir = dir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
      TEST_UTIL.getConfiguration().setBoolean(BulkLoadHFilesTool.BULK_LOAD_HFILES_BY_FAMILY, true);
      byte[] from = Bytes.toBytes(cfName + "begin");
      byte[] to = Bytes.toBytes(cfName + "end");

      Path familyDir = new Path(dir, cfName);
      HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), fs, new Path(familyDir, "MyHFile"),
        Bytes.toBytes(cfName), QUALIFIER, from, to, 1000);

      BulkLoadHFiles loader = new BulkLoadHFilesTool(TEST_UTIL.getConfiguration());
      loader.bulkLoad(table.getName(), dir);

      assertEquals(1100, HBaseTestingUtil.countRows(table));

      // Wait for 10 seconds here
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread was interrupted while waiting", e);
      }

      Path walDir = new Path(backupRootDir, "WALs/default/usertable");
      assertTrue("WAL directory does not exist!", fs.exists(walDir));
      FileStatus[] walFiles = fs.listStatus(walDir);
      assertNotNull("No WAL files found!", walFiles);
      assertTrue("Expected some WAL files but found none!", walFiles.length > 0);

      Path bulkLoadFilesDir = new Path(backupRootDir, "bulk-load-files/default/usertable");
      assertTrue("Bulk load files directory does not exist!", fs.exists(bulkLoadFilesDir));
      FileStatus[] bulkLoadFiles = fs.listStatus(bulkLoadFilesDir);
      assertNotNull("No Bulk load files found!", bulkLoadFiles);
      assertTrue("Expected some Bulk load files but found none!", bulkLoadFiles.length > 0);
    } finally {
      TEST_UTIL.getConfiguration().setBoolean(BulkLoadHFilesTool.BULK_LOAD_HFILES_BY_FAMILY, false);
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    }
  }
}
