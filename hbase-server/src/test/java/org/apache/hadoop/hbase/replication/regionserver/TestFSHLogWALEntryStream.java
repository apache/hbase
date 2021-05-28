package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * TestWALEntryStream with {@link org.apache.hadoop.hbase.wal.FSHLogProvider} as the WAL provider.
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestFSHLogWALEntryStream extends TestWALEntryStream {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    CONF.setClass(WALFactory.WAL_PROVIDER, FSHLogProvider.class, AbstractFSWALProvider.class);
    CONF.setLong("replication.source.sleepforretries", 10);
    TEST_UTIL.startMiniDFSCluster(3);
    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
  }
}
