package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class IntegrationTestMultiTableSnapshotInputFormat extends IntegrationTestBase {


  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    util = getTestingUtil(conf);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    util = getTestingUtil(getConf());
    util.initializeCluster(1);
    this.setConf(util.getConfiguration());
  }

  @Override
  public void setUpCluster() throws Exception {

  }


  @Override
  public int runTestFromCommandLine() throws Exception {
    return 0;
  }

  @Override
  public TableName getTablename() {
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null;
  }

  @Test
  public void testMultipleSnapshotsMultipleScans() throws Exception {


  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestMultiTableSnapshotInputFormat(), args);
    System.exit(ret);
  }
}
