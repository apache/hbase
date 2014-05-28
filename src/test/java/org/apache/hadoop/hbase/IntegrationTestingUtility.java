package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Class used like HBaseTestingUtility but only used by IntegrationTests.
 *
 * Mostly this is used to ensure a cluster is ready for tests to start and that
 * the cluster is restored after a test is finished.
 */
public class IntegrationTestingUtility {

  private final Configuration conf;
  private final HBaseDistributedCluster cluster;

  public IntegrationTestingUtility() {
    this.conf = HBaseConfiguration.create();
    this.cluster = new HBaseDistributedCluster();
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public void restoreCluster() {

  }

  public HBaseDistributedCluster getDistributedCluster() {
    return null;
  }

  public HBaseAdmin getHBaseAdmin() {
    return null;
  }
}
