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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;

/**
 * Tests RegionReplicaReplicationEndpoint class by setting up region replicas and verifying
 * async wal replication replays the edits to the secondary region in various scenarios.
 */
@Category({FlakeyTests.class, LargeTests.class})
public class TestRegionReplicaReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaReplication.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionReplicaReplication.class);

  private static final int NB_SERVERS = 2;

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5); // less number of retries is needed
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);

    HTU.startMiniCluster(NB_SERVERS);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  private void testRegionReplicaReplication(int regionReplication) throws Exception {
    // test region replica replication. Create a table with single region, write some data
    // ensure that data is replicated to the secondary region
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationWithReplicas_"
        + regionReplication);
    TableDescriptor htd = HTU
      .createModifyableTableDescriptor(TableName.valueOf(tableName.toString()),
        ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
        ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED)
      .setRegionReplication(regionReplication).build();
    createOrEnableTableWithRetries(htd, true);
    TableName tableNameNoReplicas =
        TableName.valueOf("testRegionReplicaReplicationWithReplicas_NO_REPLICAS");
    HTU.deleteTableIfAny(tableNameNoReplicas);
    HTU.createTable(tableNameNoReplicas, HBaseTestingUtil.fam1);

    try (Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
      Table table = connection.getTable(tableName);
      Table tableNoReplicas = connection.getTable(tableNameNoReplicas)) {
      // load some data to the non-replicated table
      HTU.loadNumericRows(tableNoReplicas, HBaseTestingUtil.fam1, 6000, 7000);

      // load the data to the table
      HTU.loadNumericRows(table, HBaseTestingUtil.fam1, 0, 1000);

      verifyReplication(tableName, regionReplication, 0, 1000);
    } finally {
      HTU.deleteTableIfAny(tableNameNoReplicas);
    }
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow) throws Exception {
    verifyReplication(tableName, regionReplication, startRow, endRow, true);
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow, final boolean present) throws Exception {
    // find the regions
    final Region[] regions = new Region[regionReplication];

    for (int i=0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }

    for (Region region : regions) {
      assertNotNull(region);
    }

    for (int i = 1; i < regionReplication; i++) {
      final Region region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 90000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("verifying replication for region replica:" + region.getRegionInfo());
          try {
            HTU.verifyNumericRows(region, HBaseTestingUtil.fam1, startRow, endRow, present);
          } catch(Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet", ex);
            // still wait
            return false;
          }
          return true;
        }
      });
    }
  }

  @Test
  public void testRegionReplicaReplicationWith2Replicas() throws Exception {
    testRegionReplicaReplication(2);
  }

  @Test
  public void testRegionReplicaReplicationWith3Replicas() throws Exception {
    testRegionReplicaReplication(3);
  }

  @Test
  public void testRegionReplicaReplicationWith10Replicas() throws Exception {
    testRegionReplicaReplication(10);
  }

  @Test
  public void testRegionReplicaWithoutMemstoreReplication() throws Exception {
    int regionReplication = 3;
    TableDescriptor htd = HTU.createModifyableTableDescriptor(name.getMethodName())
      .setRegionReplication(regionReplication).setRegionMemStoreReplication(false).build();
    createOrEnableTableWithRetries(htd, true);
    final TableName tableName = htd.getTableName();
    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    try {
      // write data to the primary. The replicas should not receive the data
      final int STEP = 100;
      for (int i = 0; i < 3; ++i) {
        final int startRow = i * STEP;
        final int endRow = (i + 1) * STEP;
        LOG.info("Writing data from " + startRow + " to " + endRow);
        HTU.loadNumericRows(table, HBaseTestingUtil.fam1, startRow, endRow);
        verifyReplication(tableName, regionReplication, startRow, endRow, false);

        // Flush the table, now the data should show up in the replicas
        LOG.info("flushing table");
        HTU.flush(tableName);
        verifyReplication(tableName, regionReplication, 0, endRow, true);
      }
    } finally {
      table.close();
      connection.close();
    }
  }

  @Test
  public void testRegionReplicaReplicationForFlushAndCompaction() throws Exception {
    // Tests a table with region replication 3. Writes some data, and causes flushes and
    // compactions. Verifies that the data is readable from the replicas. Note that this
    // does not test whether the replicas actually pick up flushed files and apply compaction
    // to their stores
    int regionReplication = 3;
    TableDescriptor htd = HTU.createModifyableTableDescriptor(name.getMethodName())
      .setRegionReplication(regionReplication).build();
    createOrEnableTableWithRetries(htd, true);
    final TableName tableName = htd.getTableName();

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    try {
      // load the data to the table

      for (int i = 0; i < 6000; i += 1000) {
        LOG.info("Writing data from " + i + " to " + (i+1000));
        HTU.loadNumericRows(table, HBaseTestingUtil.fam1, i, i+1000);
        LOG.info("flushing table");
        HTU.flush(tableName);
        LOG.info("compacting table");
        HTU.compact(tableName, false);
      }

      verifyReplication(tableName, regionReplication, 0, 1000);
    } finally {
      table.close();
      connection.close();
    }
  }

  private void createOrEnableTableWithRetries(TableDescriptor htd, boolean createTableOperation) {
    // Helper function to run create/enable table operations with a retry feature
    boolean continueToRetry = true;
    int tries = 0;
    while (continueToRetry && tries < 50) {
      try {
        continueToRetry = false;
        if (createTableOperation) {
          HTU.getAdmin().createTable(htd);
        } else {
          HTU.getAdmin().enableTable(htd.getTableName());
        }
      } catch (IOException e) {
        if (e.getCause() instanceof ReplicationException) {
          continueToRetry = true;
          tries++;
          Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }
    }
  }
}
