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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
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

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import static org.junit.Assert.assertEquals;

/**
 * Testcase for HBASE-24871.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestRefreshRecoveredReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRefreshRecoveredReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshRecoveredReplication.class);

  private static final int BATCH = 50;

  @Rule
  public TestName name = new TestName();

  private TableName tablename;
  private Table table1;
  private Table table2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // NUM_SLAVES1 is presumed 2 in below.
    NUM_SLAVES1 = 2;
    // replicate slowly
    Configuration conf1 = UTIL1.getConfiguration();
    conf1.setInt(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY, 100);
    TestReplicationBase.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
  }

  @Before
  public void setup() throws Exception {
    setUpBase();

    tablename = TableName.valueOf(name.getMethodName());
    TableDescriptor table = TableDescriptorBuilder.newBuilder(tablename)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build();

    UTIL1.getAdmin().createTable(table);
    UTIL2.getAdmin().createTable(table);
    UTIL1.waitTableAvailable(tablename);
    UTIL2.waitTableAvailable(tablename);
    table1 = UTIL1.getConnection().getTable(tablename);
    table2 = UTIL2.getConnection().getTable(tablename);
  }

  @After
  public void teardown() throws Exception {
    tearDownBase();

    UTIL1.deleteTableIfAny(tablename);
    UTIL2.deleteTableIfAny(tablename);
  }

  @Test
  public void testReplicationRefreshSource() throws Exception {
    // put some data
    for (int i = 0; i < BATCH; i++) {
      byte[] r = Bytes.toBytes(i);
      table1.put(new Put(r).addColumn(famName, famName, r));
    }

    // Kill rs holding table region. There are only TWO servers. We depend on it.
    List<RegionServerThread> rss = UTIL1.getMiniHBaseCluster().getLiveRegionServerThreads();
    assertEquals(2, rss.size());
    Optional<RegionServerThread> server = rss.stream()
        .filter(rst -> CollectionUtils.isNotEmpty(rst.getRegionServer().getRegions(tablename)))
        .findAny();
    Assert.assertTrue(server.isPresent());
    HRegionServer otherServer = rss.get(0).getRegionServer() == server.get().getRegionServer()?
      rss.get(1).getRegionServer(): rss.get(0).getRegionServer();
    server.get().getRegionServer().abort("stopping for test");
    // waiting for recovered peer to appear.
    Replication replication = (Replication)otherServer.getReplicationSourceService();
    UTIL1.waitFor(60000, () -> !replication.getReplicationManager().getOldSources().isEmpty());
    // Wait on only one server being up.
    // Have to go back to source here because getLiveRegionServerThreads makes new array each time
    UTIL1.waitFor(60000,
      () -> UTIL1.getMiniHBaseCluster().getLiveRegionServerThreads().size() == NUM_SLAVES1 - 1);
    UTIL1.waitTableAvailable(tablename);
    LOG.info("Available {}", tablename);

    // disable peer to trigger refreshSources
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    LOG.info("has replicated {} rows before disable peer", checkReplicationData());
    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    // waiting to replicate all data to slave
    UTIL2.waitFor(60000, () -> {
      int count = checkReplicationData();
      LOG.info("Waiting all logs pushed to slave. Expected {} , actual {}", BATCH, count);
      return count == BATCH;
    });
  }

  private int checkReplicationData() throws IOException {
    int count = 0;
    ResultScanner results = table2.getScanner(new Scan().setCaching(BATCH));
    for (Result r : results) {
      count++;
    }
    return count;
  }
}
