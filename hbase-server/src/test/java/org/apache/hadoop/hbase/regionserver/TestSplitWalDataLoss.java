/**
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegion.PrepareFlushResult;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testcase for https://issues.apache.org/jira/browse/HBASE-13811
 */
@Category({ MediumTests.class })
public class TestSplitWalDataLoss {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitWalDataLoss.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitWalDataLoss.class);

  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();

  private NamespaceDescriptor namespace = NamespaceDescriptor.create(getClass().getSimpleName())
      .build();

  private TableName tableName = TableName.valueOf(namespace.getName(), "dataloss");

  private byte[] family = Bytes.toBytes("f");

  private byte[] qualifier = Bytes.toBytes("q");

  @Before
  public void setUp() throws Exception {
    testUtil.getConfiguration().setInt("hbase.regionserver.msginterval", 30000);
    testUtil.startMiniCluster(2);
    Admin admin = testUtil.getAdmin();
    admin.createNamespace(namespace);
    admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build());
    testUtil.waitTableAvailable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    final HRegionServer rs = testUtil.getRSForFirstRegionInTable(tableName);
    final HRegion region = (HRegion) rs.getRegions(tableName).get(0);
    HRegion spiedRegion = spy(region);
    final MutableBoolean flushed = new MutableBoolean(false);
    final MutableBoolean reported = new MutableBoolean(false);
    doAnswer(new Answer<FlushResult>() {
      @Override
      public FlushResult answer(InvocationOnMock invocation) throws Throwable {
        synchronized (flushed) {
          flushed.setValue(true);
          flushed.notifyAll();
        }
        synchronized (reported) {
          while (!reported.booleanValue()) {
            reported.wait();
          }
        }
        rs.getWAL(region.getRegionInfo()).abortCacheFlush(
          region.getRegionInfo().getEncodedNameAsBytes());
        throw new DroppedSnapshotException("testcase");
      }
    }).when(spiedRegion).internalFlushCacheAndCommit(Matchers.<WAL> any(),
      Matchers.<MonitoredTask> any(), Matchers.<PrepareFlushResult> any(),
      Matchers.<Collection<HStore>> any());
    // Find region key; don't pick up key for hbase:meta by mistake.
    String key = null;
    for (Map.Entry<String, HRegion> entry: rs.getOnlineRegions().entrySet()) {
      if (entry.getValue().getRegionInfo().getTable().equals(this.tableName)) {
        key = entry.getKey();
        break;
      }
    }
    rs.getOnlineRegions().put(key, spiedRegion);
    Connection conn = testUtil.getConnection();

    try (Table table = conn.getTable(tableName)) {
      table.put(new Put(Bytes.toBytes("row0"))
              .addColumn(family, qualifier, Bytes.toBytes("val0")));
    }
    long oldestSeqIdOfStore = region.getOldestSeqIdOfStore(family);
    LOG.info("CHANGE OLDEST " + oldestSeqIdOfStore);
    assertTrue(oldestSeqIdOfStore > HConstants.NO_SEQNUM);
    rs.getMemStoreFlusher().requestFlush(spiedRegion, false, FlushLifeCycleTracker.DUMMY);
    synchronized (flushed) {
      while (!flushed.booleanValue()) {
        flushed.wait();
      }
    }
    try (Table table = conn.getTable(tableName)) {
      table.put(new Put(Bytes.toBytes("row1"))
              .addColumn(family, qualifier, Bytes.toBytes("val1")));
    }
    long now = EnvironmentEdgeManager.currentTime();
    rs.tryRegionServerReport(now - 500, now);
    synchronized (reported) {
      reported.setValue(true);
      reported.notifyAll();
    }
    while (testUtil.getRSForFirstRegionInTable(tableName) == rs) {
      Thread.sleep(100);
    }
    try (Table table = conn.getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes("row0")));
      assertArrayEquals(Bytes.toBytes("val0"), result.getValue(family, qualifier));
    }
  }
}
