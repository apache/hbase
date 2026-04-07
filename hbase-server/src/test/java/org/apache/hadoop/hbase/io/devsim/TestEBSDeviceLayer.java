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
package org.apache.hadoop.hbase.io.devsim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test verifying that the EBS device layer proxy is correctly installed by
 * MiniDFSCluster and intercepts read and write IO at the DataNode storage level.
 * <p>
 * Starts a single-DataNode MiniDFSCluster with 2 storage volumes and the EBS device layer
 * configured with high bandwidth/IOPS budgets (so throttling does not slow the test) but with
 * device latency disabled. Writes data through HBase, flushes, reads it back via scan and get, and
 * asserts that the device layer metrics reflect the IO.
 */
@Category({ IOTests.class, MediumTests.class })
public class TestEBSDeviceLayer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEBSDeviceLayer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestEBSDeviceLayer.class);

  private static final TableName TABLE_NAME = TableName.valueOf("TestEBSDeviceLayer");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_ROWS = 200;
  private static final int VALUE_SIZE = 4096;
  private static final int NUM_VOLUMES = 2;

  private static HBaseTestingUtil UTIL;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      "org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy");
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    EBSDevice.configure(conf, 1000, 100000, 0, 1024, 0);

    UTIL = new HBaseTestingUtil(conf);
    UTIL.startMiniZKCluster();
    MiniDFSCluster dfsCluster =
      new MiniDFSCluster.Builder(conf).numDataNodes(1).storagesPerDatanode(NUM_VOLUMES).build();
    dfsCluster.waitClusterUp();
    UTIL.setDFSCluster(dfsCluster);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EBSDevice.shutdown();
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testDeviceLayerInterceptsIO() throws Exception {
    assertEquals("Expected 1 DataNode registered with EBSDevice", 1, EBSDevice.getNumDataNodes());
    EBSDevice.DataNodeContext dnCtx = EBSDevice.getDataNodeContext(0);
    assertEquals("Expected " + NUM_VOLUMES + " volumes", NUM_VOLUMES, dnCtx.getNumVolumes());

    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(
      ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setBlocksize(64 * 1024).build()).build();
    UTIL.getAdmin().createTable(desc);
    UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);

    EBSDevice.resetMetrics();

    byte[] value = new byte[VALUE_SIZE];
    Bytes.random(value);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < NUM_ROWS; i++) {
        Put put = new Put(Bytes.toBytes(String.format("row-%05d", i)));
        put.addColumn(FAMILY, QUALIFIER, value);
        table.put(put);
      }
    }

    UTIL.getAdmin().flush(TABLE_NAME);
    waitForFlush();

    long writeBytesAfterFlush = EBSDevice.getTotalBytesWritten();
    long writeInterceptsAfterFlush = EBSDevice.getWriteInterceptCount();
    LOG.info("After write+flush: bytesWritten={}, writeIntercepts={}, deviceWriteOps={}",
      writeBytesAfterFlush, writeInterceptsAfterFlush, EBSDevice.getDeviceWriteOps());

    assertTrue("Expected write intercepts after flush, got " + writeInterceptsAfterFlush,
      writeInterceptsAfterFlush > 0);
    assertTrue("Expected bytes written > 0 after flush, got " + writeBytesAfterFlush,
      writeBytesAfterFlush > 0);

    EBSDevice.resetMetrics();
    int rowCount = 0;
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result;
        while ((result = scanner.next()) != null) {
          assertTrue("Row should not be empty", !result.isEmpty());
          rowCount++;
        }
      }
    }
    assertEquals("Expected to read back all rows", NUM_ROWS, rowCount);

    long readBytes = EBSDevice.getTotalBytesRead();
    long readIntercepts = EBSDevice.getReadInterceptCount();
    long deviceReadOps = EBSDevice.getDeviceReadOps();
    long readOps = EBSDevice.getReadOpCount();
    LOG.info("After scan: bytesRead={}, readIntercepts={}, appReadOps={}, deviceReadOps={}",
      readBytes, readIntercepts, readOps, deviceReadOps);

    assertTrue("Expected read intercepts after scan, got " + readIntercepts, readIntercepts > 0);
    assertTrue("Expected bytes read > 0 after scan, got " + readBytes, readBytes > 0);
    assertTrue("Expected device read ops > 0 (IOPS coalescing should still produce ops), got "
      + deviceReadOps, deviceReadOps > 0);

    EBSDevice.resetMetrics();
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      Result result = table.get(new Get(Bytes.toBytes("row-00050")));
      assertTrue("Get should return data", !result.isEmpty());
    }
    long getReadIntercepts = EBSDevice.getReadInterceptCount();
    LOG.info("After get: readIntercepts={}, bytesRead={}", getReadIntercepts,
      EBSDevice.getTotalBytesRead());
    assertTrue("Expected read intercepts after get, got " + getReadIntercepts,
      getReadIntercepts > 0);

    long totalIntercepts = EBSDevice.getReadInterceptCount() + EBSDevice.getWriteInterceptCount();
    long unresolved = EBSDevice.getUnresolvedVolumeCount();
    if (totalIntercepts > 0) {
      double unresolvedRatio = (double) unresolved / totalIntercepts;
      assertTrue("Unresolved volume ratio too high: " + unresolvedRatio + " (unresolved="
        + unresolved + ", total=" + totalIntercepts + ")", unresolvedRatio <= 0.01);
    }

    LOG.info("Per-volume stats: {}", EBSDevice.getPerVolumeStats());
  }

  private void waitForFlush() throws Exception {
    long deadline = System.currentTimeMillis() + 60000;
    while (System.currentTimeMillis() < deadline) {
      long memstoreSize = 0;
      for (HRegion region : UTIL.getMiniHBaseCluster().getRegionServer(0).getRegions(TABLE_NAME)) {
        memstoreSize += region.getMemStoreDataSize();
      }
      if (memstoreSize == 0) {
        return;
      }
      Thread.sleep(500);
    }
    throw new IOException("Flush did not complete within timeout");
  }
}
