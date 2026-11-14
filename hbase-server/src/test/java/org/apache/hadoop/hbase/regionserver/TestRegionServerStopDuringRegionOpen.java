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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Stopping a region server while an AssignRegionHandler is opening a region must not abort the
 * region server, and the region that was opened must be closed.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionServerStopDuringRegionOpen {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("StopDuringOpen");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static volatile ServerName blockOpenOn;

  private static volatile HRegion openedRegion;

  private static final CountDownLatch OPENING = new CountDownLatch(1);

  private static final CountDownLatch RESUME = new CountDownLatch(1);

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(2).build());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setCoprocessor(BlockOpenCoprocessor.class.getName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build();
    UTIL.getAdmin().createTable(td);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    RESUME.countDown();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStopDuringRegionOpen() throws Exception {
    SingleProcessHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    // Open the region on the region server that does not carry meta, so stopping it is cheap.
    int metaIndex = cluster.getServerWithMeta();
    RegionServerThread target = cluster.getRegionServerThreads().get(metaIndex == 0 ? 1 : 0);
    HRegionServer rs = target.getRegionServer();
    HRegionServer other = cluster.getRegionServer(metaIndex);
    RegionInfo region = UTIL.getAdmin().getRegions(TABLE_NAME).get(0);
    if (!rs.getRegions(TABLE_NAME).isEmpty()) {
      UTIL.getAdmin().move(region.getEncodedNameAsBytes(), other.getServerName());
    }

    blockOpenOn = rs.getServerName();
    UTIL.getAsyncConnection().getAdmin().move(region.getEncodedNameAsBytes(), rs.getServerName());
    assertTrue(OPENING.await(60, TimeUnit.SECONDS), "Region open did not start");

    rs.stop("Stop while opening a region");
    RESUME.countDown();
    target.join(60000);

    assertFalse(target.isAlive(), "Region server did not stop");
    assertFalse(rs.isAborted(), "Region server should not be aborted");
    assertNotNull(openedRegion);
    assertTrue(openedRegion.isClosed(), "Opened region should be closed");
  }

  public static class BlockOpenCoprocessor implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
      RegionCoprocessorEnvironment env = c.getEnvironment();
      if (!env.getServerName().equals(blockOpenOn) || openedRegion != null) {
        return;
      }
      openedRegion = (HRegion) env.getRegion();
      OPENING.countDown();
      try {
        RESUME.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
