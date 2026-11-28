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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestMetricsWALTableLifecycle {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE = TableName.valueOf("wal_metrics_lifecycle");
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final String COUNT_KEY = TABLE + "." + MetricsWALSource.APPEND_COUNT;
  private static final String SIZE_KEY = TABLE + "." + MetricsWALSource.APPEND_SIZE;
  private static final TableName RETRIED_TABLE = TableName.valueOf("wal_metrics_retried_open");
  private static final String RETRIED_COUNT_KEY =
    RETRIED_TABLE + "." + MetricsWALSource.APPEND_COUNT;
  private static final AtomicBoolean FAIL_OPEN = new AtomicBoolean(true);
  private static volatile MutableFastCounter failedOpenCount;

  public static final class FailOpenRegion extends HRegion {
    @SuppressWarnings("deprecation")
    public FailOpenRegion(Path tableDir, WAL wal, FileSystem fs, Configuration conf,
      RegionInfo regionInfo, TableDescriptor tableDescriptor, RegionServerServices services) {
      super(tableDir, wal, fs, conf, regionInfo, tableDescriptor, services);
    }

    @Override
    protected void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
      super.writeRegionOpenMarker(wal, openSeqId);
      if (getRegionInfo().getTable().equals(RETRIED_TABLE) && FAIL_OPEN.getAndSet(false)) {
        failedOpenCount = (MutableFastCounter) walRegistry().get(RETRIED_COUNT_KEY);
        throw new IOException("Injected failure after writing the region open marker");
      }
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.REGION_IMPL, FailOpenRegion.class, HRegion.class);
    UTIL.startMiniCluster(2);
    UTIL.getAdmin().balancerSwitch(false, true);
    // The test utility stops cache refreshes during cluster startup.
    JmxCacheBuster.restart();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    JmxCacheBuster.stop();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMoveDeleteAndRecreateTable() throws Exception {
    DynamicMetricsRegistry registry = walRegistry();
    ObjectName bean = new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=WAL");
    Admin admin = UTIL.getAdmin();
    WALKeyImpl oldKey;
    MutableFastCounter count;
    MutableFastCounter size;
    try (Table table = UTIL.createTable(TABLE, FAMILY, new byte[][] { Bytes.toBytes("m") })) {
      HRegionServer src = UTIL.getRSForFirstRegionInTable(TABLE);
      HRegionServer dst = UTIL.getOtherRegionServer(src);
      for (RegionInfo region : dst.getRegions(TABLE).stream().map(HRegion::getRegionInfo)
        .toList()) {
        UTIL.moveRegionAndWait(region, src.getServerName());
      }

      put(table, "a");
      put(table, "z");
      UTIL.waitFor(30_000, () -> hasTableAttributes(bean, true));
      count = (MutableFastCounter) registry.get(COUNT_KEY);
      size = (MutableFastCounter) registry.get(SIZE_KEY);
      assertNotNull(count);
      assertNotNull(size);
      long initialCount = count.value();
      long initialSize = size.value();

      HRegion moved = src.getRegions(TABLE).stream()
        .filter(region -> Bytes.equals(region.getRegionInfo().getStartKey(), Bytes.toBytes("m")))
        .findFirst().orElseThrow();
      oldKey =
        new WALKeyImpl(moved.getRegionInfo().getEncodedNameAsBytes(), TABLE, 0, moved.getMVCC());
      UTIL.moveRegionAndWait(moved.getRegionInfo(), dst.getServerName());
      put(table, "a");
      assertSame(count, registry.get(COUNT_KEY));
      assertSame(size, registry.get(SIZE_KEY));
      assertTrue(count.value() > initialCount);
      assertTrue(size.value() > initialSize);
    }

    admin.disableTable(TABLE);
    admin.deleteTable(TABLE);
    assertFalse(admin.tableExists(TABLE));
    assertNull(registry.get(COUNT_KEY));
    assertNull(registry.get(SIZE_KEY));
    UTIL.waitFor(30_000, () -> hasTableAttributes(bean, false));

    try (Table table = UTIL.createTable(TABLE, FAMILY)) {
      put(table, "a");
      MutableFastCounter newCount = (MutableFastCounter) registry.get(COUNT_KEY);
      MutableFastCounter newSize = (MutableFastCounter) registry.get(SIZE_KEY);
      assertNotNull(newCount);
      assertNotNull(newSize);
      assertNotSame(count, newCount);
      assertNotSame(size, newSize);
      long recreatedCount = newCount.value();
      long recreatedSize = newSize.value();
      new MetricsWAL().postAppend(123, 0, oldKey, null);
      assertEquals(recreatedCount, newCount.value());
      assertEquals(recreatedSize, newSize.value());
    }
  }

  @Test
  public void testFailedOpenReleasesTableMetrics() throws Exception {
    DynamicMetricsRegistry registry = walRegistry();
    try (Table table = UTIL.createTable(RETRIED_TABLE, FAMILY)) {
      put(table, "a");
      assertNotNull(failedOpenCount);
      assertNotNull(registry.get(RETRIED_COUNT_KEY));
      assertNotSame(failedOpenCount, registry.get(RETRIED_COUNT_KEY));
    }
    UTIL.deleteTable(RETRIED_TABLE);
    assertNull(registry.get(RETRIED_COUNT_KEY));
  }

  private static DynamicMetricsRegistry walRegistry() {
    return ((MetricsWALSourceImpl) CompatibilitySingletonFactory
      .getInstance(MetricsWALSource.class)).getMetricsRegistry();
  }

  private static boolean hasTableAttributes(ObjectName bean, boolean present) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      Set<String> attributes = Arrays.stream(server.getMBeanInfo(bean).getAttributes())
        .map(MBeanAttributeInfo::getName).collect(Collectors.toSet());
      return attributes.contains(COUNT_KEY) == present && attributes.contains(SIZE_KEY) == present
        && server.getAttribute(bean, MetricsWALSource.APPEND_COUNT) instanceof Number;
    } catch (InstanceNotFoundException | AttributeNotFoundException e) {
      // Wait for the WAL MBean and its global count to be available after a cache refresh.
      return false;
    }
  }

  private static void put(Table table, String row) throws IOException {
    table.put(new Put(Bytes.toBytes(row)).addColumn(FAMILY, FAMILY, FAMILY)
      .setDurability(Durability.SYNC_WAL));
  }
}
