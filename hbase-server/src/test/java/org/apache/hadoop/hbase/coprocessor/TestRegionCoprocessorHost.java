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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.USER_COPROCESSORS_ENABLED_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({SmallTests.class})
public class TestRegionCoprocessorHost {
  private Configuration conf;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionCoprocessorHost.class);

  @Rule
  public final TestName name = new TestName();
  private RegionInfo regionInfo;
  private HRegion region;
  private RegionServerServices rsServices;
  public static final int MAX_VERSIONS = 3;
  public static final int MIN_VERSIONS = 2;
  public static final int TTL = 1000;
  public static final int TIME_TO_PURGE_DELETES = 2000;

  @Before
  public void setup() throws IOException {
    init(null);
  }

  private void init(Boolean flag) throws IOException {
    conf = HBaseConfiguration.create();
    conf.setBoolean(COPROCESSORS_ENABLED_CONF_KEY, true);
    conf.setBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY, true);
    TableName tableName = TableName.valueOf(name.getMethodName());
    regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptor tableDesc = null;
    if (flag == null) {
      // configure a coprocessor which override postScannerFilterRow
      tableDesc = TableDescriptorBuilder.newBuilder(tableName)
          .setCoprocessor(SimpleRegionObserver.class.getName()).build();
    } else if (flag) {
      // configure a coprocessor which don't override postScannerFilterRow
      tableDesc = TableDescriptorBuilder.newBuilder(tableName)
          .setCoprocessor(TempRegionObserver.class.getName()).build();
    } else {
      // configure two coprocessors, one don't override postScannerFilterRow but another one does
      conf.set(REGION_COPROCESSOR_CONF_KEY, TempRegionObserver.class.getName());
      tableDesc = TableDescriptorBuilder.newBuilder(tableName)
          .setCoprocessor(SimpleRegionObserver.class.getName()).build();
    }
    region = mock(HRegion.class);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(region.getTableDescriptor()).thenReturn(tableDesc);
    rsServices = mock(RegionServerServices.class);
  }

  @Test
  public void testLoadDuplicateCoprocessor() throws Exception {
    conf.setBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR, true);
    conf.set(REGION_COPROCESSOR_CONF_KEY, SimpleRegionObserver.class.getName());
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    // Only one coprocessor SimpleRegionObserver loaded
    assertEquals(1, host.coprocEnvironments.size());

    // Allow to load duplicate coprocessor
    conf.setBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR, false);
    host = new RegionCoprocessorHost(region, rsServices, conf);
    // Two duplicate coprocessors loaded
    assertEquals(2, host.coprocEnvironments.size());
  }

  @Test
  public void testPreStoreScannerOpen() throws IOException {

    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    Scan scan = new Scan();
    scan.setTimeRange(TimeRange.INITIAL_MIN_TIMESTAMP, TimeRange.INITIAL_MAX_TIMESTAMP);
    assertTrue("Scan is not for all time", scan.getTimeRange().isAllTime());
    //SimpleRegionObserver is set to update the ScanInfo parameters if the passed-in scan
    //is for all time. this lets us exercise both that the Scan is wired up properly in the coproc
    //and that we can customize the metadata

    ScanInfo oldScanInfo = getScanInfo();

    HStore store = mock(HStore.class);
    when(store.getScanInfo()).thenReturn(oldScanInfo);
    ScanInfo newScanInfo = host.preStoreScannerOpen(store, scan);

    verifyScanInfo(newScanInfo);
  }

  @Test
  public void testPreCompactScannerOpen() throws IOException {
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    ScanInfo oldScanInfo = getScanInfo();
    HStore store = mock(HStore.class);
    when(store.getScanInfo()).thenReturn(oldScanInfo);
    ScanInfo newScanInfo = host.preCompactScannerOpen(store, ScanType.COMPACT_DROP_DELETES,
      mock(CompactionLifeCycleTracker.class), mock(CompactionRequest.class), mock(User.class));
    verifyScanInfo(newScanInfo);
  }

  @Test
  public void testPreFlushScannerOpen() throws IOException {
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    ScanInfo oldScanInfo = getScanInfo();
    HStore store = mock(HStore.class);
    when(store.getScanInfo()).thenReturn(oldScanInfo);
    ScanInfo newScanInfo = host.preFlushScannerOpen(store, mock(FlushLifeCycleTracker.class));
    verifyScanInfo(newScanInfo);
  }

  @Test
  public void testPreMemStoreCompactionCompactScannerOpen() throws IOException {
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    ScanInfo oldScanInfo = getScanInfo();
    HStore store = mock(HStore.class);
    when(store.getScanInfo()).thenReturn(oldScanInfo);
    ScanInfo newScanInfo = host.preMemStoreCompactionCompactScannerOpen(store);
    verifyScanInfo(newScanInfo);
  }

  @Test
  public void testPostScannerFilterRow() throws IOException {
    // By default SimpleRegionObserver is set as region coprocessor which implements
    // postScannerFilterRow
    RegionCoprocessorHost host = new RegionCoprocessorHost(region, rsServices, conf);
    assertTrue("Region coprocessor implement postScannerFilterRow",
      host.hasCustomPostScannerFilterRow());

    // Set a region CP which doesn't implement postScannerFilterRow
    init(true);
    host = new RegionCoprocessorHost(region, rsServices, conf);
    assertFalse("Region coprocessor implement postScannerFilterRow",
      host.hasCustomPostScannerFilterRow());

    // Set multiple region CPs, in which one implements postScannerFilterRow
    init(false);
    host = new RegionCoprocessorHost(region, rsServices, conf);
    assertTrue("Region coprocessor doesn't implement postScannerFilterRow",
      host.hasCustomPostScannerFilterRow());
  }

  private void verifyScanInfo(ScanInfo newScanInfo) {
    assertEquals(KeepDeletedCells.TRUE, newScanInfo.getKeepDeletedCells());
    assertEquals(MAX_VERSIONS, newScanInfo.getMaxVersions());
    assertEquals(MIN_VERSIONS, newScanInfo.getMinVersions());
    assertEquals(TTL, newScanInfo.getTtl());
    assertEquals(TIME_TO_PURGE_DELETES, newScanInfo.getTimeToPurgeDeletes());
  }

  private ScanInfo getScanInfo() {
    int oldMaxVersions = 1;
    int oldMinVersions = 0;
    long oldTTL = 10000;

    return new ScanInfo(conf, Bytes.toBytes("cf"), oldMinVersions, oldMaxVersions, oldTTL,
    KeepDeletedCells.FALSE, HConstants.FOREVER, 1000,
      CellComparator.getInstance(), true);
  }

  /*
   * Simple region coprocessor which doesn't override postScannerFilterRow
   */
  public static class TempRegionObserver implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }
}
