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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRegionSplitPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionSplitPolicy.class);

  private Configuration conf;
  private HRegion mockRegion;
  private List<HStore> stores;
  private static final TableName TABLENAME = TableName.valueOf("t");

  @Before
  public void setupMocks() {
    conf = HBaseConfiguration.create();
    RegionInfo hri = RegionInfoBuilder.newBuilder(TABLENAME).build();
    mockRegion = mock(HRegion.class);
    doReturn(hri).when(mockRegion).getRegionInfo();
    doReturn(true).when(mockRegion).isAvailable();
    stores = new ArrayList<>();
    doReturn(stores).when(mockRegion).getStores();
  }

  @Test
  public void testForceSplitRegionWithReference() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).setMaxFileSize(1024L).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    // Add a store above the requisite size. Should split.
    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    // Act as if there's a reference file or some other reason it can't split.
    // This should prevent splitting even though it's big enough.
    doReturn(false).when(mockStore).canSplit();
    stores.add(mockStore);

    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    assertFalse(policy.shouldSplit());

    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      IncreasingToUpperBoundRegionSplitPolicy.class.getName());
    policy = (IncreasingToUpperBoundRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    assertFalse(policy.shouldSplit());
  }

  @Test
  public void testIncreasingToUpperBoundRegionSplitPolicy() throws IOException {
    // Configure IncreasingToUpperBoundRegionSplitPolicy as our split policy
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      IncreasingToUpperBoundRegionSplitPolicy.class.getName());
    // Now make it so the mock region has a RegionServerService that will
    // return 'online regions'.
    RegionServerServices rss = mock(RegionServerServices.class);
    final List<HRegion> regions = new ArrayList<>();
    doReturn(regions).when(rss).getRegions(TABLENAME);
    when(mockRegion.getRegionServerServices()).thenReturn(rss);
    // Set max size for this 'table'.
    long maxSplitSize = 1024L;
    // Set flush size to 1/8. IncreasingToUpperBoundRegionSplitPolicy
    // grows by the cube of the number of regions times flushsize each time.
    long flushSize = maxSplitSize / 8;
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSize);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).setMaxFileSize(maxSplitSize)
      .setMemStoreFlushSize(flushSize).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    // If RegionServerService with no regions in it -- 'online regions' == 0 --
    // then IncreasingToUpperBoundRegionSplitPolicy should act like a
    // ConstantSizePolicy
    IncreasingToUpperBoundRegionSplitPolicy policy =
      (IncreasingToUpperBoundRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    doConstantSizePolicyTests(policy);

    // Add a store in excess of split size. Because there are "no regions"
    // on this server -- rss.getOnlineRegions is 0 -- then we should split
    // like a constantsizeregionsplitpolicy would
    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);
    // It should split
    assertTrue(policy.shouldSplit());

    // Now test that we increase our split size as online regions for a table
    // grows. With one region, split size should be flushsize.
    regions.add(mockRegion);
    doReturn(flushSize).when(mockStore).getSize();
    // Should not split since store is flush size.
    assertFalse(policy.shouldSplit());
    // Set size of store to be > 2*flush size and we should split
    doReturn(flushSize * 2 + 1).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());
    // Add another region to the 'online regions' on this server and we should
    // now be no longer be splittable since split size has gone up.
    regions.add(mockRegion);
    assertFalse(policy.shouldSplit());
    // make sure its just over; verify it'll split
    doReturn((long) (maxSplitSize * 1.25 + 1)).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());

    // Finally assert that even if loads of regions, we'll split at max size
    assertWithinJitter(maxSplitSize, policy.getSizeToCheck(1000));
    // Assert same is true if count of regions is zero.
    assertWithinJitter(maxSplitSize, policy.getSizeToCheck(0));
  }

  @Test
  public void testIsExceedSize() throws IOException {
    // Configure SteppingAllStoresSizeSplitPolicy as our split policy
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    conf.set(HConstants.OVERALL_HREGION_FILES, "true");
    // Now make it so the mock region has a RegionServerService that will
    // return 'online regions'.
    RegionServerServices rss = mock(RegionServerServices.class);
    final List<HRegion> regions = new ArrayList<>();
    doReturn(regions).when(rss).getRegions(TABLENAME);
    when(mockRegion.getRegionServerServices()).thenReturn(rss);

    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    regions.add(mockRegion);

    HStore mockStore1 = mock(HStore.class);
    doReturn(100L).when(mockStore1).getSize();
    HStore mockStore2 = mock(HStore.class);
    doReturn(924L).when(mockStore2).getSize();
    HStore mockStore3 = mock(HStore.class);
    doReturn(925L).when(mockStore3).getSize();

    // test sum of store's size not greater than sizeToCheck
    stores.add(mockStore1);
    stores.add(mockStore2);
    assertFalse(policy.isExceedSize(1024));
    stores.clear();

    // test sum of store's size greater than sizeToCheck
    stores.add(mockStore1);
    stores.add(mockStore3);
    assertTrue(policy.isExceedSize(1024));
  }

  @Test
  public void testBusyRegionSplitPolicy() throws Exception {
    doReturn(TableDescriptorBuilder.newBuilder(TABLENAME).build()).when(mockRegion)
      .getTableDescriptor();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, BusyRegionSplitPolicy.class.getName());
    conf.setLong("hbase.busy.policy.minAge", 1000000L);
    conf.setFloat("hbase.busy.policy.blockedRequests", 0.1f);

    RegionServerServices rss = mock(RegionServerServices.class);
    final List<HRegion> regions = new ArrayList<>();
    doReturn(regions).when(rss).getRegions(TABLENAME);
    when(mockRegion.getRegionServerServices()).thenReturn(rss);
    when(mockRegion.getBlockedRequestsCount()).thenReturn(0L);
    when(mockRegion.getWriteRequestsCount()).thenReturn(0L);

    BusyRegionSplitPolicy policy =
      (BusyRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);

    when(mockRegion.getBlockedRequestsCount()).thenReturn(10L);
    when(mockRegion.getWriteRequestsCount()).thenReturn(10L);
    // Not enough time since region came online
    assertFalse(policy.shouldSplit());

    // Reset min age for split to zero
    conf.setLong("hbase.busy.policy.minAge", 0L);
    // Aggregate over 500 ms periods
    conf.setLong("hbase.busy.policy.aggWindow", 500L);
    policy = (BusyRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    long start = EnvironmentEdgeManager.currentTime();
    when(mockRegion.getBlockedRequestsCount()).thenReturn(10L);
    when(mockRegion.getWriteRequestsCount()).thenReturn(20L);
    Thread.sleep(300);
    assertFalse(policy.shouldSplit());
    when(mockRegion.getBlockedRequestsCount()).thenReturn(12L);
    when(mockRegion.getWriteRequestsCount()).thenReturn(30L);
    Thread.sleep(2);
    // Enough blocked requests since last time, but aggregate blocked request
    // rate over last 500 ms is still low, because major portion of the window is constituted
    // by the previous zero blocked request period which lasted at least 300 ms off last 500 ms.
    if (EnvironmentEdgeManager.currentTime() - start < 500) {
      assertFalse(policy.shouldSplit());
    }
    when(mockRegion.getBlockedRequestsCount()).thenReturn(14L);
    when(mockRegion.getWriteRequestsCount()).thenReturn(40L);
    Thread.sleep(200);
    assertTrue(policy.shouldSplit());
  }

  private void assertWithinJitter(long maxSplitSize, long sizeToCheck) {
    assertTrue("Size greater than lower bound of jitter",
      (long) (maxSplitSize * 0.75) <= sizeToCheck);
    assertTrue("Size less than upper bound of jitter", (long) (maxSplitSize * 1.25) >= sizeToCheck);
  }

  @Test
  public void testCreateDefault() throws IOException {
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 1234L);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    // Using a default HTD, should pick up the file size from
    // configuration.
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    assertWithinJitter(1234L, policy.getDesiredMaxFileSize());

    // If specified in HTD, should use that
    td = TableDescriptorBuilder.newBuilder(TABLENAME).setMaxFileSize(9999L).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    policy = (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    assertWithinJitter(9999L, policy.getDesiredMaxFileSize());
  }

  /**
   * Test setting up a customized split policy
   */
  @Test
  public void testCustomPolicy() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME)
      .setRegionSplitPolicyClassName(KeyPrefixRegionSplitPolicy.class.getName())
      .setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "2").build();

    doReturn(td).when(mockRegion).getTableDescriptor();

    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    doReturn(Optional.of(Bytes.toBytes("abcd"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    KeyPrefixRegionSplitPolicy policy =
      (KeyPrefixRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);

    assertEquals("ab", Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testConstantSizePolicy() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).setMaxFileSize(1024L).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    doConstantSizePolicyTests(policy);
  }

  /**
   * Run through tests for a ConstantSizeRegionSplitPolicy
   */
  private void doConstantSizePolicyTests(final ConstantSizeRegionSplitPolicy policy) {
    // For no stores, should not split
    assertFalse(policy.shouldSplit());

    // Add a store above the requisite size. Should split.
    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);

    assertTrue(policy.shouldSplit());

    // Act as if there's a reference file or some other reason it can't split.
    // This should prevent splitting even though it's big enough.
    doReturn(false).when(mockStore).canSplit();
    assertFalse(policy.shouldSplit());

    // Reset splittability after above
    doReturn(true).when(mockStore).canSplit();

    // Set to a small size, should not split
    doReturn(100L).when(mockStore).getSize();
    assertFalse(policy.shouldSplit());

    // Clear families we added above
    stores.clear();
  }

  @Test
  public void testGetSplitPoint() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME).build();
    doReturn(td).when(mockRegion).getTableDescriptor();

    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);

    // For no stores, should not split
    assertFalse(policy.shouldSplit());
    assertNull(policy.getSplitPoint());

    // Add a store above the requisite size. Should split.
    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    doReturn(Optional.of(Bytes.toBytes("store 1 split"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    assertEquals("store 1 split", Bytes.toString(policy.getSplitPoint()));

    // Add a bigger store. The split point should come from that one
    HStore mockStore2 = mock(HStore.class);
    doReturn(4000L).when(mockStore2).getSize();
    doReturn(true).when(mockStore2).canSplit();
    doReturn(Optional.of(Bytes.toBytes("store 2 split"))).when(mockStore2).getSplitPoint();
    stores.add(mockStore2);

    assertEquals("store 2 split", Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testDelimitedKeyPrefixRegionSplitPolicy() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLENAME)
      .setRegionSplitPolicyClassName(DelimitedKeyPrefixRegionSplitPolicy.class.getName())
      .setValue(DelimitedKeyPrefixRegionSplitPolicy.DELIMITER_KEY, ",").build();

    doReturn(td).when(mockRegion).getTableDescriptor();
    doReturn(stores).when(mockRegion).getStores();

    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    doReturn(Optional.of(Bytes.toBytes("ab,cd"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    DelimitedKeyPrefixRegionSplitPolicy policy =
      (DelimitedKeyPrefixRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);

    assertEquals("ab", Bytes.toString(policy.getSplitPoint()));

    doReturn(Optional.of(Bytes.toBytes("ijk"))).when(mockStore).getSplitPoint();
    assertEquals("ijk", Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testConstantSizePolicyWithJitter() throws IOException {
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    TableDescriptor td =
      TableDescriptorBuilder.newBuilder(TABLENAME).setMaxFileSize(Long.MAX_VALUE).build();
    doReturn(td).when(mockRegion).getTableDescriptor();
    boolean positiveJitter = false;
    ConstantSizeRegionSplitPolicy policy = null;
    while (!positiveJitter) {
      policy = (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
      positiveJitter = policy.positiveJitterRate();
    }
    // add a store
    HStore mockStore = mock(HStore.class);
    doReturn(2000L).when(mockStore).getSize();
    doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);
    // Jitter shouldn't cause overflow when HTableDescriptor.MAX_FILESIZE set to Long.MAX_VALUE
    assertFalse(policy.shouldSplit());
  }
}
