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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRegionSplitPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionSplitPolicy.class);

  private Configuration conf;
  private HTableDescriptor htd;
  private HRegion mockRegion;
  private List<HStore> stores;
  private static final TableName TABLENAME = TableName.valueOf("t");

  @Rule
  public TestName name = new TestName();

  @Before
  public void setupMocks() {
    conf = HBaseConfiguration.create();
    HRegionInfo hri = new HRegionInfo(TABLENAME);
    htd = new HTableDescriptor(TABLENAME);
    mockRegion = Mockito.mock(HRegion.class);
    Mockito.doReturn(htd).when(mockRegion).getTableDescriptor();
    Mockito.doReturn(hri).when(mockRegion).getRegionInfo();
    stores = new ArrayList<>();
    Mockito.doReturn(stores).when(mockRegion).getStores();
  }

  @Test
  public void testForceSplitRegionWithReference() throws IOException {
    htd.setMaxFileSize(1024L);
    // Add a store above the requisite size. Should split.
    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    // Act as if there's a reference file or some other reason it can't split.
    // This should prevent splitting even though it's big enough.
    Mockito.doReturn(false).when(mockStore).canSplit();
    stores.add(mockStore);

    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    ConstantSizeRegionSplitPolicy policy =
        (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    assertFalse(policy.shouldSplit());
    Mockito.doReturn(true).when(mockRegion).shouldForceSplit();
    assertFalse(policy.shouldSplit());

    Mockito.doReturn(false).when(mockRegion).shouldForceSplit();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      IncreasingToUpperBoundRegionSplitPolicy.class.getName());
    policy = (IncreasingToUpperBoundRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
    assertFalse(policy.shouldSplit());
    Mockito.doReturn(true).when(mockRegion).shouldForceSplit();
    assertFalse(policy.shouldSplit());
  }

  @Test
  public void testIncreasingToUpperBoundRegionSplitPolicy() throws IOException {
    // Configure IncreasingToUpperBoundRegionSplitPolicy as our split policy
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      IncreasingToUpperBoundRegionSplitPolicy.class.getName());
    // Now make it so the mock region has a RegionServerService that will
    // return 'online regions'.
    RegionServerServices rss = Mockito.mock(RegionServerServices.class);
    final List<HRegion> regions = new ArrayList<>();
    Mockito.doReturn(regions).when(rss).getRegions(TABLENAME);
    Mockito.when(mockRegion.getRegionServerServices()).thenReturn(rss);
    // Set max size for this 'table'.
    long maxSplitSize = 1024L;
    htd.setMaxFileSize(maxSplitSize);
    // Set flush size to 1/8.  IncreasingToUpperBoundRegionSplitPolicy
    // grows by the cube of the number of regions times flushsize each time.
    long flushSize = maxSplitSize/8;
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSize);
    htd.setMemStoreFlushSize(flushSize);
    // If RegionServerService with no regions in it -- 'online regions' == 0 --
    // then IncreasingToUpperBoundRegionSplitPolicy should act like a
    // ConstantSizePolicy
    IncreasingToUpperBoundRegionSplitPolicy policy =
      (IncreasingToUpperBoundRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    doConstantSizePolicyTests(policy);

    // Add a store in excess of split size.  Because there are "no regions"
    // on this server -- rss.getOnlineRegions is 0 -- then we should split
    // like a constantsizeregionsplitpolicy would
    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);
    // It should split
    assertTrue(policy.shouldSplit());

    // Now test that we increase our split size as online regions for a table
    // grows. With one region, split size should be flushsize.
    regions.add(mockRegion);
    Mockito.doReturn(flushSize).when(mockStore).getSize();
    // Should not split since store is flush size.
    assertFalse(policy.shouldSplit());
    // Set size of store to be > 2*flush size and we should split
    Mockito.doReturn(flushSize*2 + 1).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());
    // Add another region to the 'online regions' on this server and we should
    // now be no longer be splittable since split size has gone up.
    regions.add(mockRegion);
    assertFalse(policy.shouldSplit());
    // make sure its just over; verify it'll split
    Mockito.doReturn((long)(maxSplitSize * 1.25 + 1)).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());

    // Finally assert that even if loads of regions, we'll split at max size
    assertWithinJitter(maxSplitSize, policy.getSizeToCheck(1000));
    // Assert same is true if count of regions is zero.
    assertWithinJitter(maxSplitSize, policy.getSizeToCheck(0));
  }

  @Test
  public void testBusyRegionSplitPolicy() throws Exception {
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
        BusyRegionSplitPolicy.class.getName());
    conf.setLong("hbase.busy.policy.minAge", 1000000L);
    conf.setFloat("hbase.busy.policy.blockedRequests", 0.1f);

    RegionServerServices rss  = Mockito.mock(RegionServerServices.class);
    final List<HRegion> regions = new ArrayList<>();
    Mockito.doReturn(regions).when(rss).getRegions(TABLENAME);
    Mockito.when(mockRegion.getRegionServerServices()).thenReturn(rss);
    Mockito.when(mockRegion.getBlockedRequestsCount()).thenReturn(0L);
    Mockito.when(mockRegion.getWriteRequestsCount()).thenReturn(0L);


    BusyRegionSplitPolicy policy =
        (BusyRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);

    Mockito.when(mockRegion.getBlockedRequestsCount()).thenReturn(10L);
    Mockito.when(mockRegion.getWriteRequestsCount()).thenReturn(10L);
    // Not enough time since region came online
    assertFalse(policy.shouldSplit());


    // Reset min age for split to zero
    conf.setLong("hbase.busy.policy.minAge", 0L);
    // Aggregate over 500 ms periods
    conf.setLong("hbase.busy.policy.aggWindow", 500L);
    policy =
        (BusyRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    long start = EnvironmentEdgeManager.currentTime();
    Mockito.when(mockRegion.getBlockedRequestsCount()).thenReturn(10L);
    Mockito.when(mockRegion.getWriteRequestsCount()).thenReturn(20L);
    Thread.sleep(300);
    assertFalse(policy.shouldSplit());
    Mockito.when(mockRegion.getBlockedRequestsCount()).thenReturn(12L);
    Mockito.when(mockRegion.getWriteRequestsCount()).thenReturn(30L);
    Thread.sleep(2);
    // Enough blocked requests since last time, but aggregate blocked request
    // rate over last 500 ms is still low, because major portion of the window is constituted
    // by the previous zero blocked request period which lasted at least 300 ms off last 500 ms.
    if (EnvironmentEdgeManager.currentTime() - start < 500) {
      assertFalse(policy.shouldSplit());
    }
    Mockito.when(mockRegion.getBlockedRequestsCount()).thenReturn(14L);
    Mockito.when(mockRegion.getWriteRequestsCount()).thenReturn(40L);
    Thread.sleep(200);
    assertTrue(policy.shouldSplit());
  }

  private void assertWithinJitter(long maxSplitSize, long sizeToCheck) {
    assertTrue("Size greater than lower bound of jitter",
        (long)(maxSplitSize * 0.75) <= sizeToCheck);
    assertTrue("Size less than upper bound of jitter",
        (long)(maxSplitSize * 1.25) >= sizeToCheck);
  }

  @Test
  public void testCreateDefault() throws IOException {
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 1234L);

    // Using a default HTD, should pick up the file size from
    // configuration.
    ConstantSizeRegionSplitPolicy policy =
        (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(
            mockRegion, conf);
    assertWithinJitter(1234L, policy.getDesiredMaxFileSize());

    // If specified in HTD, should use that
    htd.setMaxFileSize(9999L);
    policy = (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(
        mockRegion, conf);
    assertWithinJitter(9999L, policy.getDesiredMaxFileSize());
  }

  /**
   * Test setting up a customized split policy
   */
  @Test
  public void testCustomPolicy() throws IOException {
    HTableDescriptor myHtd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    myHtd.setValue(HTableDescriptor.SPLIT_POLICY,
        KeyPrefixRegionSplitPolicy.class.getName());
    myHtd.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, String.valueOf(2));

    HRegion myMockRegion = Mockito.mock(HRegion.class);
    Mockito.doReturn(myHtd).when(myMockRegion).getTableDescriptor();
    Mockito.doReturn(stores).when(myMockRegion).getStores();

    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    Mockito.doReturn(Optional.of(Bytes.toBytes("abcd"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    KeyPrefixRegionSplitPolicy policy = (KeyPrefixRegionSplitPolicy) RegionSplitPolicy
        .create(myMockRegion, conf);

    assertEquals("ab", Bytes.toString(policy.getSplitPoint()));

    Mockito.doReturn(true).when(myMockRegion).shouldForceSplit();
    Mockito.doReturn(Bytes.toBytes("efgh")).when(myMockRegion)
        .getExplicitSplitPoint();

    policy = (KeyPrefixRegionSplitPolicy) RegionSplitPolicy
        .create(myMockRegion, conf);

    assertEquals("ef", Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testConstantSizePolicy() throws IOException {
    htd.setMaxFileSize(1024L);
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    doConstantSizePolicyTests(policy);
  }

  /**
   * Run through tests for a ConstantSizeRegionSplitPolicy
   * @param policy
   */
  private void doConstantSizePolicyTests(final ConstantSizeRegionSplitPolicy policy) {
    // For no stores, should not split
    assertFalse(policy.shouldSplit());

    // Add a store above the requisite size. Should split.
    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);

    assertTrue(policy.shouldSplit());

    // Act as if there's a reference file or some other reason it can't split.
    // This should prevent splitting even though it's big enough.
    Mockito.doReturn(false).when(mockStore).canSplit();
    assertFalse(policy.shouldSplit());

    // Reset splittability after above
    Mockito.doReturn(true).when(mockStore).canSplit();

    // Set to a small size but turn on forceSplit. Should result in a split.
    Mockito.doReturn(true).when(mockRegion).shouldForceSplit();
    Mockito.doReturn(100L).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());

    // Turn off forceSplit, should not split
    Mockito.doReturn(false).when(mockRegion).shouldForceSplit();
    assertFalse(policy.shouldSplit());

    // Clear families we added above
    stores.clear();
  }

  @Test
  public void testGetSplitPoint() throws IOException {
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);

    // For no stores, should not split
    assertFalse(policy.shouldSplit());
    assertNull(policy.getSplitPoint());

    // Add a store above the requisite size. Should split.
    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    Mockito.doReturn(Optional.of(Bytes.toBytes("store 1 split"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    assertEquals("store 1 split",
        Bytes.toString(policy.getSplitPoint()));

    // Add a bigger store. The split point should come from that one
    HStore mockStore2 = Mockito.mock(HStore.class);
    Mockito.doReturn(4000L).when(mockStore2).getSize();
    Mockito.doReturn(true).when(mockStore2).canSplit();
    Mockito.doReturn(Optional.of(Bytes.toBytes("store 2 split"))).when(mockStore2).getSplitPoint();
    stores.add(mockStore2);

    assertEquals("store 2 split",
        Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testDelimitedKeyPrefixRegionSplitPolicy() throws IOException {
    HTableDescriptor myHtd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    myHtd.setValue(HTableDescriptor.SPLIT_POLICY,
        DelimitedKeyPrefixRegionSplitPolicy.class.getName());
    myHtd.setValue(DelimitedKeyPrefixRegionSplitPolicy.DELIMITER_KEY, ",");

    HRegion myMockRegion = Mockito.mock(HRegion.class);
    Mockito.doReturn(myHtd).when(myMockRegion).getTableDescriptor();
    Mockito.doReturn(stores).when(myMockRegion).getStores();

    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    Mockito.doReturn(Optional.of(Bytes.toBytes("ab,cd"))).when(mockStore).getSplitPoint();
    stores.add(mockStore);

    DelimitedKeyPrefixRegionSplitPolicy policy = (DelimitedKeyPrefixRegionSplitPolicy) RegionSplitPolicy
        .create(myMockRegion, conf);

    assertEquals("ab", Bytes.toString(policy.getSplitPoint()));

    Mockito.doReturn(true).when(myMockRegion).shouldForceSplit();
    Mockito.doReturn(Bytes.toBytes("efg,h")).when(myMockRegion)
        .getExplicitSplitPoint();

    policy = (DelimitedKeyPrefixRegionSplitPolicy) RegionSplitPolicy
        .create(myMockRegion, conf);

    assertEquals("efg", Bytes.toString(policy.getSplitPoint()));

    Mockito.doReturn(Bytes.toBytes("ijk")).when(myMockRegion)
    .getExplicitSplitPoint();
    assertEquals("ijk", Bytes.toString(policy.getSplitPoint()));
  }

  @Test
  public void testConstantSizePolicyWithJitter() throws IOException {
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    htd.setMaxFileSize(Long.MAX_VALUE);
    boolean positiveJitter = false;
    ConstantSizeRegionSplitPolicy policy = null;
    while (!positiveJitter) {
      policy = (ConstantSizeRegionSplitPolicy) RegionSplitPolicy.create(mockRegion, conf);
      positiveJitter = policy.positiveJitterRate();
    }
    // add a store
    HStore mockStore = Mockito.mock(HStore.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    stores.add(mockStore);
    // Jitter shouldn't cause overflow when HTableDescriptor.MAX_FILESIZE set to Long.MAX_VALUE
    assertFalse(policy.shouldSplit());
  }

}
