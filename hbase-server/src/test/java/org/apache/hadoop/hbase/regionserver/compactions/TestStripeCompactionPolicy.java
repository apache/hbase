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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StripeStoreFileManager;
import org.apache.hadoop.hbase.regionserver.StripeStoreFlusher;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy.StripeInformationProvider;
import org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.StoreFileWritersCapture;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
@Category({RegionServerTests.class, SmallTests.class})
public class TestStripeCompactionPolicy {
  private static final byte[] KEY_A = Bytes.toBytes("aaa");
  private static final byte[] KEY_B = Bytes.toBytes("bbb");
  private static final byte[] KEY_C = Bytes.toBytes("ccc");
  private static final byte[] KEY_D = Bytes.toBytes("ddd");
  private static final byte[] KEY_E = Bytes.toBytes("eee");
  private static final KeyValue KV_A = new KeyValue(KEY_A, 0L);
  private static final KeyValue KV_B = new KeyValue(KEY_B, 0L);
  private static final KeyValue KV_C = new KeyValue(KEY_C, 0L);
  private static final KeyValue KV_D = new KeyValue(KEY_D, 0L);
  private static final KeyValue KV_E = new KeyValue(KEY_E, 0L);


  private static long defaultSplitSize = 18;
  private static float defaultSplitCount = 1.8F;
  private final static int defaultInitialCount = 1;
  private static long defaultTtl = 1000 * 1000;

  @Parameters(name = "{index}: usePrivateReaders={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @Parameter
  public boolean usePrivateReaders;
  @Test
  public void testNoStripesFromFlush() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(StripeStoreConfig.FLUSH_TO_L0_KEY, true);
    StripeCompactionPolicy policy = createPolicy(conf);
    StripeInformationProvider si = createStripesL0Only(0, 0);

    KeyValue[] input = new KeyValue[] { KV_A, KV_B, KV_C, KV_D, KV_E };
    KeyValue[][] expected = new KeyValue[][] { input };
    verifyFlush(policy, si, input, expected, null);
  }

  @Test
  public void testOldStripesFromFlush() throws Exception {
    StripeCompactionPolicy policy = createPolicy(HBaseConfiguration.create());
    StripeInformationProvider si = createStripes(0, KEY_C, KEY_D);

    KeyValue[] input = new KeyValue[] { KV_B, KV_C, KV_C, KV_D, KV_E };
    KeyValue[][] expected = new KeyValue[][] { new KeyValue[] { KV_B },
        new KeyValue[] { KV_C, KV_C }, new KeyValue[] {  KV_D, KV_E } };
    verifyFlush(policy, si, input, expected, new byte[][] { OPEN_KEY, KEY_C, KEY_D, OPEN_KEY });
  }

  @Test
  public void testNewStripesFromFlush() throws Exception {
    StripeCompactionPolicy policy = createPolicy(HBaseConfiguration.create());
    StripeInformationProvider si = createStripesL0Only(0, 0);
    KeyValue[] input = new KeyValue[] { KV_B, KV_C, KV_C, KV_D, KV_E };
    // Starts with one stripe; unlike flush results, must have metadata
    KeyValue[][] expected = new KeyValue[][] { input };
    verifyFlush(policy, si, input, expected, new byte[][] { OPEN_KEY, OPEN_KEY });
  }

  @Test
  public void testSingleStripeCompaction() throws Exception {
    // Create a special policy that only compacts single stripes, using standard methods.
    Configuration conf = HBaseConfiguration.create();
    // Test depends on this not being set to pass.  Default breaks test.  TODO: Revisit.
    conf.unset("hbase.hstore.compaction.min.size");
    conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.0F);
    conf.setInt(StripeStoreConfig.MIN_FILES_KEY, 3);
    conf.setInt(StripeStoreConfig.MAX_FILES_KEY, 4);
    conf.setLong(StripeStoreConfig.SIZE_TO_SPLIT_KEY, 1000); // make sure the are no splits
    StoreConfigInformation sci = mock(StoreConfigInformation.class);
    StripeStoreConfig ssc = new StripeStoreConfig(conf, sci);
    StripeCompactionPolicy policy = new StripeCompactionPolicy(conf, sci, ssc) {
      @Override
      public StripeCompactionRequest selectCompaction(StripeInformationProvider si,
          List<StoreFile> filesCompacting, boolean isOffpeak) throws IOException {
        if (!filesCompacting.isEmpty()) return null;
        return selectSingleStripeCompaction(si, false, false, isOffpeak);
      }

      @Override
      public boolean needsCompactions(
          StripeInformationProvider si, List<StoreFile> filesCompacting) {
        if (!filesCompacting.isEmpty()) return false;
        return needsSingleStripeCompaction(si);
      }
    };

    // No compaction due to min files or ratio
    StripeInformationProvider si = createStripesWithSizes(0, 0,
        new Long[] { 2L }, new Long[] { 3L, 3L }, new Long[] { 5L, 1L });
    verifyNoCompaction(policy, si);
    // No compaction due to min files or ratio - will report needed, but not do any.
    si = createStripesWithSizes(0, 0,
        new Long[] { 2L }, new Long[] { 3L, 3L }, new Long[] { 5L, 1L, 1L });
    assertNull(policy.selectCompaction(si, al(), false));
    assertTrue(policy.needsCompactions(si, al()));
    // One stripe has possible compaction
    si = createStripesWithSizes(0, 0,
        new Long[] { 2L }, new Long[] { 3L, 3L }, new Long[] { 5L, 4L, 3L });
    verifySingleStripeCompaction(policy, si, 2, null);
    // Several stripes have possible compactions; choose best quality (removes most files)
    si = createStripesWithSizes(0, 0,
        new Long[] { 3L, 2L, 2L }, new Long[] { 2L, 2L, 1L }, new Long[] { 3L, 2L, 2L, 1L });
    verifySingleStripeCompaction(policy, si, 2, null);
    si = createStripesWithSizes(0, 0,
        new Long[] { 5L }, new Long[] { 3L, 2L, 2L, 1L }, new Long[] { 3L, 2L, 2L });
    verifySingleStripeCompaction(policy, si, 1, null);
    // Or with smallest files, if the count is the same 
    si = createStripesWithSizes(0, 0,
        new Long[] { 3L, 3L, 3L }, new Long[] { 3L, 1L, 2L }, new Long[] { 3L, 2L, 2L });
    verifySingleStripeCompaction(policy, si, 1, null);
    // Verify max count is respected.
    si = createStripesWithSizes(0, 0, new Long[] { 5L }, new Long[] { 5L, 4L, 4L, 4L, 4L });
    List<StoreFile> sfs = si.getStripes().get(1).subList(1, 5);
    verifyCompaction(policy, si, sfs, null, 1, null, si.getStartRow(1), si.getEndRow(1), true);
    // Verify ratio is applied.
    si = createStripesWithSizes(0, 0, new Long[] { 5L }, new Long[] { 50L, 4L, 4L, 4L, 4L });
    sfs = si.getStripes().get(1).subList(1, 5);
    verifyCompaction(policy, si, sfs, null, 1, null, si.getStartRow(1), si.getEndRow(1), true);
  }

  @Test
  public void testWithParallelCompaction() throws Exception {
    // TODO: currently only one compaction at a time per store is allowed. If this changes,
    //       the appropriate file exclusion testing would need to be done in respective tests.
    assertNull(createPolicy(HBaseConfiguration.create()).selectCompaction(
        mock(StripeInformationProvider.class), al(createFile()), false));
  }

  @Test
  public void testWithReferences() throws Exception {
    StripeCompactionPolicy policy = createPolicy(HBaseConfiguration.create());
    StripeCompactor sc = mock(StripeCompactor.class);
    StoreFile ref = createFile();
    when(ref.isReference()).thenReturn(true);
    StripeInformationProvider si = mock(StripeInformationProvider.class);
    Collection<StoreFile> sfs = al(ref, createFile());
    when(si.getStorefiles()).thenReturn(sfs);

    assertTrue(policy.needsCompactions(si, al()));
    StripeCompactionPolicy.StripeCompactionRequest scr = policy.selectCompaction(si, al(), false);
    assertEquals(si.getStorefiles(), scr.getRequest().getFiles());
    scr.execute(sc, NoLimitThroughputController.INSTANCE, null);
    verify(sc, only()).compact(eq(scr.getRequest()), anyInt(), anyLong(), aryEq(OPEN_KEY),
      aryEq(OPEN_KEY), aryEq(OPEN_KEY), aryEq(OPEN_KEY),
      any(NoLimitThroughputController.class), any(User.class));
  }

  @Test
  public void testInitialCountFromL0() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(StripeStoreConfig.MIN_FILES_L0_KEY, 2);
    StripeCompactionPolicy policy = createPolicy(
        conf, defaultSplitSize, defaultSplitCount, 2, false);
    StripeCompactionPolicy.StripeInformationProvider si = createStripesL0Only(3, 8);
    verifyCompaction(policy, si, si.getStorefiles(), true, 2, 12L, OPEN_KEY, OPEN_KEY, true);
    si = createStripesL0Only(3, 10); // If result would be too large, split into smaller parts.
    verifyCompaction(policy, si, si.getStorefiles(), true, 3, 10L, OPEN_KEY, OPEN_KEY, true);
    policy = createPolicy(conf, defaultSplitSize, defaultSplitCount, 6, false);
    verifyCompaction(policy, si, si.getStorefiles(), true, 6, 5L, OPEN_KEY, OPEN_KEY, true);
  }

  @Test
  public void testExistingStripesFromL0() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(StripeStoreConfig.MIN_FILES_L0_KEY, 3);
    StripeCompactionPolicy.StripeInformationProvider si = createStripes(3, KEY_A);
    verifyCompaction(
        createPolicy(conf), si, si.getLevel0Files(), null, null, si.getStripeBoundaries());
  }

  @Test
  public void testNothingToCompactFromL0() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(StripeStoreConfig.MIN_FILES_L0_KEY, 4);
    StripeCompactionPolicy.StripeInformationProvider si = createStripesL0Only(3, 10);
    StripeCompactionPolicy policy = createPolicy(conf);
    verifyNoCompaction(policy, si);

    si = createStripes(3, KEY_A);
    verifyNoCompaction(policy, si);
  }

  @Test
  public void testSplitOffStripe() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Test depends on this not being set to pass.  Default breaks test.  TODO: Revisit.
    conf.unset("hbase.hstore.compaction.min.size");
    // First test everything with default split count of 2, then split into more.
    conf.setInt(StripeStoreConfig.MIN_FILES_KEY, 2);
    Long[] toSplit = new Long[] { defaultSplitSize - 2, 1L, 1L };
    Long[] noSplit = new Long[] { defaultSplitSize - 2, 1L };
    long splitTargetSize = (long)(defaultSplitSize / defaultSplitCount);
    // Don't split if not eligible for compaction.
    StripeCompactionPolicy.StripeInformationProvider si =
        createStripesWithSizes(0, 0, new Long[] { defaultSplitSize - 2, 2L });
    assertNull(createPolicy(conf).selectCompaction(si, al(), false));
    // Make sure everything is eligible.
    conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 500f);
    StripeCompactionPolicy policy = createPolicy(conf);
    verifyWholeStripesCompaction(policy, si, 0, 0, null, 2, splitTargetSize);
    // Add some extra stripes...
    si = createStripesWithSizes(0, 0, noSplit, noSplit, toSplit);
    verifyWholeStripesCompaction(policy, si, 2, 2, null, 2, splitTargetSize);
    // In the middle.
    si = createStripesWithSizes(0, 0, noSplit, toSplit, noSplit);
    verifyWholeStripesCompaction(policy, si, 1, 1, null, 2, splitTargetSize);
    // No split-off with different config (larger split size).
    // However, in this case some eligible stripe will just be compacted alone.
    StripeCompactionPolicy specPolicy = createPolicy(
        conf, defaultSplitSize + 1, defaultSplitCount, defaultInitialCount, false);
    verifySingleStripeCompaction(specPolicy, si, 1, null);
  }

  @Test
  public void testSplitOffStripeOffPeak() throws Exception {
    // for HBASE-11439
    Configuration conf = HBaseConfiguration.create();

    // Test depends on this not being set to pass.  Default breaks test.  TODO: Revisit.
    conf.unset("hbase.hstore.compaction.min.size");

    conf.setInt(StripeStoreConfig.MIN_FILES_KEY, 2);
    // Select the last 2 files.
    StripeCompactionPolicy.StripeInformationProvider si =
        createStripesWithSizes(0, 0, new Long[] { defaultSplitSize - 2, 1L, 1L });
    assertEquals(2, createPolicy(conf).selectCompaction(si, al(), false).getRequest().getFiles()
        .size());
    // Make sure everything is eligible in offpeak.
    conf.setFloat("hbase.hstore.compaction.ratio.offpeak", 500f);
    assertEquals(3, createPolicy(conf).selectCompaction(si, al(), true).getRequest().getFiles()
        .size());
  }

  @Test
  public void testSplitOffStripeDropDeletes() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(StripeStoreConfig.MIN_FILES_KEY, 2);
    StripeCompactionPolicy policy = createPolicy(conf);
    Long[] toSplit = new Long[] { defaultSplitSize / 2, defaultSplitSize / 2 };
    Long[] noSplit = new Long[] { 1L };
    long splitTargetSize = (long)(defaultSplitSize / defaultSplitCount);

    // Verify the deletes can be dropped if there are no L0 files.
    StripeCompactionPolicy.StripeInformationProvider si =
        createStripesWithSizes(0, 0, noSplit, toSplit);
    verifyWholeStripesCompaction(policy, si, 1, 1,    true, null, splitTargetSize);
    // But cannot be dropped if there are.
    si = createStripesWithSizes(2, 2, noSplit, toSplit);
    verifyWholeStripesCompaction(policy, si, 1, 1,    false, null, splitTargetSize);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMergeExpiredFiles() throws Exception {
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    long now = defaultTtl + 2;
    edge.setValue(now);
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      StoreFile expiredFile = createFile(), notExpiredFile = createFile();
      when(expiredFile.getReader().getMaxTimestamp()).thenReturn(now - defaultTtl - 1);
      when(notExpiredFile.getReader().getMaxTimestamp()).thenReturn(now - defaultTtl + 1);
      List<StoreFile> expired = Lists.newArrayList(expiredFile, expiredFile);
      List<StoreFile> notExpired = Lists.newArrayList(notExpiredFile, notExpiredFile);
      List<StoreFile> mixed = Lists.newArrayList(expiredFile, notExpiredFile);

      StripeCompactionPolicy policy = createPolicy(HBaseConfiguration.create(),
          defaultSplitSize, defaultSplitCount, defaultInitialCount, true);
      // Merge expired if there are eligible stripes.
      StripeCompactionPolicy.StripeInformationProvider si =
          createStripesWithFiles(expired, expired, expired);
      verifyWholeStripesCompaction(policy, si, 0, 2, null, 1, Long.MAX_VALUE, false);
      // Don't merge if nothing expired.
      si = createStripesWithFiles(notExpired, notExpired, notExpired);
      assertNull(policy.selectCompaction(si, al(), false));
      // Merge one expired stripe with next.
      si = createStripesWithFiles(notExpired, expired, notExpired);
      verifyWholeStripesCompaction(policy, si, 1, 2, null, 1, Long.MAX_VALUE, false);
      // Merge the biggest run out of multiple options.
      // Merge one expired stripe with next.
      si = createStripesWithFiles(notExpired, expired, notExpired, expired, expired, notExpired);
      verifyWholeStripesCompaction(policy, si, 3, 4, null, 1, Long.MAX_VALUE, false);
      // Stripe with a subset of expired files is not merged.
      si = createStripesWithFiles(expired, expired, notExpired, expired, mixed);
      verifyWholeStripesCompaction(policy, si, 0, 1, null, 1, Long.MAX_VALUE, false);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMergeExpiredStripes() throws Exception {
    // HBASE-11397
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    long now = defaultTtl + 2;
    edge.setValue(now);
    EnvironmentEdgeManager.injectEdge(edge);
    try {
      StoreFile expiredFile = createFile(), notExpiredFile = createFile();
      when(expiredFile.getReader().getMaxTimestamp()).thenReturn(now - defaultTtl - 1);
      when(notExpiredFile.getReader().getMaxTimestamp()).thenReturn(now - defaultTtl + 1);
      List<StoreFile> expired = Lists.newArrayList(expiredFile, expiredFile);
      List<StoreFile> notExpired = Lists.newArrayList(notExpiredFile, notExpiredFile);

      StripeCompactionPolicy policy =
          createPolicy(HBaseConfiguration.create(), defaultSplitSize, defaultSplitCount,
            defaultInitialCount, true);

      // Merge all three expired stripes into one.
      StripeCompactionPolicy.StripeInformationProvider si =
          createStripesWithFiles(expired, expired, expired);
      verifyMergeCompatcion(policy, si, 0, 2);

      // Merge two adjacent expired stripes into one.
      si = createStripesWithFiles(notExpired, expired, notExpired, expired, expired, notExpired);
      verifyMergeCompatcion(policy, si, 3, 4);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @SuppressWarnings("unchecked")
  private static StripeCompactionPolicy.StripeInformationProvider createStripesWithFiles(
      List<StoreFile>... stripeFiles) throws Exception {
    return createStripesWithFiles(createBoundaries(stripeFiles.length),
        Lists.newArrayList(stripeFiles), new ArrayList<StoreFile>());
  }

  @Test
  public void testSingleStripeDropDeletes() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Test depends on this not being set to pass.  Default breaks test.  TODO: Revisit.
    conf.unset("hbase.hstore.compaction.min.size");
    StripeCompactionPolicy policy = createPolicy(conf);
    // Verify the deletes can be dropped if there are no L0 files.
    Long[][] stripes = new Long[][] { new Long[] { 3L, 2L, 2L, 2L }, new Long[] { 6L } };
    StripeInformationProvider si = createStripesWithSizes(0, 0, stripes);
    verifySingleStripeCompaction(policy, si, 0, true);
    // But cannot be dropped if there are.
    si = createStripesWithSizes(2, 2, stripes);
    verifySingleStripeCompaction(policy, si, 0, false);
    // Unless there are enough to cause L0 compaction.
    si = createStripesWithSizes(6, 2, stripes);
    ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
    sfs.addSublist(si.getLevel0Files());
    sfs.addSublist(si.getStripes().get(0));
    verifyCompaction(
        policy, si, sfs, si.getStartRow(0), si.getEndRow(0), si.getStripeBoundaries());
    // If we cannot actually compact all files in some stripe, L0 is chosen.
    si = createStripesWithSizes(6, 2,
        new Long[][] { new Long[] { 10L, 1L, 1L, 1L, 1L }, new Long[] { 12L } });
    verifyCompaction(policy, si, si.getLevel0Files(), null, null, si.getStripeBoundaries());
    // even if L0 has no file
    // if all files of stripe aren't selected, delete must not be dropped.
    stripes = new Long[][] { new Long[] { 100L, 3L, 2L, 2L, 2L }, new Long[] { 6L } };
    si = createStripesWithSizes(0, 0, stripes);
    List<StoreFile> compact_file = new ArrayList<StoreFile>();
    Iterator<StoreFile> iter = si.getStripes().get(0).listIterator(1);
    while (iter.hasNext()) {
        compact_file.add(iter.next());
    }
    verifyCompaction(policy, si, compact_file, false, 1, null, si.getStartRow(0), si.getEndRow(0), true);
  }

  /********* HELPER METHODS ************/
  private static StripeCompactionPolicy createPolicy(
      Configuration conf) throws Exception {
    return createPolicy(conf, defaultSplitSize, defaultSplitCount, defaultInitialCount, false);
  }

  private static StripeCompactionPolicy createPolicy(Configuration conf,
      long splitSize, float splitCount, int initialCount, boolean hasTtl) throws Exception {
    conf.setLong(StripeStoreConfig.SIZE_TO_SPLIT_KEY, splitSize);
    conf.setFloat(StripeStoreConfig.SPLIT_PARTS_KEY, splitCount);
    conf.setInt(StripeStoreConfig.INITIAL_STRIPE_COUNT_KEY, initialCount);
    StoreConfigInformation sci = mock(StoreConfigInformation.class);
    when(sci.getStoreFileTtl()).thenReturn(hasTtl ? defaultTtl : Long.MAX_VALUE);
    StripeStoreConfig ssc = new StripeStoreConfig(conf, sci);
    return new StripeCompactionPolicy(conf, sci, ssc);
  }

  private static ArrayList<StoreFile> al(StoreFile... sfs) {
    return new ArrayList<StoreFile>(Arrays.asList(sfs));
  }

  private void verifyMergeCompatcion(StripeCompactionPolicy policy, StripeInformationProvider si,
      int from, int to) throws Exception {
    StripeCompactionPolicy.StripeCompactionRequest scr = policy.selectCompaction(si, al(), false);
    Collection<StoreFile> sfs = getAllFiles(si, from, to);
    verifyCollectionsEqual(sfs, scr.getRequest().getFiles());

    // All the Stripes are expired, so the Compactor will not create any Writers. We need to create
    // an empty file to preserve metadata
    StripeCompactor sc = createCompactor();
    List<Path> paths = scr.execute(sc, NoLimitThroughputController.INSTANCE, null);
    assertEquals(1, paths.size());
  }

  /**
   * Verify the compaction that includes several entire stripes.
   * @param policy Policy to test.
   * @param si Stripe information pre-set with stripes to test.
   * @param from Starting stripe.
   * @param to Ending stripe (inclusive).
   * @param dropDeletes Whether to drop deletes from compaction range.
   * @param count Expected # of resulting stripes, null if not checked.
   * @param size Expected target stripe size, null if not checked.
   */
  private void verifyWholeStripesCompaction(StripeCompactionPolicy policy,
      StripeInformationProvider si, int from, int to, Boolean dropDeletes,
      Integer count, Long size, boolean needsCompaction) throws IOException {
    verifyCompaction(policy, si, getAllFiles(si, from, to), dropDeletes,
        count, size, si.getStartRow(from), si.getEndRow(to), needsCompaction);
  }

  private void verifyWholeStripesCompaction(StripeCompactionPolicy policy,
      StripeInformationProvider si, int from, int to, Boolean dropDeletes,
      Integer count, Long size) throws IOException {
    verifyWholeStripesCompaction(policy, si, from, to, dropDeletes, count, size, true);
  }

  private void verifySingleStripeCompaction(StripeCompactionPolicy policy,
      StripeInformationProvider si, int index, Boolean dropDeletes) throws IOException {
    verifyWholeStripesCompaction(policy, si, index, index, dropDeletes, 1, null, true);
  }

  /**
   * Verify no compaction is needed or selected.
   * @param policy Policy to test.
   * @param si Stripe information pre-set with stripes to test.
   */
  private void verifyNoCompaction(
      StripeCompactionPolicy policy, StripeInformationProvider si) throws IOException {
    assertNull(policy.selectCompaction(si, al(), false));
    assertFalse(policy.needsCompactions(si, al()));
  }

  /**
   * Verify arbitrary compaction.
   * @param policy Policy to test.
   * @param si Stripe information pre-set with stripes to test.
   * @param sfs Files that should be compacted.
   * @param dropDeletesFrom Row from which to drop deletes.
   * @param dropDeletesTo Row to which to drop deletes.
   * @param boundaries Expected target stripe boundaries.
   */
  private void verifyCompaction(StripeCompactionPolicy policy, StripeInformationProvider si,
      Collection<StoreFile> sfs, byte[] dropDeletesFrom, byte[] dropDeletesTo,
      final List<byte[]> boundaries) throws Exception {
    StripeCompactor sc = mock(StripeCompactor.class);
    assertTrue(policy.needsCompactions(si, al()));
    StripeCompactionPolicy.StripeCompactionRequest scr = policy.selectCompaction(si, al(), false);
    verifyCollectionsEqual(sfs, scr.getRequest().getFiles());
    scr.execute(sc, NoLimitThroughputController.INSTANCE, null);
    verify(sc, times(1)).compact(eq(scr.getRequest()), argThat(new ArgumentMatcher<List<byte[]>>() {
      @Override
      public boolean matches(Object argument) {
        @SuppressWarnings("unchecked")
        List<byte[]> other = (List<byte[]>) argument;
        if (other.size() != boundaries.size()) return false;
        for (int i = 0; i < other.size(); ++i) {
          if (!Bytes.equals(other.get(i), boundaries.get(i))) return false;
        }
        return true;
      }
    }), dropDeletesFrom == null ? isNull(byte[].class) : aryEq(dropDeletesFrom),
      dropDeletesTo == null ? isNull(byte[].class) : aryEq(dropDeletesTo),
      any(NoLimitThroughputController.class), any(User.class));
  }

  /**
   * Verify arbitrary compaction.
   * @param policy Policy to test.
   * @param si Stripe information pre-set with stripes to test.
   * @param sfs Files that should be compacted.
   * @param dropDeletes Whether to drop deletes from compaction range.
   * @param count Expected # of resulting stripes, null if not checked.
   * @param size Expected target stripe size, null if not checked.
   * @param start Left boundary of the compaction.
   * @param righr Right boundary of the compaction.
   */
  private void verifyCompaction(StripeCompactionPolicy policy, StripeInformationProvider si,
      Collection<StoreFile> sfs, Boolean dropDeletes, Integer count, Long size,
      byte[] start, byte[] end, boolean needsCompaction) throws IOException {
    StripeCompactor sc = mock(StripeCompactor.class);
    assertTrue(!needsCompaction || policy.needsCompactions(si, al()));
    StripeCompactionPolicy.StripeCompactionRequest scr = policy.selectCompaction(si, al(), false);
    verifyCollectionsEqual(sfs, scr.getRequest().getFiles());
    scr.execute(sc, NoLimitThroughputController.INSTANCE, null);
    verify(sc, times(1)).compact(eq(scr.getRequest()),
      count == null ? anyInt() : eq(count.intValue()),
      size == null ? anyLong() : eq(size.longValue()), aryEq(start), aryEq(end),
      dropDeletesMatcher(dropDeletes, start), dropDeletesMatcher(dropDeletes, end),
      any(NoLimitThroughputController.class), any(User.class));
  }

  /** Verify arbitrary flush. */
  protected void verifyFlush(StripeCompactionPolicy policy, StripeInformationProvider si,
      KeyValue[] input, KeyValue[][] expected, byte[][] boundaries) throws IOException {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StripeStoreFlusher.StripeFlushRequest req = policy.selectFlush(CellComparator.COMPARATOR, si,
      input.length);
    StripeMultiFileWriter mw = req.createWriter();
    mw.init(null, writers);
    for (KeyValue kv : input) {
      mw.append(kv);
    }
    boolean hasMetadata = boundaries != null;
    mw.commitWriters(0, false);
    writers.verifyKvs(expected, true, hasMetadata);
    if (hasMetadata) {
      writers.verifyBoundaries(boundaries);
    }
  }


  private byte[] dropDeletesMatcher(Boolean dropDeletes, byte[] value) {
    return dropDeletes == null ? any(byte[].class)
            : (dropDeletes.booleanValue() ? aryEq(value) : isNull(byte[].class));
  }

  private void verifyCollectionsEqual(Collection<StoreFile> sfs, Collection<StoreFile> scr) {
    // Dumb.
    assertEquals(sfs.size(), scr.size());
    assertTrue(scr.containsAll(sfs));
  }

  private static List<StoreFile> getAllFiles(
      StripeInformationProvider si, int fromStripe, int toStripe) {
    ArrayList<StoreFile> expected = new ArrayList<StoreFile>();
    for (int i = fromStripe; i <= toStripe; ++i) {
      expected.addAll(si.getStripes().get(i));
    }
    return expected;
  }

  /**
   * @param l0Count Number of L0 files.
   * @param boundaries Target boundaries.
   * @return Mock stripes.
   */
  private static StripeInformationProvider createStripes(
      int l0Count, byte[]... boundaries) throws Exception {
    List<Long> l0Sizes = new ArrayList<Long>();
    for (int i = 0; i < l0Count; ++i) {
      l0Sizes.add(5L);
    }
    List<List<Long>> sizes = new ArrayList<List<Long>>();
    for (int i = 0; i <= boundaries.length; ++i) {
      sizes.add(Arrays.asList(Long.valueOf(5)));
    }
    return createStripes(Arrays.asList(boundaries), sizes, l0Sizes);
  }

  /**
   * @param l0Count Number of L0 files.
   * @param l0Size Size of each file.
   * @return Mock stripes.
   */
  private static StripeInformationProvider createStripesL0Only(
      int l0Count, long l0Size) throws Exception {
    List<Long> l0Sizes = new ArrayList<Long>();
    for (int i = 0; i < l0Count; ++i) {
      l0Sizes.add(l0Size);
    }
    return createStripes(null, new ArrayList<List<Long>>(), l0Sizes);
  }

  /**
   * @param l0Count Number of L0 files.
   * @param l0Size Size of each file.
   * @param sizes Sizes of the files; each sub-array representing a stripe.
   * @return Mock stripes.
   */
  private static StripeInformationProvider createStripesWithSizes(
      int l0Count, long l0Size, Long[]... sizes) throws Exception {
    ArrayList<List<Long>> sizeList = new ArrayList<List<Long>>();
    for (Long[] size : sizes) {
      sizeList.add(Arrays.asList(size));
    }
    return createStripesWithSizes(l0Count, l0Size, sizeList);
  }

  private static StripeInformationProvider createStripesWithSizes(
      int l0Count, long l0Size, List<List<Long>> sizes) throws Exception {
    List<byte[]> boundaries = createBoundaries(sizes.size());
    List<Long> l0Sizes = new ArrayList<Long>();
    for (int i = 0; i < l0Count; ++i) {
      l0Sizes.add(l0Size);
    }
    return createStripes(boundaries, sizes, l0Sizes);
  }

  private static List<byte[]> createBoundaries(int stripeCount) {
    byte[][] keys = new byte[][] { KEY_A, KEY_B, KEY_C, KEY_D, KEY_E };
    assert stripeCount <= keys.length + 1;
    List<byte[]> boundaries = new ArrayList<byte[]>();
    boundaries.addAll(Arrays.asList(keys).subList(0, stripeCount - 1));
    return boundaries;
  }

  private static StripeInformationProvider createStripes(List<byte[]> boundaries,
      List<List<Long>> stripeSizes, List<Long> l0Sizes) throws Exception {
    List<List<StoreFile>> stripeFiles = new ArrayList<List<StoreFile>>(stripeSizes.size());
    for (List<Long> sizes : stripeSizes) {
      List<StoreFile> sfs = new ArrayList<StoreFile>();
      for (Long size : sizes) {
        sfs.add(createFile(size));
      }
      stripeFiles.add(sfs);
    }
    List<StoreFile> l0Files = new ArrayList<StoreFile>();
    for (Long size : l0Sizes) {
      l0Files.add(createFile(size));
    }
    return createStripesWithFiles(boundaries, stripeFiles, l0Files);
  }

  /**
   * This method actually does all the work.
   */
  private static StripeInformationProvider createStripesWithFiles(List<byte[]> boundaries,
      List<List<StoreFile>> stripeFiles, List<StoreFile> l0Files) throws Exception {
    ArrayList<ImmutableList<StoreFile>> stripes = new ArrayList<ImmutableList<StoreFile>>();
    ArrayList<byte[]> boundariesList = new ArrayList<byte[]>();
    StripeInformationProvider si = mock(StripeInformationProvider.class);
    if (!stripeFiles.isEmpty()) {
      assert stripeFiles.size() == (boundaries.size() + 1);
      boundariesList.add(OPEN_KEY);
      for (int i = 0; i <= boundaries.size(); ++i) {
        byte[] startKey = ((i == 0) ? OPEN_KEY : boundaries.get(i - 1));
        byte[] endKey = ((i == boundaries.size()) ? OPEN_KEY : boundaries.get(i));
        boundariesList.add(endKey);
        for (StoreFile sf : stripeFiles.get(i)) {
          setFileStripe(sf, startKey, endKey);
        }
        stripes.add(ImmutableList.copyOf(stripeFiles.get(i)));
        when(si.getStartRow(eq(i))).thenReturn(startKey);
        when(si.getEndRow(eq(i))).thenReturn(endKey);
      }
    }
    ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
    sfs.addAllSublists(stripes);
    sfs.addSublist(l0Files);
    when(si.getStorefiles()).thenReturn(sfs);
    when(si.getStripes()).thenReturn(stripes);
    when(si.getStripeBoundaries()).thenReturn(boundariesList);
    when(si.getStripeCount()).thenReturn(stripes.size());
    when(si.getLevel0Files()).thenReturn(l0Files);
    return si;
  }

  private static StoreFile createFile(long size) throws Exception {
    StoreFile sf = mock(StoreFile.class);
    when(sf.getPath()).thenReturn(new Path("moo"));
    StoreFile.Reader r = mock(StoreFile.Reader.class);
    when(r.getEntries()).thenReturn(size);
    when(r.length()).thenReturn(size);
    when(r.getBloomFilterType()).thenReturn(BloomType.NONE);
    when(r.getHFileReader()).thenReturn(mock(HFile.Reader.class));
    when(r.getStoreFileScanner(anyBoolean(), anyBoolean(), anyBoolean(), anyLong())).thenReturn(
      mock(StoreFileScanner.class));
    when(sf.getReader()).thenReturn(r);
    when(sf.createReader(anyBoolean())).thenReturn(r);
    when(sf.createReader()).thenReturn(r);
    when(sf.cloneForReader()).thenReturn(sf);
    return sf;
  }

  private static StoreFile createFile() throws Exception {
    return createFile(0);
  }

  private static void setFileStripe(StoreFile sf, byte[] startKey, byte[] endKey) {
    when(sf.getMetadataValue(StripeStoreFileManager.STRIPE_START_KEY)).thenReturn(startKey);
    when(sf.getMetadataValue(StripeStoreFileManager.STRIPE_END_KEY)).thenReturn(endKey);
  }

  private StripeCompactor createCompactor() throws Exception {
    HColumnDescriptor col = new HColumnDescriptor(Bytes.toBytes("foo"));
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    Store store = mock(Store.class);
    HRegionInfo info = mock(HRegionInfo.class);
    when(info.getRegionNameAsString()).thenReturn("testRegion");
    when(store.getFamily()).thenReturn(col);
    when(store.getRegionInfo()).thenReturn(info);
    when(
      store.createWriterInTmp(anyLong(), any(Compression.Algorithm.class), anyBoolean(),
        anyBoolean(), anyBoolean(), anyBoolean())).thenAnswer(writers);

    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.regionserver.compaction.private.readers", usePrivateReaders);
    final Scanner scanner = new Scanner();
    return new StripeCompactor(conf, store) {
      @Override
      protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
          long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
          byte[] dropDeletesToRow) throws IOException {
        return scanner;
      }

      @Override
      protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
          ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        return scanner;
      }
    };
  }

  private static class Scanner implements InternalScanner {
    private final ArrayList<KeyValue> kvs;

    public Scanner(KeyValue... kvs) {
      this.kvs = new ArrayList<KeyValue>(Arrays.asList(kvs));
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      if (kvs.isEmpty()) return false;
      results.add(kvs.remove(0));
      return !kvs.isEmpty();
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext)
        throws IOException {
      return next(result);
    }

    @Override
    public void close() throws IOException {
    }
  }
}
