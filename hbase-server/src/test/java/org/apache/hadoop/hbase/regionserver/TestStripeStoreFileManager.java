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

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({RegionServerTests.class, MediumTests.class})
public class TestStripeStoreFileManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStripeStoreFileManager.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Path BASEDIR =
      TEST_UTIL.getDataTestDir(TestStripeStoreFileManager.class.getSimpleName());
  private static final Path CFDIR = HRegionFileSystem.getStoreHomedir(BASEDIR, "region",
    Bytes.toBytes("cf"));

  private static final byte[] KEY_A = Bytes.toBytes("aaa");
  private static final byte[] KEY_B = Bytes.toBytes("aab");
  private static final byte[] KEY_C = Bytes.toBytes("aac");
  private static final byte[] KEY_D = Bytes.toBytes("aad");

  private static final KeyValue KV_A = new KeyValue(KEY_A, 0L);
  private static final KeyValue KV_B = new KeyValue(KEY_B, 0L);
  private static final KeyValue KV_C = new KeyValue(KEY_C, 0L);
  private static final KeyValue KV_D = new KeyValue(KEY_D, 0L);

  @Before
  public void setUp() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    if (!fs.mkdirs(CFDIR)) {
      throw new IOException("Cannot create test directory " + CFDIR);
    }
  }

  @After
  public void tearDown() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    if (fs.exists(CFDIR) && !fs.delete(CFDIR, true)) {
      throw new IOException("Cannot delete test directory " + CFDIR);
    }
  }

  @Test
  public void testInsertFilesIntoL0() throws Exception {
    StripeStoreFileManager manager = createManager();
    MockHStoreFile sf = createFile();
    manager.insertNewFiles(al(sf));
    assertEquals(1, manager.getStorefileCount());
    Collection<HStoreFile> filesForGet = manager.getFilesForScan(KEY_A, true, KEY_A, true);
    assertEquals(1, filesForGet.size());
    assertTrue(filesForGet.contains(sf));

    // Add some stripes and make sure we get this file for every stripe.
    manager.addCompactionResults(al(), al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, OPEN_KEY)));
    assertTrue(manager.getFilesForScan(KEY_A, true, KEY_A, true).contains(sf));
    assertTrue(manager.getFilesForScan(KEY_C, true, KEY_C, true).contains(sf));
  }

  @Test
  public void testClearFiles() throws Exception {
    StripeStoreFileManager manager = createManager();
    manager.insertNewFiles(al(createFile()));
    manager.insertNewFiles(al(createFile()));
    manager.addCompactionResults(al(), al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, OPEN_KEY)));
    assertEquals(4, manager.getStorefileCount());
    Collection<HStoreFile> allFiles = manager.clearFiles();
    assertEquals(4, allFiles.size());
    assertEquals(0, manager.getStorefileCount());
    assertEquals(0, manager.getStorefiles().size());
  }

  private static ArrayList<HStoreFile> dumpIterator(Iterator<HStoreFile> iter) {
    ArrayList<HStoreFile> result = new ArrayList<>();
    for (; iter.hasNext(); result.add(iter.next())) {
      continue;
    }
    return result;
  }

  @Test
  public void testRowKeyBefore() throws Exception {
    StripeStoreFileManager manager = createManager();
    HStoreFile l0File = createFile(), l0File2 = createFile();
    manager.insertNewFiles(al(l0File));
    manager.insertNewFiles(al(l0File2));
    // Get candidate files.
    Iterator<HStoreFile> sfs = manager.getCandidateFilesForRowKeyBefore(KV_B);
    sfs.next();
    sfs.remove();
    // Suppose we found a candidate in this file... make sure L0 file remaining is not removed.
    sfs = manager.updateCandidateFilesForRowKeyBefore(sfs, KV_B, KV_A);
    assertTrue(sfs.hasNext());
    // Now add some stripes (remove L0 file too)
    MockHStoreFile stripe0a = createFile(0, 100, OPEN_KEY, KEY_B),
        stripe1 = createFile(KEY_B, OPEN_KEY);
    manager.addCompactionResults(al(l0File), al(stripe0a, stripe1));
    manager.removeCompactedFiles(al(l0File));
    // If we want a key <= KEY_A, we should get everything except stripe1.
    ArrayList<HStoreFile> sfsDump = dumpIterator(manager.getCandidateFilesForRowKeyBefore(KV_A));
    assertEquals(2, sfsDump.size());
    assertTrue(sfsDump.contains(stripe0a));
    assertFalse(sfsDump.contains(stripe1));
    // If we want a key <= KEY_B, we should get everything since lower bound is inclusive.
    sfsDump = dumpIterator(manager.getCandidateFilesForRowKeyBefore(KV_B));
    assertEquals(3, sfsDump.size());
    assertTrue(sfsDump.contains(stripe1));
    // For KEY_D, we should also get everything.
    sfsDump = dumpIterator(manager.getCandidateFilesForRowKeyBefore(KV_D));
    assertEquals(3, sfsDump.size());
    // Suppose in the first file we found candidate with KEY_C.
    // Then, stripe0 no longer matters and should be removed, but stripe1 should stay.
    sfs = manager.getCandidateFilesForRowKeyBefore(KV_D);
    sfs.next(); // Skip L0 file.
    sfs.remove();
    sfs = manager.updateCandidateFilesForRowKeyBefore(sfs, KV_D, KV_C);
    assertEquals(stripe1, sfs.next());
    assertFalse(sfs.hasNext());
    // Add one more, later, file to stripe0, remove the last annoying L0 file.
    // This file should be returned in preference to older L0 file; also, after we get
    // a candidate from the first file, the old one should not be removed.
    HStoreFile stripe0b = createFile(0, 101, OPEN_KEY, KEY_B);
    manager.addCompactionResults(al(l0File2), al(stripe0b));
    manager.removeCompactedFiles(al(l0File2));
    sfs = manager.getCandidateFilesForRowKeyBefore(KV_A);
    assertEquals(stripe0b, sfs.next());
    sfs.remove();
    sfs = manager.updateCandidateFilesForRowKeyBefore(sfs, KV_A, KV_A);
    assertEquals(stripe0a, sfs.next());
  }

  @Test
  public void testGetSplitPointEdgeCases() throws Exception {
    StripeStoreFileManager manager = createManager();
    // No files => no split.
    assertFalse(manager.getSplitPoint().isPresent());

    // If there are no stripes, should pick midpoint from the biggest file in L0.
    MockHStoreFile sf5 = createFile(5, 0);
    sf5.splitPoint = new byte[] { 1 };
    manager.insertNewFiles(al(sf5));
    manager.insertNewFiles(al(createFile(1, 0)));
    assertArrayEquals(sf5.splitPoint, manager.getSplitPoint().get());

    // Same if there's one stripe but the biggest file is still in L0.
    manager.addCompactionResults(al(), al(createFile(2, 0, OPEN_KEY, OPEN_KEY)));
    assertArrayEquals(sf5.splitPoint, manager.getSplitPoint().get());

    // If the biggest file is in the stripe, should get from it.
    MockHStoreFile sf6 = createFile(6, 0, OPEN_KEY, OPEN_KEY);
    sf6.splitPoint = new byte[] { 2 };
    manager.addCompactionResults(al(), al(sf6));
    assertArrayEquals(sf6.splitPoint, manager.getSplitPoint().get());
  }

  @Test
  public void testGetStripeBoundarySplits() throws Exception {
    /* First number - split must be after this stripe; further numbers - stripes */
    verifySplitPointScenario(5, false, 0f,     2, 1, 1, 1, 1, 1, 10);
    verifySplitPointScenario(0, false, 0f,     6, 3, 1, 1, 2);
    verifySplitPointScenario(2, false, 0f,     1, 1, 1, 1, 2);
    verifySplitPointScenario(0, false, 0f,     5, 4);
    verifySplitPointScenario(2, false, 0f,     5, 2, 5, 5, 5);
  }

  @Test
  public void testGetUnbalancedSplits() throws Exception {
    /* First number - split must be inside/after this stripe; further numbers - stripes */
    verifySplitPointScenario(0, false, 2.1f,      4, 4, 4); // 8/4 is less than 2.1f
    verifySplitPointScenario(1, true,  1.5f,      4, 4, 4); // 8/4 > 6/6
    verifySplitPointScenario(1, false, 1.1f,      3, 4, 1, 1, 2, 2); // 7/6 < 8/5
    verifySplitPointScenario(1, false, 1.1f,      3, 6, 1, 1, 2, 2); // 9/6 == 9/6
    verifySplitPointScenario(1, true,  1.1f,      3, 8, 1, 1, 2, 2); // 11/6 > 10/7
    verifySplitPointScenario(3, false, 1.1f,      2, 2, 1, 1, 4, 3); // reverse order
    verifySplitPointScenario(4, true,  1.1f,      2, 2, 1, 1, 8, 3); // reverse order
    verifySplitPointScenario(0, true,  1.5f,      10, 4); // 10/4 > 9/5
    verifySplitPointScenario(0, false, 1.4f,      6, 4);  // 6/4 == 6/4
    verifySplitPointScenario(1, true,  1.5f,      4, 10); // reverse just in case
    verifySplitPointScenario(0, false, 1.4f,      4, 6);  // reverse just in case
  }


  /**
   * Verifies scenario for finding a split point.
   * @param splitPointAfter Stripe to expect the split point at/after.
   * @param shouldSplitStripe If true, the split point is expected in the middle of the above
   *                          stripe; if false, should be at the end.
   * @param splitRatioToVerify Maximum split imbalance ratio.
   * @param sizes Stripe sizes.
   */
  private void verifySplitPointScenario(int splitPointAfter, boolean shouldSplitStripe,
      float splitRatioToVerify, int... sizes) throws Exception {
    assertTrue(sizes.length > 1);
    ArrayList<HStoreFile> sfs = new ArrayList<>();
    for (int sizeIx = 0; sizeIx < sizes.length; ++sizeIx) {
      byte[] startKey = (sizeIx == 0) ? OPEN_KEY : Bytes.toBytes(sizeIx - 1);
      byte[] endKey = (sizeIx == sizes.length - 1) ? OPEN_KEY : Bytes.toBytes(sizeIx);
      MockHStoreFile sf = createFile(sizes[sizeIx], 0, startKey, endKey);
      sf.splitPoint = Bytes.toBytes(-sizeIx); // set split point to the negative index
      sfs.add(sf);
    }

    Configuration conf = HBaseConfiguration.create();
    if (splitRatioToVerify != 0) {
      conf.setFloat(StripeStoreConfig.MAX_REGION_SPLIT_IMBALANCE_KEY, splitRatioToVerify);
    }
    StripeStoreFileManager manager = createManager(al(), conf);
    manager.addCompactionResults(al(), sfs);
    int result = Bytes.toInt(manager.getSplitPoint().get());
    // Either end key and thus positive index, or "middle" of the file and thus negative index.
    assertEquals(splitPointAfter * (shouldSplitStripe ? -1 : 1), result);
  }

  private static byte[] keyAfter(byte[] key) {
    return Arrays.copyOf(key, key.length + 1);
  }

  @Test
  public void testGetFilesForGetAndScan() throws Exception {
    StripeStoreFileManager manager = createManager();
    verifyGetAndScanScenario(manager, null, null);
    verifyGetAndScanScenario(manager, KEY_B, KEY_C);

    // Populate one L0 file.
    MockHStoreFile sf0 = createFile();
    manager.insertNewFiles(al(sf0));
    verifyGetAndScanScenario(manager, null, null,   sf0);
    verifyGetAndScanScenario(manager, null, KEY_C,  sf0);
    verifyGetAndScanScenario(manager, KEY_B, null,  sf0);
    verifyGetAndScanScenario(manager, KEY_B, KEY_C, sf0);

    // Populate a bunch of files for stripes, keep L0.
    MockHStoreFile sfA = createFile(OPEN_KEY, KEY_A);
    MockHStoreFile sfB = createFile(KEY_A, KEY_B);
    MockHStoreFile sfC = createFile(KEY_B, KEY_C);
    MockHStoreFile sfD = createFile(KEY_C, KEY_D);
    MockHStoreFile sfE = createFile(KEY_D, OPEN_KEY);
    manager.addCompactionResults(al(), al(sfA, sfB, sfC, sfD, sfE));

    verifyGetAndScanScenario(manager, null, null,              sf0, sfA, sfB, sfC, sfD, sfE);
    verifyGetAndScanScenario(manager, keyAfter(KEY_A), null,   sf0, sfB, sfC, sfD, sfE);
    verifyGetAndScanScenario(manager, null, keyAfter(KEY_C),   sf0, sfA, sfB, sfC, sfD);
    verifyGetAndScanScenario(manager, KEY_B, null,             sf0, sfC, sfD, sfE);
    verifyGetAndScanScenario(manager, null, KEY_C,             sf0, sfA, sfB, sfC, sfD);
    verifyGetAndScanScenario(manager, KEY_B, keyAfter(KEY_B),  sf0, sfC);
    verifyGetAndScanScenario(manager, keyAfter(KEY_A), KEY_B,  sf0, sfB, sfC);
    verifyGetAndScanScenario(manager, KEY_D, KEY_D,            sf0, sfE);
    verifyGetAndScanScenario(manager, keyAfter(KEY_B), keyAfter(KEY_C), sf0, sfC, sfD);
  }

  private void verifyGetAndScanScenario(StripeStoreFileManager manager, byte[] start, byte[] end,
      HStoreFile... results) throws Exception {
    verifyGetOrScanScenario(manager, start, end, results);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLoadFilesWithRecoverableBadFiles() throws Exception {
    // In L0, there will be file w/o metadata (real L0, 3 files with invalid metadata, and 3
    // files that overlap valid stripes in various ways). Note that the 4th way to overlap the
    // stripes will cause the structure to be mostly scraped, and is tested separately.
    ArrayList<HStoreFile> validStripeFiles = al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, KEY_C), createFile(KEY_C, OPEN_KEY),
        createFile(KEY_C, OPEN_KEY));
    ArrayList<HStoreFile> filesToGoToL0 = al(createFile(), createFile(null, KEY_A),
        createFile(KEY_D, null), createFile(KEY_D, KEY_A), createFile(keyAfter(KEY_A), KEY_C),
        createFile(OPEN_KEY, KEY_D), createFile(KEY_D, keyAfter(KEY_D)));
    ArrayList<HStoreFile> allFilesToGo = flattenLists(validStripeFiles, filesToGoToL0);
    Collections.shuffle(allFilesToGo);
    StripeStoreFileManager manager = createManager(allFilesToGo);
    List<HStoreFile> l0Files = manager.getLevel0Files();
    assertEquals(filesToGoToL0.size(), l0Files.size());
    for (HStoreFile sf : filesToGoToL0) {
      assertTrue(l0Files.contains(sf));
    }
    verifyAllFiles(manager, allFilesToGo);
  }

  @Test
  public void testLoadFilesWithBadStripe() throws Exception {
    // Current "algorithm" will see the after-B key before C key, add it as valid stripe,
    // and then fail all other stripes. So everything would end up in L0.
    ArrayList<HStoreFile> allFilesToGo = al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, KEY_C), createFile(KEY_C, OPEN_KEY),
        createFile(KEY_B, keyAfter(KEY_B)));
    Collections.shuffle(allFilesToGo);
    StripeStoreFileManager manager = createManager(allFilesToGo);
    assertEquals(allFilesToGo.size(), manager.getLevel0Files().size());
  }

  @Test
  public void testLoadFilesWithGaps() throws Exception {
    // Stripes must not have gaps. If they do, everything goes to L0.
    StripeStoreFileManager manager =
      createManager(al(createFile(OPEN_KEY, KEY_B), createFile(KEY_C, OPEN_KEY)));
    assertEquals(2, manager.getLevel0Files().size());
    // Just one open stripe should be ok.
    manager = createManager(al(createFile(OPEN_KEY, OPEN_KEY)));
    assertEquals(0, manager.getLevel0Files().size());
    assertEquals(1, manager.getStorefileCount());
  }

  @Test
  public void testLoadFilesAfterSplit() throws Exception {
    // If stripes are good but have non-open ends, they must be treated as open ends.
    MockHStoreFile sf = createFile(KEY_B, KEY_C);
    StripeStoreFileManager manager = createManager(al(createFile(OPEN_KEY, KEY_B), sf));
    assertEquals(0, manager.getLevel0Files().size());
    // Here, [B, C] is logically [B, inf), so we should be able to compact it to that only.
    verifyInvalidCompactionScenario(manager, al(sf), al(createFile(KEY_B, KEY_C)));
    manager.addCompactionResults(al(sf), al(createFile(KEY_B, OPEN_KEY)));
    manager.removeCompactedFiles(al(sf));
    // Do the same for other variants.
    manager = createManager(al(sf, createFile(KEY_C, OPEN_KEY)));
    verifyInvalidCompactionScenario(manager, al(sf), al(createFile(KEY_B, KEY_C)));
    manager.addCompactionResults(al(sf), al(createFile(OPEN_KEY, KEY_C)));
    manager.removeCompactedFiles(al(sf));
    manager = createManager(al(sf));
    verifyInvalidCompactionScenario(manager, al(sf), al(createFile(KEY_B, KEY_C)));
    manager.addCompactionResults(al(sf), al(createFile(OPEN_KEY, OPEN_KEY)));
  }

  @Test
  public void testAddingCompactionResults() throws Exception {
    StripeStoreFileManager manager = createManager();
    // First, add some L0 files and "compact" one with new stripe creation.
    HStoreFile sf_L0_0a = createFile(), sf_L0_0b = createFile();
    manager.insertNewFiles(al(sf_L0_0a, sf_L0_0b));

    // Try compacting with invalid new branches (gaps, overlaps) - no effect.
    verifyInvalidCompactionScenario(manager, al(sf_L0_0a), al(createFile(OPEN_KEY, KEY_B)));
    verifyInvalidCompactionScenario(manager, al(sf_L0_0a), al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_C, OPEN_KEY)));
    verifyInvalidCompactionScenario(manager, al(sf_L0_0a), al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, OPEN_KEY), createFile(KEY_A, KEY_D)));
    verifyInvalidCompactionScenario(manager, al(sf_L0_0a), al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_A, KEY_B), createFile(KEY_B, OPEN_KEY)));

    HStoreFile sf_i2B_0 = createFile(OPEN_KEY, KEY_B);
    HStoreFile sf_B2C_0 = createFile(KEY_B, KEY_C);
    HStoreFile sf_C2i_0 = createFile(KEY_C, OPEN_KEY);
    manager.addCompactionResults(al(sf_L0_0a), al(sf_i2B_0, sf_B2C_0, sf_C2i_0));
    manager.removeCompactedFiles(al(sf_L0_0a));
    verifyAllFiles(manager, al(sf_L0_0b, sf_i2B_0, sf_B2C_0, sf_C2i_0));

    // Add another l0 file, "compact" both L0 into two stripes
    HStoreFile sf_L0_1 = createFile();
    HStoreFile sf_i2B_1 = createFile(OPEN_KEY, KEY_B);
    HStoreFile sf_B2C_1 = createFile(KEY_B, KEY_C);
    manager.insertNewFiles(al(sf_L0_1));
    manager.addCompactionResults(al(sf_L0_0b, sf_L0_1), al(sf_i2B_1, sf_B2C_1));
    manager.removeCompactedFiles(al(sf_L0_0b, sf_L0_1));
    verifyAllFiles(manager, al(sf_i2B_0, sf_B2C_0, sf_C2i_0, sf_i2B_1, sf_B2C_1));

    // Try compacting with invalid file (no metadata) - should add files to L0.
    HStoreFile sf_L0_2 = createFile(null, null);
    manager.addCompactionResults(al(), al(sf_L0_2));
    manager.removeCompactedFiles(al());
    verifyAllFiles(manager, al(sf_i2B_0, sf_B2C_0, sf_C2i_0, sf_i2B_1, sf_B2C_1, sf_L0_2));
    // Remove it...
    manager.addCompactionResults(al(sf_L0_2), al());
    manager.removeCompactedFiles(al(sf_L0_2));

    // Do regular compaction in the first stripe.
    HStoreFile sf_i2B_3 = createFile(OPEN_KEY, KEY_B);
    manager.addCompactionResults(al(sf_i2B_0, sf_i2B_1), al(sf_i2B_3));
    manager.removeCompactedFiles(al(sf_i2B_0, sf_i2B_1));
    verifyAllFiles(manager, al(sf_B2C_0, sf_C2i_0, sf_B2C_1, sf_i2B_3));

    // Rebalance two stripes.
    HStoreFile sf_B2D_4 = createFile(KEY_B, KEY_D);
    HStoreFile sf_D2i_4 = createFile(KEY_D, OPEN_KEY);
    manager.addCompactionResults(al(sf_B2C_0, sf_C2i_0, sf_B2C_1), al(sf_B2D_4, sf_D2i_4));
    manager.removeCompactedFiles(al(sf_B2C_0, sf_C2i_0, sf_B2C_1));
    verifyAllFiles(manager, al(sf_i2B_3, sf_B2D_4, sf_D2i_4));

    // Split the first stripe.
    HStoreFile sf_i2A_5 = createFile(OPEN_KEY, KEY_A);
    HStoreFile sf_A2B_5 = createFile(KEY_A, KEY_B);
    manager.addCompactionResults(al(sf_i2B_3), al(sf_i2A_5, sf_A2B_5));
    manager.removeCompactedFiles(al(sf_i2B_3));
    verifyAllFiles(manager, al(sf_B2D_4, sf_D2i_4, sf_i2A_5, sf_A2B_5));

    // Split the middle stripe.
    HStoreFile sf_B2C_6 = createFile(KEY_B, KEY_C);
    HStoreFile sf_C2D_6 = createFile(KEY_C, KEY_D);
    manager.addCompactionResults(al(sf_B2D_4), al(sf_B2C_6, sf_C2D_6));
    manager.removeCompactedFiles(al(sf_B2D_4));
    verifyAllFiles(manager, al(sf_D2i_4, sf_i2A_5, sf_A2B_5, sf_B2C_6, sf_C2D_6));

    // Merge two different middle stripes.
    HStoreFile sf_A2C_7 = createFile(KEY_A, KEY_C);
    manager.addCompactionResults(al(sf_A2B_5, sf_B2C_6), al(sf_A2C_7));
    manager.removeCompactedFiles(al(sf_A2B_5, sf_B2C_6));
    verifyAllFiles(manager, al(sf_D2i_4, sf_i2A_5, sf_C2D_6, sf_A2C_7));

    // Merge lower half.
    HStoreFile sf_i2C_8 = createFile(OPEN_KEY, KEY_C);
    manager.addCompactionResults(al(sf_i2A_5, sf_A2C_7), al(sf_i2C_8));
    manager.removeCompactedFiles(al(sf_i2A_5, sf_A2C_7));
    verifyAllFiles(manager, al(sf_D2i_4, sf_C2D_6, sf_i2C_8));

    // Merge all.
    HStoreFile sf_i2i_9 = createFile(OPEN_KEY, OPEN_KEY);
    manager.addCompactionResults(al(sf_D2i_4, sf_C2D_6, sf_i2C_8), al(sf_i2i_9));
    manager.removeCompactedFiles(al(sf_D2i_4, sf_C2D_6, sf_i2C_8));
    verifyAllFiles(manager, al(sf_i2i_9));
  }

  @Test
  public void testCompactionAndFlushConflict() throws Exception {
    // Add file flush into stripes
    StripeStoreFileManager sfm = createManager();
    assertEquals(0, sfm.getStripeCount());
    HStoreFile sf_i2c = createFile(OPEN_KEY, KEY_C), sf_c2i = createFile(KEY_C, OPEN_KEY);
    sfm.insertNewFiles(al(sf_i2c, sf_c2i));
    assertEquals(2, sfm.getStripeCount());
    // Now try to add conflicting flush - should throw.
    HStoreFile sf_i2d = createFile(OPEN_KEY, KEY_D), sf_d2i = createFile(KEY_D, OPEN_KEY);
    sfm.insertNewFiles(al(sf_i2d, sf_d2i));
    assertEquals(2, sfm.getStripeCount());
    assertEquals(2, sfm.getLevel0Files().size());
    verifyGetAndScanScenario(sfm, KEY_C, KEY_C, sf_i2d, sf_d2i, sf_c2i);
    // Remove these files.
    sfm.addCompactionResults(al(sf_i2d, sf_d2i), al());
    sfm.removeCompactedFiles(al(sf_i2d, sf_d2i));
    assertEquals(0, sfm.getLevel0Files().size());
    // Add another file to stripe; then "rebalance" stripes w/o it - the file, which was
    // presumably flushed during compaction, should go to L0.
    HStoreFile sf_i2c_2 = createFile(OPEN_KEY, KEY_C);
    sfm.insertNewFiles(al(sf_i2c_2));
    sfm.addCompactionResults(al(sf_i2c, sf_c2i), al(sf_i2d, sf_d2i));
    sfm.removeCompactedFiles(al(sf_i2c, sf_c2i));
    assertEquals(1, sfm.getLevel0Files().size());
    verifyGetAndScanScenario(sfm, KEY_C, KEY_C, sf_i2d, sf_i2c_2);
  }

  @Test
  public void testEmptyResultsForStripes() throws Exception {
    // Test that we can compact L0 into a subset of stripes.
    StripeStoreFileManager manager = createManager();
    HStoreFile sf0a = createFile();
    HStoreFile sf0b = createFile();
    manager.insertNewFiles(al(sf0a));
    manager.insertNewFiles(al(sf0b));
    ArrayList<HStoreFile> compacted = al(createFile(OPEN_KEY, KEY_B),
        createFile(KEY_B, KEY_C), createFile(KEY_C, OPEN_KEY));
    manager.addCompactionResults(al(sf0a), compacted);
    manager.removeCompactedFiles(al(sf0a));
    // Next L0 compaction only produces file for the first and last stripe.
    ArrayList<HStoreFile> compacted2 = al(createFile(OPEN_KEY, KEY_B), createFile(KEY_C, OPEN_KEY));
    manager.addCompactionResults(al(sf0b), compacted2);
    manager.removeCompactedFiles(al(sf0b));
    compacted.addAll(compacted2);
    verifyAllFiles(manager, compacted);
  }

  @Test
  public void testPriority() throws Exception {
    // Expected priority, file limit, stripe count, files per stripe, l0 files.
    testPriorityScenario(5,    5, 0, 0, 0);
    testPriorityScenario(2,    5, 0, 0, 3);
    testPriorityScenario(4,   25, 5, 1, 0); // example case.
    testPriorityScenario(3,   25, 5, 1, 1); // L0 files counts for all stripes.
    testPriorityScenario(3,   25, 5, 2, 0); // file to each stripe - same as one L0 file.
    testPriorityScenario(2,   25, 5, 4, 0); // 1 is priority user, so 2 is returned.
    testPriorityScenario(2,   25, 5, 4, 4); // don't return higher than user unless over limit.
    testPriorityScenario(2,   25, 5, 1, 10); // same.
    testPriorityScenario(0,   25, 5, 4, 5); // at limit.
    testPriorityScenario(-5,  25, 5, 6, 0); // over limit!
    testPriorityScenario(-1,  25, 0, 0, 26); // over limit with just L0
  }

  private void testPriorityScenario(int expectedPriority,
      int limit, int stripes, int filesInStripe, int l0Files) throws Exception {
    final byte[][] keys = { KEY_A, KEY_B, KEY_C, KEY_D };
    assertTrue(stripes <= keys.length + 1);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.hstore.blockingStoreFiles", limit);
    StripeStoreFileManager sfm = createManager(al(), conf);
    for (int i = 0; i < l0Files; ++i) {
      sfm.insertNewFiles(al(createFile()));
    }
    for (int i = 0; i < filesInStripe; ++i) {
      ArrayList<HStoreFile> stripe = new ArrayList<>();
      for (int j = 0; j < stripes; ++j) {
        stripe.add(createFile(
            (j == 0) ? OPEN_KEY : keys[j - 1], (j == stripes - 1) ? OPEN_KEY : keys[j]));
      }
      sfm.addCompactionResults(al(), stripe);
    }
    assertEquals(expectedPriority, sfm.getStoreCompactionPriority());
  }

  private void verifyInvalidCompactionScenario(StripeStoreFileManager manager,
      ArrayList<HStoreFile> filesToCompact, ArrayList<HStoreFile> filesToInsert) throws Exception {
    Collection<HStoreFile> allFiles = manager.getStorefiles();
    try {
      manager.addCompactionResults(filesToCompact, filesToInsert);
      fail("Should have thrown");
    } catch (IOException ex) {
      // Ignore it.
    }
    verifyAllFiles(manager, allFiles); // must have the same files.
  }

  private void verifyGetOrScanScenario(StripeStoreFileManager manager, byte[] start, byte[] end,
      HStoreFile... results) throws Exception {
    verifyGetOrScanScenario(manager, start, end, Arrays.asList(results));
  }

  private void verifyGetOrScanScenario(StripeStoreFileManager manager, byte[] start, byte[] end,
      Collection<HStoreFile> results) throws Exception {
    start = start != null ? start : HConstants.EMPTY_START_ROW;
    end = end != null ? end : HConstants.EMPTY_END_ROW;
    Collection<HStoreFile> sfs = manager.getFilesForScan(start, true, end, false);
    assertEquals(results.size(), sfs.size());
    for (HStoreFile result : results) {
      assertTrue(sfs.contains(result));
    }
  }

  private void verifyAllFiles(
      StripeStoreFileManager manager, Collection<HStoreFile> results) throws Exception {
    verifyGetOrScanScenario(manager, null, null, results);
  }

  // TODO: replace with Mockito?
  private static MockHStoreFile createFile(
      long size, long seqNum, byte[] startKey, byte[] endKey) throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path testFilePath = StoreFileWriter.getUniqueFile(fs, CFDIR);
    fs.create(testFilePath).close();
    MockHStoreFile sf = new MockHStoreFile(TEST_UTIL, testFilePath, size, 0, false, seqNum);
    if (startKey != null) {
      sf.setMetadataValue(StripeStoreFileManager.STRIPE_START_KEY, startKey);
    }
    if (endKey != null) {
      sf.setMetadataValue(StripeStoreFileManager.STRIPE_END_KEY, endKey);
    }
    return sf;
  }

  private static MockHStoreFile createFile(long size, long seqNum) throws Exception {
    return createFile(size, seqNum, null, null);
  }

  private static MockHStoreFile createFile(byte[] startKey, byte[] endKey) throws Exception {
    return createFile(0, 0, startKey, endKey);
  }

  private static MockHStoreFile createFile() throws Exception {
    return createFile(null, null);
  }

  private static StripeStoreFileManager createManager() throws Exception {
    return createManager(new ArrayList<>());
  }

  private static StripeStoreFileManager createManager(ArrayList<HStoreFile> sfs) throws Exception {
    return createManager(sfs, TEST_UTIL.getConfiguration());
  }

  private static StripeStoreFileManager createManager(
      ArrayList<HStoreFile> sfs, Configuration conf) throws Exception {
    StripeStoreConfig config = new StripeStoreConfig(
        conf, Mockito.mock(StoreConfigInformation.class));
    StripeStoreFileManager result = new StripeStoreFileManager(CellComparatorImpl.COMPARATOR, conf,
        config);
    result.loadFiles(sfs);
    return result;
  }

  private static ArrayList<HStoreFile> al(HStoreFile... sfs) {
    return new ArrayList<>(Arrays.asList(sfs));
  }

  private static ArrayList<HStoreFile> flattenLists(ArrayList<HStoreFile>... sfls) {
    ArrayList<HStoreFile> result = new ArrayList<>();
    for (ArrayList<HStoreFile> sfl : sfls) {
      result.addAll(sfl);
    }
    return result;
  }
}
