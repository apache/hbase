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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.FailedArchiveException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Tests that archiving compacted files behaves correctly when encountering exceptions.
 */
@Category(MediumTests.class)
public class TestCompactionArchiveIOException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionArchiveIOException.class);

  private static final String ERROR_FILE = "fffffffffffffffffdeadbeef";

  public HBaseTestingUtility testUtil;

  private Path testDir;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniDFSCluster(1);
    testDir = testUtil.getDataTestDirOnTestFS();
    CommonFSUtils.setRootDir(testUtil.getConfiguration(), testDir);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.cleanupTestDir();
    testUtil.shutdownMiniDFSCluster();
  }

  @Test
  public void testRemoveCompactedFilesWithException() throws Exception {
    byte[] fam = Bytes.toBytes("f");
    byte[] col = Bytes.toBytes("c");
    byte[] val = Bytes.toBytes("val");

    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    HRegion region = initHRegion(htd, info);
    RegionServerServices rss = mock(RegionServerServices.class);
    List<HRegion> regions = new ArrayList<>();
    regions.add(region);
    Mockito.doReturn(regions).when(rss).getRegions();

    // Create the cleaner object
    final CompactedHFilesDischarger cleaner =
        new CompactedHFilesDischarger(1000, (Stoppable) null, rss, false);
    // Add some data to the region and do some flushes
    int batchSize = 10;
    int fileCount = 10;
    for (int f = 0; f < fileCount; f++) {
      int start = f * batchSize;
      for (int i = start; i < start + batchSize; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, col, val);
        region.put(p);
      }
      // flush them
      region.flush(true);
    }

    HStore store = region.getStore(fam);
    assertEquals(fileCount, store.getStorefilesCount());

    Collection<HStoreFile> storefiles = store.getStorefiles();
    // None of the files should be in compacted state.
    for (HStoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }

    StoreFileManager fileManager = store.getStoreEngine().getStoreFileManager();
    Collection<HStoreFile> initialCompactedFiles = fileManager.getCompactedfiles();
    assertTrue(initialCompactedFiles == null || initialCompactedFiles.isEmpty());

    // Do compaction
    region.compact(true);

    // all prior store files should now be compacted
    Collection<HStoreFile> compactedFilesPreClean = fileManager.getCompactedfiles();
    assertNotNull(compactedFilesPreClean);
    assertTrue(compactedFilesPreClean.size() > 0);

    // add the dummy file to the store directory
    HRegionFileSystem regionFS = region.getRegionFileSystem();
    Path errFile = regionFS.getStoreFilePath(Bytes.toString(fam), ERROR_FILE);
    FSDataOutputStream out = regionFS.getFileSystem().create(errFile);
    out.writeInt(1);
    out.close();

    HStoreFile errStoreFile = new MockHStoreFile(testUtil, errFile, 1, 0, false, 1);
    fileManager.addCompactionResults(
        ImmutableList.of(errStoreFile), ImmutableList.of());

    // cleanup compacted files
    cleaner.chore();

    // make sure the compacted files are cleared
    Collection<HStoreFile> compactedFilesPostClean = fileManager.getCompactedfiles();
    assertEquals(1, compactedFilesPostClean.size());
    for (HStoreFile origFile : compactedFilesPreClean) {
      assertFalse(compactedFilesPostClean.contains(origFile));
    }

    // close the region
    try {
      region.close();
    } catch (FailedArchiveException e) {
      // expected due to errorfile
      assertEquals(1, e.getFailedFiles().size());
      assertEquals(ERROR_FILE, e.getFailedFiles().iterator().next().getName());
    }
  }

  private HRegion initHRegion(TableDescriptor htd, RegionInfo info) throws IOException {
    Configuration conf = testUtil.getConfiguration();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    Path tableDir = CommonFSUtils.getTableDir(testDir, htd.getTableName());
    Path regionDir = new Path(tableDir, info.getEncodedName());
    Path storeDir = new Path(regionDir, htd.getColumnFamilies()[0].getNameAsString());

    FileSystem errFS = spy(testUtil.getTestFileSystem());
    // Prior to HBASE-16964, when an exception is thrown archiving any compacted file,
    // none of the other files are cleared from the compactedfiles list.
    // Simulate this condition with a dummy file
    doThrow(new IOException("Error for test")).when(errFS)
        .rename(eq(new Path(storeDir, ERROR_FILE)), any());

    HRegionFileSystem fs = new HRegionFileSystem(conf, errFS, tableDir, info);
    final Configuration walConf = new Configuration(conf);
    CommonFSUtils.setRootDir(walConf, tableDir);
    final WALFactory wals = new WALFactory(walConf, "log_" + info.getEncodedName());
    HRegion region = new HRegion(fs, wals.getWAL(info), conf, htd, null);

    region.initialize();

    return region;
  }
}
