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
package org.apache.hadoop.hbase.mob;

import static org.apache.hadoop.hbase.HBaseTestingUtil.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam1;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MOB_CELLS_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionAsTable;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test mob store compaction
 */
@Category(MediumTests.class)
public class TestMobStoreCompaction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobStoreCompaction.class);

  @Rule
  public TestName name = new TestName();
  static final Logger LOG = LoggerFactory.getLogger(TestMobStoreCompaction.class.getName());
  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private Configuration conf = null;

  private HRegion region = null;
  private TableDescriptor tableDescriptor = null;
  private ColumnFamilyDescriptor familyDescriptor = null;
  private long mobCellThreshold = 1000;

  private FileSystem fs;

  private static final byte[] COLUMN_FAMILY = fam1;
  private final byte[] STARTROW = Bytes.toBytes(START_KEY);
  private int compactionThreshold;

  private void init(Configuration conf, long mobThreshold) throws Exception {
    this.conf = conf;
    this.mobCellThreshold = mobThreshold;
    HBaseTestingUtil UTIL = new HBaseTestingUtil(conf);

    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);
    familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).setMobEnabled(true)
      .setMobThreshold(mobThreshold).setMaxVersions(1).build();
    tableDescriptor = UTIL.createModifyableTableDescriptor(name.getMethodName())
      .modifyColumnFamily(familyDescriptor).build();

    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    region = HBaseTestingUtil.createRegionAndWAL(regionInfo,
      UTIL.getDataTestDir(), conf, tableDescriptor, new MobFileCache(conf));
    fs = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws Exception {
    region.close();
    fs.delete(UTIL.getDataTestDir(), true);
  }

  /**
   * During compaction, cells smaller than the threshold won't be affected.
   */
  @Test
  public void testSmallerValue() throws Exception {
    init(UTIL.getConfiguration(), 500);
    byte[] dummyData = makeDummyData(300); // smaller than mob threshold
    Table loader = new RegionAsTable(region);
    // one hfile per row
    for (int i = 0; i < compactionThreshold; i++) {
      Put p = createPut(i, dummyData);
      loader.put(p);
      region.flush(true);
    }
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", 0, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("Before compaction: mob rows", 0, countMobRows());

    region.compactStores();

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count", 0, countMobFiles());
    assertEquals("After compaction: referenced mob file count", 0, countReferencedMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("After compaction: mob rows", 0, countMobRows());
  }

  /**
   * During compaction, the mob threshold size is changed.
   */
  @Test
  public void testLargerValue() throws Exception {
    init(UTIL.getConfiguration(), 200);
    byte[] dummyData = makeDummyData(300); // larger than mob threshold
    Table loader = new RegionAsTable(region);
    for (int i = 0; i < compactionThreshold; i++) {
      Put p = createPut(i, dummyData);
      loader.put(p);
      region.flush(true);
    }
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", compactionThreshold, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("Before compaction: mob rows", compactionThreshold, countMobRows());
    assertEquals("Before compaction: number of mob cells", compactionThreshold,
        countMobCellsInMetadata());
    // Change the threshold larger than the data size
    setMobThreshold(region, COLUMN_FAMILY, 500);
    region.initialize();

    List<HStore> stores = region.getStores();
    for (HStore store: stores) {
      // Force major compaction
      store.triggerMajorCompaction();
      Optional<CompactionContext> context =
          store.requestCompaction(HStore.PRIORITY_USER, CompactionLifeCycleTracker.DUMMY,
            User.getCurrent());
      if (!context.isPresent()) {
        continue;
      }
      region.compact(context.get(), store,
        NoLimitThroughputController.INSTANCE, User.getCurrent());
    }

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count", compactionThreshold, countMobFiles());
    assertEquals("After compaction: referenced mob file count", 0, countReferencedMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("After compaction: mob rows", 0, countMobRows());
  }

  private static HRegion setMobThreshold(HRegion region, byte[] cfName, long modThreshold) {
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
            .newBuilder(region.getTableDescriptor().getColumnFamily(cfName))
            .setMobThreshold(modThreshold)
            .build();
    TableDescriptor td = TableDescriptorBuilder
            .newBuilder(region.getTableDescriptor())
            .removeColumnFamily(cfName)
            .setColumnFamily(cfd)
            .build();
    region.setTableDescriptor(td);
    return region;
  }

  /**
   * This test will first generate store files, then bulk load them and trigger the compaction.
   * When compaction, the cell value will be larger than the threshold.
   */
  @Test
  public void testMobCompactionWithBulkload() throws Exception {
    // The following will produce store files of 600.
    init(UTIL.getConfiguration(), 300);
    byte[] dummyData = makeDummyData(600);

    Path hbaseRootDir = CommonFSUtils.getRootDir(conf);
    Path basedir = new Path(hbaseRootDir, tableDescriptor.getTableName().getNameAsString());
    List<Pair<byte[], String>> hfiles = new ArrayList<>(1);
    for (int i = 0; i < compactionThreshold; i++) {
      Path hpath = new Path(basedir, "hfile" + i);
      hfiles.add(Pair.newPair(COLUMN_FAMILY, hpath.toString()));
      createHFile(hpath, i, dummyData);
    }

    // The following will bulk load the above generated store files and compact, with 600(fileSize)
    // > 300(threshold)
    Map<byte[], List<Path>> map = region.bulkLoadHFiles(hfiles, true, null);
    assertTrue("Bulkload result:", !map.isEmpty());
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", 0, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("Before compaction: mob rows", 0, countMobRows());
    assertEquals("Before compaction: referenced mob file count", 0, countReferencedMobFiles());

    region.compactStores();

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count:", 1, countMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, UTIL.countRows(region));
    assertEquals("After compaction: mob rows", compactionThreshold, countMobRows());
    assertEquals("After compaction: referenced mob file count", 1, countReferencedMobFiles());
    assertEquals("After compaction: number of mob cells", compactionThreshold,
        countMobCellsInMetadata());
  }

  @Test
  public void testMajorCompactionAfterDelete() throws Exception {
    init(UTIL.getConfiguration(), 100);
    byte[] dummyData = makeDummyData(200); // larger than mob threshold
    Table loader = new RegionAsTable(region);
    // create hfiles and mob hfiles but don't trigger compaction
    int numHfiles = compactionThreshold - 1;
    byte[] deleteRow = Bytes.add(STARTROW, Bytes.toBytes(0));
    for (int i = 0; i < numHfiles; i++) {
      Put p = createPut(i, dummyData);
      loader.put(p);
      region.flush(true);
    }
    assertEquals("Before compaction: store files", numHfiles, countStoreFiles());
    assertEquals("Before compaction: mob file count", numHfiles, countMobFiles());
    assertEquals("Before compaction: rows", numHfiles, UTIL.countRows(region));
    assertEquals("Before compaction: mob rows", numHfiles, countMobRows());
    assertEquals("Before compaction: number of mob cells", numHfiles, countMobCellsInMetadata());
    // now let's delete some cells that contain mobs
    Delete delete = new Delete(deleteRow);
    delete.addFamily(COLUMN_FAMILY);
    region.delete(delete);
    region.flush(true);

    assertEquals("Before compaction: store files", numHfiles + 1, countStoreFiles());
    assertEquals("Before compaction: mob files", numHfiles, countMobFiles());
    // region.compactStores();
    region.compact(true);
    assertEquals("After compaction: store files", 1, countStoreFiles());
  }

  private int countStoreFiles() throws IOException {
    HStore store = region.getStore(COLUMN_FAMILY);
    return store.getStorefilesCount();
  }

  private int countMobFiles() throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableDescriptor.getTableName(),
      familyDescriptor.getNameAsString());
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = UTIL.getTestFileSystem().listStatus(mobDirPath);
      return files.length;
    }
    return 0;
  }

  private long countMobCellsInMetadata() throws IOException {
    long mobCellsCount = 0;
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableDescriptor.getTableName(),
      familyDescriptor.getNameAsString());
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    CacheConfig cacheConfig = new CacheConfig(copyOfConf);
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = UTIL.getTestFileSystem().listStatus(mobDirPath);
      for (FileStatus file : files) {
        HStoreFile sf = new HStoreFile(fs, file.getPath(), conf, cacheConfig, BloomType.NONE, true);
        sf.initReader();
        Map<byte[], byte[]> fileInfo = sf.getReader().loadFileInfo();
        byte[] count = fileInfo.get(MOB_CELLS_COUNT);
        assertTrue(count != null);
        mobCellsCount += Bytes.toLong(count);
      }
    }
    return mobCellsCount;
  }

  private Put createPut(int rowIdx, byte[] dummyData) throws IOException {
    Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(rowIdx)));
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(COLUMN_FAMILY, Bytes.toBytes("colX"), dummyData);
    return p;
  }

  /**
   * Create an HFile with the given number of bytes
   */
  private void createHFile(Path path, int rowIdx, byte[] dummyData) throws IOException {
    HFileContext meta = new HFileContextBuilder().build();
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, path)
        .withFileContext(meta).create();
    long now = EnvironmentEdgeManager.currentTime();
    try {
      KeyValue kv = new KeyValue(Bytes.add(STARTROW, Bytes.toBytes(rowIdx)), COLUMN_FAMILY,
          Bytes.toBytes("colX"), now, dummyData);
      writer.append(kv);
    } finally {
      writer.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(EnvironmentEdgeManager.currentTime()));
      writer.close();
    }
  }

  private int countMobRows() throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = region.getScanner(scan);

    int scannedCount = 0;
    List<Cell> results = new ArrayList<>();
    boolean hasMore = true;
    while (hasMore) {
      hasMore = scanner.next(results);
      for (Cell c : results) {
        if (MobUtils.isMobReferenceCell(c)) {
          scannedCount++;
        }
      }
      results.clear();
    }
    scanner.close();

    return scannedCount;
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }

  private int countReferencedMobFiles() throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = region.getScanner(scan);

    List<Cell> kvs = new ArrayList<>();
    boolean hasMore = true;
    String fileName;
    Set<String> files = new HashSet<>();
    do {
      kvs.clear();
      hasMore = scanner.next(kvs);
      for (Cell kv : kvs) {
        if (!MobUtils.isMobReferenceCell(kv)) {
          continue;
        }
        if (!MobUtils.hasValidMobRefCellValue(kv)) {
          continue;
        }
        int size = MobUtils.getMobValueLength(kv);
        if (size <= mobCellThreshold) {
          continue;
        }
        fileName = MobUtils.getMobFileName(kv);
        if (fileName.isEmpty()) {
          continue;
        }
        files.add(fileName);
        Path familyPath = MobUtils.getMobFamilyPath(conf, tableDescriptor.getTableName(),
            familyDescriptor.getNameAsString());
        assertTrue(fs.exists(new Path(familyPath, fileName)));
      }
    } while (hasMore);

    scanner.close();

    return files.size();
  }

}
