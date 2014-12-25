/**
 *
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

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test mob compaction
 */
@Category(MediumTests.class)
public class TestMobCompaction {
  @Rule
  public TestName name = new TestName();
  static final Log LOG = LogFactory.getLog(TestMobCompaction.class.getName());
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private Configuration conf = null;

  private HRegion region = null;
  private HTableDescriptor htd = null;
  private HColumnDescriptor hcd = null;
  private long mobCellThreshold = 1000;

  private FileSystem fs;

  private static final byte[] COLUMN_FAMILY = fam1;
  private final byte[] STARTROW = Bytes.toBytes(START_KEY);
  private int compactionThreshold;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void init(Configuration conf, long mobThreshold) throws Exception {
    this.conf = conf;
    this.mobCellThreshold = mobThreshold;
    HBaseTestingUtility UTIL = new HBaseTestingUtility(conf);

    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);
    htd = UTIL.createTableDescriptor(name.getMethodName());
    hcd = new HColumnDescriptor(COLUMN_FAMILY);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobThreshold);
    hcd.setMaxVersions(1);
    htd.addFamily(hcd);

    region = UTIL.createLocalHRegion(htd, null, null);
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
    HRegionIncommon loader = new HRegionIncommon(region);
    // one hfile per row
    for (int i = 0; i < compactionThreshold; i++) {
      Put p = createPut(i, dummyData);
      loader.put(p);
      loader.flushcache();
    }
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", 0, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, countRows());
    assertEquals("Before compaction: mob rows", 0, countMobRows());

    region.compactStores();

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count", 0, countMobFiles());
    assertEquals("After compaction: referenced mob file count", 0, countReferencedMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, countRows());
    assertEquals("After compaction: mob rows", 0, countMobRows());
  }

  /**
   * During compaction, the mob threshold size is changed.
   */
  @Test
  public void testLargerValue() throws Exception {
    init(UTIL.getConfiguration(), 200);
    byte[] dummyData = makeDummyData(300); // larger than mob threshold
    HRegionIncommon loader = new HRegionIncommon(region);
    for (int i = 0; i < compactionThreshold; i++) {
      Put p = createPut(i, dummyData);
      loader.put(p);
      loader.flushcache();
    }
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", compactionThreshold, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, countRows());
    assertEquals("Before compaction: mob rows", compactionThreshold, countMobRows());
    assertEquals("Before compaction: number of mob cells", compactionThreshold,
        countMobCellsInMetadata());
    // Change the threshold larger than the data size
    region.getTableDesc().getFamily(COLUMN_FAMILY).setMobThreshold(500);
    region.initialize();
    region.compactStores();

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count", compactionThreshold, countMobFiles());
    assertEquals("After compaction: referenced mob file count", 0, countReferencedMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, countRows());
    assertEquals("After compaction: mob rows", 0, countMobRows());
  }

  /**
   * This test will first generate store files, then bulk load them and trigger the compaction. When
   * compaction, the cell value will be larger than the threshold.
   */
  @Test
  public void testMobCompactionWithBulkload() throws Exception {
    // The following will produce store files of 600.
    init(UTIL.getConfiguration(), 300);
    byte[] dummyData = makeDummyData(600);

    Path hbaseRootDir = FSUtils.getRootDir(conf);
    Path basedir = new Path(hbaseRootDir, htd.getNameAsString());
    List<Pair<byte[], String>> hfiles = new ArrayList<Pair<byte[], String>>(1);
    for (int i = 0; i < compactionThreshold; i++) {
      Path hpath = new Path(basedir, "hfile" + i);
      hfiles.add(Pair.newPair(COLUMN_FAMILY, hpath.toString()));
      createHFile(hpath, i, dummyData);
    }

    // The following will bulk load the above generated store files and compact, with 600(fileSize)
    // > 300(threshold)
    boolean result = region.bulkLoadHFiles(hfiles, true);
    assertTrue("Bulkload result:", result);
    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    assertEquals("Before compaction: mob file count", 0, countMobFiles());
    assertEquals("Before compaction: rows", compactionThreshold, countRows());
    assertEquals("Before compaction: mob rows", 0, countMobRows());
    assertEquals("Before compaction: referenced mob file count", 0, countReferencedMobFiles());

    region.compactStores();

    assertEquals("After compaction: store files", 1, countStoreFiles());
    assertEquals("After compaction: mob file count:", 1, countMobFiles());
    assertEquals("After compaction: rows", compactionThreshold, countRows());
    assertEquals("After compaction: mob rows", compactionThreshold, countMobRows());
    assertEquals("After compaction: referenced mob file count", 1, countReferencedMobFiles());
    assertEquals("After compaction: number of mob cells", compactionThreshold,
        countMobCellsInMetadata());
  }

  /**
   * Tests the major compaction when the zk is not connected.
   * After that the major compaction will be marked as retainDeleteMarkers, the delete marks
   * will be retained.
   * @throws Exception
   */
  @Test
  public void testMajorCompactionWithZKError() throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // use the wrong zk settings
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 100);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181) - 1);
    init(conf, 200);
    byte[] dummyData = makeDummyData(300); // larger than mob threshold
    HRegionIncommon loader = new HRegionIncommon(region);
    byte[] deleteRow = Bytes.toBytes(0);
    for (int i = 0; i < compactionThreshold - 1 ; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.setDurability(Durability.SKIP_WAL);
      p.add(COLUMN_FAMILY, Bytes.toBytes("colX"), dummyData);
      loader.put(p);
      loader.flushcache();
    }
    Delete delete = new Delete(deleteRow);
    delete.deleteFamily(COLUMN_FAMILY);
    region.delete(delete);
    loader.flushcache();

    assertEquals("Before compaction: store files", compactionThreshold, countStoreFiles());
    region.compactStores(true);
    assertEquals("After compaction: store files", 1, countStoreFiles());

    Scan scan = new Scan();
    scan.setRaw(true);
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    scanner.next(results);
    int deleteCount = 0;
    while (!results.isEmpty()) {
      for (Cell c : results) {
        if (c.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
          deleteCount++;
          assertTrue(Bytes.equals(CellUtil.cloneRow(c), deleteRow));
        }
      }
      results.clear();
      scanner.next(results);
    }
    // assert the delete mark is retained, the major compaction is marked as
    // retainDeleteMarkers.
    assertEquals(1, deleteCount);
    scanner.close();
  }

  private int countStoreFiles() throws IOException {
    Store store = region.getStore(COLUMN_FAMILY);
    return store.getStorefilesCount();
  }

  private int countMobFiles() throws IOException {
    Path mobDirPath = new Path(MobUtils.getMobRegionPath(conf, htd.getTableName()),
        hcd.getNameAsString());
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = UTIL.getTestFileSystem().listStatus(mobDirPath);
      return files.length;
    }
    return 0;
  }

  private long countMobCellsInMetadata() throws IOException {
    long mobCellsCount = 0;
    Path mobDirPath = new Path(MobUtils.getMobRegionPath(conf, htd.getTableName()),
        hcd.getNameAsString());
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    CacheConfig cacheConfig = new CacheConfig(copyOfConf);
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = UTIL.getTestFileSystem().listStatus(mobDirPath);
      for (FileStatus file : files) {
        StoreFile sf = new StoreFile(fs, file.getPath(), conf, cacheConfig, BloomType.NONE);
        Map<byte[], byte[]> fileInfo = sf.createReader().loadFileInfo();
        byte[] count = fileInfo.get(StoreFile.MOB_CELLS_COUNT);
        assertTrue(count != null);
        mobCellsCount += Bytes.toLong(count);
      }
    }
    return mobCellsCount;
  }

  private Put createPut(int rowIdx, byte[] dummyData) throws IOException {
    Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(rowIdx)));
    p.setDurability(Durability.SKIP_WAL);
    p.add(COLUMN_FAMILY, Bytes.toBytes("colX"), dummyData);
    return p;
  }

  /**
   * Create an HFile with the given number of bytes
   */
  private void createHFile(Path path, int rowIdx, byte[] dummyData) throws IOException {
    HFileContext meta = new HFileContextBuilder().build();
    HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, path)
        .withFileContext(meta).create();
    long now = System.currentTimeMillis();
    try {
      KeyValue kv = new KeyValue(Bytes.add(STARTROW, Bytes.toBytes(rowIdx)), COLUMN_FAMILY,
          Bytes.toBytes("colX"), now, dummyData);
      writer.append(kv);
    } finally {
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
      writer.close();
    }
  }

  private int countMobRows() throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = region.getScanner(scan);

    int scannedCount = 0;
    List<Cell> results = new ArrayList<Cell>();
    boolean hasMore = scanner.next(results);
    while (hasMore) {
      for (Cell c : results) {
        if (MobUtils.isMobReferenceCell(c)) {
          scannedCount++;
        }
      }
      hasMore = scanner.next(results);
    }
    scanner.close();

    return scannedCount;
  }

  private int countRows() throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    InternalScanner scanner = region.getScanner(scan);

    int scannedCount = 0;
    List<Cell> results = new ArrayList<Cell>();
    boolean hasMore = scanner.next(results);
    while (hasMore) {
      scannedCount += results.size();
      hasMore = scanner.next(results);
    }
    scanner.close();

    return scannedCount;
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    new Random().nextBytes(dummyData);
    return dummyData;
  }

  private int countReferencedMobFiles() throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    InternalScanner scanner = region.getScanner(scan);

    List<Cell> kvs = new ArrayList<Cell>();
    boolean hasMore = true;
    String fileName;
    Set<String> files = new HashSet<String>();
    do {
      kvs.clear();
      hasMore = scanner.next(kvs);
      for (Cell c : kvs) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(c);
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
        Path familyPath = MobUtils.getMobFamilyPath(conf, htd.getTableName(),
            hcd.getNameAsString());
        assertTrue(fs.exists(new Path(familyPath, fileName)));
      }
    } while (hasMore);

    scanner.close();

    return files.size();
  }
}
