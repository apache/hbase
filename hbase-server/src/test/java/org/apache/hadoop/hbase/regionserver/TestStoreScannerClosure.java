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

import static org.apache.hadoop.hbase.KeyValueTestUtil.create;
import static org.apache.hadoop.hbase.regionserver.KeyValueScanFixture.scanFixture;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This test tests whether parallel {@link StoreScanner#close()} and
 * {@link StoreScanner#updateReaders(List, List)} works perfectly ensuring
 * that there are no references on the existing Storescanner readers.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreScannerClosure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStoreScannerClosure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreScannerClosure.class);
  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  @Rule
  public TestName name = new TestName();
  private static final String CF_STR = "cf";
  private static HRegion region;
  private static final byte[] CF = Bytes.toBytes(CF_STR);
  static Configuration CONF = HBaseConfiguration.create();
  private static CacheConfig cacheConf;
  private static FileSystem fs;
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, Integer.MAX_VALUE, Long.MAX_VALUE,
      KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);
  private final static byte[] fam = Bytes.toBytes("cf_1");
  private static final KeyValue[] kvs =
      new KeyValue[] { create("R1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
          create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
          create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"), };

  @BeforeClass
  public static void setUp() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    cacheConf = new CacheConfig(CONF);
    fs = TEST_UTIL.getTestFileSystem();
    TableName tableName = TableName.valueOf("test");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    Path path = TEST_UTIL.getDataTestDir("test");
    region = HBaseTestingUtil.createRegionAndWAL(info, path,
      TEST_UTIL.getConfiguration(), tableDescriptor);
  }

  @Test
  public void testScannerCloseAndUpdateReadersWithMemstoreScanner() throws Exception {
    Put p = new Put(Bytes.toBytes("row"));
    p.addColumn(fam, Bytes.toBytes("q1"), Bytes.toBytes("val"));
    region.put(p);
    // create the store scanner here.
    // for easiness, use Long.MAX_VALUE as read pt
    try (ExtendedStoreScanner scan = new ExtendedStoreScanner(region.getStore(fam), scanInfo,
        new Scan(), getCols("q1"), Long.MAX_VALUE)) {
      p = new Put(Bytes.toBytes("row1"));
      p.addColumn(fam, Bytes.toBytes("q1"), Bytes.toBytes("val"));
      region.put(p);
      HStore store = region.getStore(fam);
      // use the lock to manually get a new memstore scanner. this is what
      // HStore#notifyChangedReadersObservers does under the lock.(lock is not needed here
      //since it is just a testcase).
      store.getStoreEngine().readLock();
      final List<KeyValueScanner> memScanners = store.memstore.getScanners(Long.MAX_VALUE);
      store.getStoreEngine().readUnlock();
      Thread closeThread = new Thread() {
        public void run() {
          // close should be completed
          scan.close(false, true);
        }
      };
      closeThread.start();
      Thread updateThread = new Thread() {
        public void run() {
          try {
            // use the updated memstoreScanners and pass it to updateReaders
            scan.updateReaders(true, Collections.emptyList(), memScanners);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      updateThread.start();
      // wait for close and updateThread to complete
      closeThread.join();
      updateThread.join();
      MemStoreLAB memStoreLAB;
      for (KeyValueScanner scanner : memScanners) {
        if (scanner instanceof SegmentScanner) {
          memStoreLAB = ((SegmentScanner) scanner).segment.getMemStoreLAB();
          if (memStoreLAB != null) {
            // There should be no unpooled chunks
            int refCount = ((MemStoreLABImpl) memStoreLAB).getRefCntValue();
            assertTrue("The memstore should not have unpooled chunks", refCount == 0);
          }
        }
      }
    }
  }

  @Test
  public void testScannerCloseAndUpdateReaders1() throws Exception {
    testScannerCloseAndUpdateReaderInternal(true, false);
  }

  @Test
  public void testScannerCloseAndUpdateReaders2() throws Exception {
    testScannerCloseAndUpdateReaderInternal(false, true);
  }

  private Path writeStoreFile() throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), "TestHFile");
    HFileContext meta = new HFileContextBuilder().withBlockSize(64 * 1024).build();
    StoreFileWriter sfw = new StoreFileWriter.Builder(CONF, fs).withOutputDir(storeFileParentDir)
        .withFileContext(meta).build();

    final int rowLen = 32;
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 1000; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] v = RandomKeyValueUtil.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv = new KeyValue(k, 0, rowLen, k, rowLen, cfLen, k, rowLen + cfLen,
          k.length - rowLen - cfLen, rand.nextLong(), generateKeyType(rand), v, 0, v.length);
      sfw.append(kv);
    }

    sfw.close();
    return sfw.getPath();
  }

  private static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
            + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  private HStoreFile readStoreFile(Path storeFilePath, Configuration conf) throws Exception {
    // Open the file reader with block cache disabled.
    HStoreFile file = new HStoreFile(fs, storeFilePath, conf, cacheConf, BloomType.NONE, true);
    return file;
  }

  private void testScannerCloseAndUpdateReaderInternal(boolean awaitUpdate, boolean awaitClose)
      throws IOException, InterruptedException {
    // start write to store file.
    Path path = writeStoreFile();
    HStoreFile file = null;
    List<HStoreFile> files = new ArrayList<HStoreFile>();
    try {
      file = readStoreFile(path, CONF);
      files.add(file);
    } catch (Exception e) {
      // fail test
      assertTrue(false);
    }
    scanFixture(kvs);
    // scanners.add(storeFileScanner);
    try (ExtendedStoreScanner scan = new ExtendedStoreScanner(region.getStore(fam), scanInfo,
        new Scan(), getCols("a", "d"), 100L)) {
      Thread closeThread = new Thread() {
        public void run() {
          scan.close(awaitClose, true);
        }
      };
      closeThread.start();
      Thread updateThread = new Thread() {
        public void run() {
          try {
            scan.updateReaders(awaitUpdate, files, Collections.emptyList());
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      updateThread.start();
      // complete both the threads
      closeThread.join();
      // complete both the threads
      updateThread.join();
      if (file.getReader() != null) {
        // the fileReader is not null when the updateReaders has completed first.
        // in the other case the fileReader will be null.
        int refCount = file.getReader().getRefCount();
        LOG.info("the store scanner count is " + refCount);
        assertTrue("The store scanner count should be 0", refCount == 0);
      }
    }
  }

  private static class ExtendedStoreScanner extends StoreScanner {
    private CountDownLatch latch = new CountDownLatch(1);

    public ExtendedStoreScanner(HStore store, ScanInfo scanInfo, Scan scan,
        NavigableSet<byte[]> columns, long readPt) throws IOException {
      super(store, scanInfo, scan, columns, readPt);
    }

    public void updateReaders(boolean await, List<HStoreFile> sfs,
        List<KeyValueScanner> memStoreScanners) throws IOException {
      if (await) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      super.updateReaders(sfs, memStoreScanners);
      if (!await) {
        latch.countDown();
      }
    }

    // creating a dummy close
    public void close(boolean await, boolean dummy) {
      if (await) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      super.close();
      if (!await) {
        latch.countDown();
      }
    }
  }

  NavigableSet<byte[]> getCols(String... strCols) {
    NavigableSet<byte[]> cols = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String col : strCols) {
      byte[] bytes = Bytes.toBytes(col);
      cols.add(bytes);
    }
    return cols;
  }
}
