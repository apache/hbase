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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test TestRowPrefixBloomFilter
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestRowPrefixBloomFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRowPrefixBloomFilter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRowPrefixBloomFilter.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
  private static final ChecksumType CKTYPE = ChecksumType.CRC32C;
  private static final int CKBYTES = 512;
  private boolean localfs = false;
  private static Configuration conf;
  private static FileSystem fs;
  private static Path testDir;
  private static final int BLOCKSIZE_SMALL = 8192;
  private static final float err = (float) 0.01;
  private static final int prefixLength = 10;
  private static final String delimiter = "#";
  private static final String invalidFormatter = "%08d";
  private static final String prefixFormatter = "%010d";
  private static final String suffixFormatter = "%010d";

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setFloat(BloomFilterFactory.IO_STOREFILE_BLOOM_ERROR_RATE, err);
    conf.setBoolean(BloomFilterFactory.IO_STOREFILE_BLOOM_ENABLED, true);
    conf.setInt(BloomFilterUtil.PREFIX_LENGTH_KEY, prefixLength);
    conf.set(BloomFilterUtil.DELIMITER_KEY, delimiter);

    localfs =
        (conf.get("fs.defaultFS", "file:///").compareTo("file:///") == 0);

    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    try {
      if (localfs) {
        testDir = TEST_UTIL.getDataTestDir("TestRowPrefixBloomFilter");
        if (fs.exists(testDir)) {
          fs.delete(testDir, true);
        }
      } else {
        testDir = FSUtils.getRootDir(conf);
      }
    } catch (Exception e) {
      LOG.error(HBaseMarkers.FATAL, "error during setup", e);
      throw e;
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (localfs) {
        if (fs.exists(testDir)) {
          fs.delete(testDir, true);
        }
      }
    } catch (Exception e) {
      LOG.error(HBaseMarkers.FATAL, "error during tear down", e);
    }
  }

  private static StoreFileScanner getStoreFileScanner(StoreFileReader reader) {
    return reader.getStoreFileScanner(false, false, false, 0, 0, false);
  }

  private void writeStoreFile(final Path f, BloomType bt, int expKeys, int prefixRowCount,
      int suffixRowCount) throws IOException {
    HFileContext meta = new HFileContextBuilder()
        .withBlockSize(BLOCKSIZE_SMALL)
        .withChecksumType(CKTYPE)
        .withBytesPerCheckSum(CKBYTES)
        .build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs)
        .withFilePath(f)
        .withBloomType(bt)
        .withMaxKeyCount(expKeys)
        .withFileContext(meta)
        .build();
    long now = System.currentTimeMillis();
    try {
      //Put with valid row style
      for (int i = 0; i < prefixRowCount; i += 2) { // prefix rows
        String prefixRow = String.format(prefixFormatter, i);
        for (int j = 0; j < suffixRowCount; j++) {   // suffix rows
          String row = prefixRow + "#" + String.format(suffixFormatter, j);
          KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
              Bytes.toBytes("col"), now, Bytes.toBytes("value"));
          writer.append(kv);
        }
      }

      //Put with invalid row style
      for (int i = prefixRowCount; i < prefixRowCount*2; i += 2) { // prefix rows
        String row = String.format(invalidFormatter, i);
        KeyValue kv = new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"),
            Bytes.toBytes("col"), now, Bytes.toBytes("value"));
        writer.append(kv);
      }
    } finally {
      writer.close();
    }
  }

  @Test
  public void testRowPrefixBloomFilter() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    BloomType[] bt = {BloomType.ROWPREFIX_FIXED_LENGTH, BloomType.ROWPREFIX_DELIMITED};
    int prefixRowCount = 50;
    int suffixRowCount = 10;
    int expKeys = 50;
    float expErr = 2*prefixRowCount*suffixRowCount*err;
    for (int x : new int[]{0,1}) {
      // write the file
      Path f = new Path(testDir, name.getMethodName());
      writeStoreFile(f, bt[x], expKeys, prefixRowCount, suffixRowCount);

      // read the file
      StoreFileReader reader = new StoreFileReader(fs, f, cacheConf, true,
          new AtomicInteger(0), true, conf);
      reader.loadFileInfo();
      reader.loadBloomfilter();

      //check basic param
      assertEquals(bt[x], reader.getBloomFilterType());
      if (bt[x] == BloomType.ROWPREFIX_FIXED_LENGTH) {
        assertEquals(prefixLength, reader.getPrefixLength());
        assertEquals("null", Bytes.toStringBinary(reader.getDelimiter()));
      } else if (bt[x] == BloomType.ROWPREFIX_DELIMITED){
        assertEquals(-1, reader.getPrefixLength());
        assertEquals(delimiter, Bytes.toStringBinary(reader.getDelimiter()));
      }
      assertEquals(expKeys, reader.getGeneralBloomFilter().getKeyCount());
      StoreFileScanner scanner = getStoreFileScanner(reader);
      HStore store = mock(HStore.class);
      when(store.getColumnFamilyDescriptor())
          .thenReturn(ColumnFamilyDescriptorBuilder.of("family"));
      // check false positives rate
      int falsePos = 0;
      int falseNeg = 0;
      for (int i = 0; i < prefixRowCount; i++) { // prefix rows
        String prefixRow = String.format(prefixFormatter, i);
        for (int j = 0; j < suffixRowCount; j++) {   // suffix rows
          String startRow = prefixRow + "#" + String.format(suffixFormatter, j);
          String stopRow = prefixRow + "#" + String.format(suffixFormatter, j+1);
          Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow))
              .withStopRow(Bytes.toBytes(stopRow));
          boolean exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
          boolean shouldPrefixRowExist = i % 2 == 0;
          if (shouldPrefixRowExist) {
            if (!exists) {
              falseNeg++;
            }
          } else {
            if (exists) {
              falsePos++;
            }
          }
        }
      }

      for (int i = prefixRowCount; i < prefixRowCount * 2; i++) { // prefix rows
        String row = String.format(invalidFormatter, i);
        Scan scan = new Scan(new Get(Bytes.toBytes(row)));
        boolean exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
        boolean shouldPrefixRowExist = i % 2 == 0;
        if (shouldPrefixRowExist) {
          if (!exists) {
            falseNeg++;
          }
        } else {
          if (exists) {
            falsePos++;
          }
        }
      }
      reader.close(true); // evict because we are about to delete the file
      fs.delete(f, true);
      assertEquals("False negatives: " + falseNeg, 0, falseNeg);
      int maxFalsePos = (int) (2 * expErr);
      assertTrue("Too many false positives: " + falsePos
              + " (err=" + err + ", expected no more than " + maxFalsePos + ")",
          falsePos <= maxFalsePos);
    }
  }

  @Test
  public void testRowPrefixBloomFilterWithGet() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    BloomType[] bt = {BloomType.ROWPREFIX_FIXED_LENGTH, BloomType.ROWPREFIX_DELIMITED};
    int prefixRowCount = 50;
    int suffixRowCount = 10;
    int expKeys = 50;
    for (int x : new int[]{0,1}) {
      // write the file
      Path f = new Path(testDir, name.getMethodName());
      writeStoreFile(f, bt[x], expKeys, prefixRowCount, suffixRowCount);

      StoreFileReader reader = new StoreFileReader(fs, f, cacheConf, true,
          new AtomicInteger(0), true, conf);
      reader.loadFileInfo();
      reader.loadBloomfilter();

      StoreFileScanner scanner = getStoreFileScanner(reader);
      HStore store = mock(HStore.class);
      when(store.getColumnFamilyDescriptor())
          .thenReturn(ColumnFamilyDescriptorBuilder.of("family"));

      //Get with valid row style
      //prefix row in bloom
      String prefixRow = String.format(prefixFormatter, prefixRowCount-2);
      String row = prefixRow + "#" + String.format(suffixFormatter, 0);
      Scan scan = new Scan(new Get(Bytes.toBytes(row)));
      boolean exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertTrue(exists);

      // prefix row not in bloom
      prefixRow = String.format(prefixFormatter, prefixRowCount-1);
      row = prefixRow + "#" + String.format(suffixFormatter, 0);
      scan = new Scan(new Get(Bytes.toBytes(row)));
      exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertFalse(exists);

      // Get with invalid row style
      // ROWPREFIX: the length of row is less than prefixLength
      // ROWPREFIX_DELIMITED: Row does not contain delimiter
      // row in bloom
      row = String.format(invalidFormatter, prefixRowCount+2);
      scan = new Scan(new Get(Bytes.toBytes(row)));
      exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertTrue(exists);

      // row not in bloom
      row = String.format(invalidFormatter, prefixRowCount+1);
      scan = new Scan(new Get(Bytes.toBytes(row)));
      exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertFalse(exists);

      reader.close(true); // evict because we are about to delete the file
      fs.delete(f, true);
    }
  }

  @Test
  public void testRowPrefixBloomFilterWithScan() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    BloomType[] bt = {BloomType.ROWPREFIX_FIXED_LENGTH, BloomType.ROWPREFIX_DELIMITED};
    int prefixRowCount = 50;
    int suffixRowCount = 10;
    int expKeys = 50;
    for (int x : new int[]{0,1}) {
      // write the file
      Path f = new Path(testDir, name.getMethodName());
      writeStoreFile(f, bt[x], expKeys, prefixRowCount, suffixRowCount);

      StoreFileReader reader = new StoreFileReader(fs, f, cacheConf, true,
          new AtomicInteger(0), true, conf);
      reader.loadFileInfo();
      reader.loadBloomfilter();

      StoreFileScanner scanner = getStoreFileScanner(reader);
      HStore store = mock(HStore.class);
      when(store.getColumnFamilyDescriptor())
          .thenReturn(ColumnFamilyDescriptorBuilder.of("family"));

      //Scan with valid row style. startRow and stopRow have a common prefix.
      //And the length of the common prefix is no less than prefixLength.
      //prefix row in bloom
      String prefixRow = String.format(prefixFormatter, prefixRowCount-2);
      String startRow = prefixRow + "#" + String.format(suffixFormatter, 0);
      String stopRow = prefixRow + "#" + String.format(suffixFormatter, 1);
      Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow))
          .withStopRow(Bytes.toBytes(stopRow));
      boolean exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertTrue(exists);

      // prefix row not in bloom
      prefixRow = String.format(prefixFormatter, prefixRowCount-1);
      startRow = prefixRow + "#" + String.format(suffixFormatter, 0);
      stopRow = prefixRow + "#" + String.format(suffixFormatter, 1);
      scan = new Scan().withStartRow(Bytes.toBytes(startRow))
          .withStopRow(Bytes.toBytes(stopRow));
      exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertFalse(exists);

      // There is no common prefix between startRow and stopRow.
      prefixRow = String.format(prefixFormatter, prefixRowCount-2);
      startRow = prefixRow + "#" + String.format(suffixFormatter, 0);
      scan = new Scan().withStartRow(Bytes.toBytes(startRow));
      exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
      assertTrue(exists);

      if (bt[x] == BloomType.ROWPREFIX_FIXED_LENGTH) {
        // startRow and stopRow have a common prefix.
        // But the length of the common prefix is less than prefixLength.
        String prefixStartRow = String.format(prefixFormatter, prefixRowCount-2);
        String prefixStopRow = String.format(prefixFormatter, prefixRowCount-1);
        startRow = prefixStartRow + "#" + String.format(suffixFormatter, 0);
        stopRow = prefixStopRow + "#" + String.format(suffixFormatter, 0);
        scan = new Scan().withStartRow(Bytes.toBytes(startRow))
            .withStopRow(Bytes.toBytes(stopRow));
        exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
        assertTrue(exists);
      }else if (bt[x] == BloomType.ROWPREFIX_DELIMITED) {
        // startRow does not contain delimiter
        String prefixStartRow = String.format(prefixFormatter, prefixRowCount-2);
        String prefixStopRow = String.format(prefixFormatter, prefixRowCount-2);
        startRow = prefixStartRow + String.format(suffixFormatter, 0);
        stopRow = prefixStopRow + "#" + String.format(suffixFormatter, 0);
        scan = new Scan().withStartRow(Bytes.toBytes(startRow))
            .withStopRow(Bytes.toBytes(stopRow));
        exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
        assertTrue(exists);

        // startRow contains delimiter, but stopRow does not have the same prefix as startRow.
        prefixStartRow = String.format(prefixFormatter, prefixRowCount-2);
        prefixStopRow = String.format(prefixFormatter, prefixRowCount-1);
        startRow = prefixStartRow + "#" + String.format(suffixFormatter, 0);
        stopRow = prefixStopRow + "#" + String.format(suffixFormatter, 0);
        scan = new Scan().withStartRow(Bytes.toBytes(startRow))
            .withStopRow(Bytes.toBytes(stopRow));
        exists = scanner.shouldUseScanner(scan, store, Long.MIN_VALUE);
        assertTrue(exists);
      }

      reader.close(true); // evict because we are about to delete the file
      fs.delete(f, true);
    }
  }
}
