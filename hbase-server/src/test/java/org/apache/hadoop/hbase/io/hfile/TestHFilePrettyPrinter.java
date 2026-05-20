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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codahale.metrics.MetricRegistry;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestHFilePrettyPrinter {
  private static final Logger LOG = LoggerFactory.getLogger(TestHFilePrettyPrinter.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static FileSystem fs;
  private static Configuration conf;
  private static byte[] cf = Bytes.toBytes("cf");
  private static byte[] fam = Bytes.toBytes("fam");
  private static byte[] value = Bytes.toBytes("val");
  private static PrintStream original;
  private static PrintStream ps;
  private static ByteArrayOutputStream stream;

  @BeforeEach
  public void setup() throws Exception {
    conf = UTIL.getConfiguration();
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    fs = UTIL.getTestFileSystem();
    stream = new ByteArrayOutputStream();
    ps = new PrintStream(stream);
  }

  @AfterEach
  public void teardown() {
    original = System.out;
    System.setOut(original);
  }

  @Test
  public void testHFilePrettyPrinterNonRootDir() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals(UTIL.getDefaultRootDirPath(), fileNotInRootDir,
      "directory used is not an HBase root dir");

    System.setOut(ps);
    new HFilePrettyPrinter(conf).run(new String[] { "-v", String.valueOf(fileNotInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedResult = "Scanning -> " + fileNotInRootDir + "\n" + "Scanned kv count -> 1000\n";
    assertEquals(expectedResult, result);
  }

  @Test
  public void testHFilePrettyPrinterRootDir() throws Exception {
    Path rootPath = CommonFSUtils.getRootDir(conf);
    String rootString = rootPath + rootPath.SEPARATOR;
    Path fileInRootDir = new Path(rootString + "hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileInRootDir, cf, fam, value, 1000);
    assertTrue(fileInRootDir.toString().startsWith(rootString), "directory used is a root dir");

    System.setOut(ps);
    HFilePrettyPrinter printer = new HFilePrettyPrinter();
    printer.setConf(conf);
    printer.processFile(fileInRootDir, true);
    printer.run(new String[] { "-v", String.valueOf(fileInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedResult = "Scanning -> " + fileInRootDir + "\n" + "Scanned kv count -> 1000\n";
    assertEquals(expectedResult, result);
  }

  @Test
  public void testHFilePrettyPrinterSeekFirstRow() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals(UTIL.getDefaultRootDirPath(), fileNotInRootDir,
      "directory used is not an HBase root dir");

    HFile.Reader reader =
      HFile.createReader(fs, fileNotInRootDir, CacheConfig.DISABLED, true, conf);
    String firstRowKey = new String(reader.getFirstRowKey().get());

    System.setOut(ps);
    new HFilePrettyPrinter(conf)
      .run(new String[] { "-v", "-w" + firstRowKey, String.valueOf(fileNotInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedResult = "Scanning -> " + fileNotInRootDir + "\n" + "Scanned kv count -> 1\n";
    assertEquals(expectedResult, result);
  }

  @Test
  public void testHistograms() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals(UTIL.getDefaultRootDirPath(), fileNotInRootDir,
      "directory used is not an HBase root dir");

    System.setOut(ps);
    new HFilePrettyPrinter(conf).run(new String[] { "-s", "-d", String.valueOf(fileNotInRootDir) });
    String result = stream.toString();
    LOG.info(result);

    // split out the output into sections based on the headers
    String[] headers =
      new String[] { "Key length", "Val length", "Row size (bytes)", "Row size (columns)" };
    // for each section, there is a corresponding expected (count, range) pairs
    int[][] expectations = new int[][] { new int[] { 0, 10, 1000, 50 }, new int[] { 0, 1, 1000, 3 },
      new int[] { 0, 10, 1000, 50 }, new int[] { 1000, 1 }, };

    for (int i = 0; i < headers.length - 1; i++) {
      int idx = result.indexOf(headers[i]);
      int nextIdx = result.indexOf(headers[i + 1]);

      assertContainsRanges(result.substring(idx, nextIdx), expectations[i]);
    }
  }

  private void assertContainsRanges(String result, int... rangeCountPairs) {
    for (int i = 0; i < rangeCountPairs.length - 1; i += 2) {
      String expected = rangeCountPairs[i + 1] + " <= " + rangeCountPairs[i];
      assertTrue(result.contains(expected),
        "expected:\n" + result + "\nto contain: '" + expected + "'");
    }
  }

  @Test
  public void testKeyValueStats() {
    HFilePrettyPrinter.KeyValueStats stats =
      new HFilePrettyPrinter.KeyValueStats(new MetricRegistry(), "test");
    long[] ranges = stats.getRanges();
    for (long range : ranges) {
      stats.update(range - 1, true);
    }

    assertEquals(ranges[ranges.length - 1] - 1, stats.getMax());
    assertEquals(ranges[0] - 1, stats.getMin());

    int total = 1;
    for (long range : ranges) {
      long val = stats.getCountAtOrBelow(range);
      assertEquals(total++, val);
    }

  }
}
