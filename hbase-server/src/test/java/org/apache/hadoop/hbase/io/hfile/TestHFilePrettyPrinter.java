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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestHFilePrettyPrinter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFilePrettyPrinter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFilePrettyPrinter.class);

  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static FileSystem fs;
  private static Configuration conf;
  private static byte[] cf = Bytes.toBytes("cf");
  private static byte[] fam = Bytes.toBytes("fam");
  private static byte[] value = Bytes.toBytes("val");
  private static PrintStream original;
  private static PrintStream ps;
  private static ByteArrayOutputStream stream;

  @Before
  public void setup() throws Exception {
    conf = UTIL.getConfiguration();
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    fs = UTIL.getTestFileSystem();
    stream = new ByteArrayOutputStream();
    ps = new PrintStream(stream);
  }

  @After
  public void teardown() {
    original = System.out;
    System.setOut(original);
  }

  @Test
  public void testHFilePrettyPrinterNonRootDir() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals("directory used is not an HBase root dir", UTIL.getDefaultRootDirPath(),
      fileNotInRootDir);

    System.setOut(ps);
    new HFilePrettyPrinter(conf).run(new String[] { "-v", String.valueOf(fileNotInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedFirstLine = "Scanning -> " + fileNotInRootDir + "\n";
    String expectedCountLine = "Scanned kv count -> 1000\n";
    assertTrue("expected to contain start line: '" + expectedFirstLine + "'",
      result.contains(expectedFirstLine));
    assertTrue("expected to contain count line: '" + expectedCountLine + "'",
      result.contains(expectedCountLine));
    assertTrue("expected start line to appear before count line",
      result.indexOf(expectedFirstLine) >= 0
        && result.indexOf(expectedCountLine) > result.indexOf(expectedFirstLine));
  }

  @Test
  public void testHFilePrettyPrinterRootDir() throws Exception {
    Path rootPath = CommonFSUtils.getRootDir(conf);
    String rootString = rootPath + Path.SEPARATOR;
    Path fileInRootDir = new Path(rootString + "hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileInRootDir, cf, fam, value, 1000);
    assertTrue("directory used is a root dir", fileInRootDir.toString().startsWith(rootString));

    System.setOut(ps);
    HFilePrettyPrinter printer = new HFilePrettyPrinter();
    printer.setConf(conf);
    printer.processFile(fileInRootDir, true);
    printer.run(new String[] { "-v", String.valueOf(fileInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedFirstLine = "Scanning -> " + fileInRootDir + "\n";
    String expectedCountLine = "Scanned kv count -> 1000\n";
    assertTrue("expected to contain start line: '" + expectedFirstLine + "'",
      result.contains(expectedFirstLine));
    assertTrue("expected to contain count line: '" + expectedCountLine + "'",
      result.contains(expectedCountLine));
    assertTrue("expected start line to appear before count line",
      result.indexOf(expectedFirstLine) >= 0
        && result.indexOf(expectedCountLine) > result.indexOf(expectedFirstLine));
  }

  @Test
  public void testHFilePrettyPrinterSeekFirstRow() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals("directory used is not an HBase root dir", UTIL.getDefaultRootDirPath(),
      fileNotInRootDir);

    HFile.Reader reader =
      HFile.createReader(fs, fileNotInRootDir, CacheConfig.DISABLED, true, conf);
    String firstRowKey = new String(reader.getFirstRowKey().get());

    System.setOut(ps);
    new HFilePrettyPrinter(conf)
      .run(new String[] { "-v", "-w" + firstRowKey, String.valueOf(fileNotInRootDir) });
    String result = new String(stream.toByteArray());
    String expectedFirstLine = "Scanning -> " + fileNotInRootDir + "\n";
    String expectedCountLine = "Scanned kv count -> 1\n";
    assertTrue("expected to contain start line: '" + expectedFirstLine + "'",
      result.contains(expectedFirstLine));
    assertTrue("expected to contain count line: '" + expectedCountLine + "'",
      result.contains(expectedCountLine));
    assertTrue("expected start line to appear before count line",
      result.indexOf(expectedFirstLine) >= 0
        && result.indexOf(expectedCountLine) > result.indexOf(expectedFirstLine));
  }

  @Test
  public void testHistograms() throws Exception {
    Path fileNotInRootDir = UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals("directory used is not an HBase root dir", UTIL.getDefaultRootDirPath(),
      fileNotInRootDir);

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
      assertTrue("expected:\n" + result + "\nto contain: '" + expected + "'",
        result.contains(expected));
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
