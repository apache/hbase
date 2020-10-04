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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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

@Category({IOTests.class, SmallTests.class})
public class TestHFilePrettyPrinter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFilePrettyPrinter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFilePrettyPrinter.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static FileSystem fs;
  private static Configuration conf;
  private static byte [] cf = Bytes.toBytes("cf");
  private static byte [] fam = Bytes.toBytes("fam");
  private static byte [] value = Bytes.toBytes("val");
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
    Path fileNotInRootDir =  UTIL.getDataTestDir("hfile");
    TestHRegionServerBulkLoad.createHFile(fs, fileNotInRootDir, cf, fam, value, 1000);
    assertNotEquals("directory used is not an HBase root dir",
      UTIL.getDefaultRootDirPath(), fileNotInRootDir);

    System.setOut(ps);
    new HFilePrettyPrinter(conf).run(new String[]{"-v", String.valueOf(fileNotInRootDir)});
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
    assertTrue("directory used is a root dir",
      fileInRootDir.toString().startsWith(rootString));

    System.setOut(ps);
    HFilePrettyPrinter printer = new HFilePrettyPrinter();
    printer.setConf(conf);
    printer.processFile(fileInRootDir, true);
    printer.run(new String[]{"-v", String.valueOf(fileInRootDir)});
    String result = new String(stream.toByteArray());
    String expectedResult = "Scanning -> " + fileInRootDir + "\n" + "Scanned kv count -> 1000\n";
    assertEquals(expectedResult, result);
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
    String expectedResult = "Scanning -> " + fileNotInRootDir + "\n" + "Scanned kv count -> 1\n";
    assertEquals(expectedResult, result);
  }
}
