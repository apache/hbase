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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Test DataBlockEncodingTool.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestDataBlockEncodingTool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDataBlockEncodingTool.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String ROOT_DIR =
      TEST_UTIL.getDataTestDir("TestDataBlockEncodingTool").toString();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static FileSystem fs;
  private static StoreFileWriter sfw;

  @Before
  public void setUp() throws IOException {
    fs = TEST_UTIL.getTestFileSystem();
  }

  private void testHFile(String fileName, boolean useTags, boolean allTags) throws IOException {
    Path path = new Path(ROOT_DIR, fileName);
    try {
      createHFileWithTags(path, useTags, allTags);
      testDataBlockingTool(path);
    } finally {
      if (fs.exists(path)) {
        fs.delete(path, false);
      }
    }
  }

  private void createHFileWithTags(Path path, boolean useTags, boolean allTags) throws IOException {
    HFileContext meta = new HFileContextBuilder()
        .withBlockSize(64 * 1024)
        .withIncludesTags(useTags).build();
    sfw =
        new StoreFileWriter.Builder(conf, fs)
            .withFilePath(path)
            .withFileContext(meta).build();
    long now = System.currentTimeMillis();
    byte[] FAMILY = Bytes.toBytes("cf");
    byte[] QUALIFIER = Bytes.toBytes("q");
    try {
      for (char d = 'a'; d <= 'z'; d++) {
        for (char e = 'a'; e <= 'z'; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          KeyValue kv;
          if (useTags) {
            if (allTags) {
              // Write cells with tags to HFile.
              Tag[] tags = new Tag[]{
                new ArrayBackedTag((byte) 0, Bytes.toString(b)),
                new ArrayBackedTag((byte) 0, Bytes.toString(b))};
              kv = new KeyValue(b, FAMILY, QUALIFIER, now, b, tags);
            } else {
              // Write half cells with tags and half without tags to HFile.
              if ((e - 'a') % 2 == 0) {
                kv = new KeyValue(b, FAMILY, QUALIFIER, now, b);
              } else {
                Tag[] tags = new Tag[]{
                  new ArrayBackedTag((byte) 0, Bytes.toString(b)),
                  new ArrayBackedTag((byte) 0, Bytes.toString(b))};
                kv = new KeyValue(b, FAMILY, QUALIFIER, now, b, tags);
              }
            }
          } else {
            // Write cells without tags to HFile.
            kv = new KeyValue(b, FAMILY, QUALIFIER, now, b);
          }
          sfw.append(kv);
        }
      }
      sfw.appendMetadata(0, false);
    } finally {
      sfw.close();
    }
  }

  private static void testDataBlockingTool(Path path) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    int maxKV = Integer.MAX_VALUE;
    boolean doVerify = true;
    boolean doBenchmark = true;
    String testHFilePath = path.toString();
    DataBlockEncodingTool.testCodecs(conf, maxKV, testHFilePath,
        Compression.Algorithm.GZ.getName(), doBenchmark, doVerify);
  }

  @Test
  public void testHFileAllCellsWithTags() throws IOException {
    testHFile("1234567890", true, true);
  }

  @Test
  public void testHFileAllCellsWithoutTags() throws IOException {
    testHFile("1234567089", false, false);
  }

  @Test
  public void testHFileHalfCellsWithTags() throws IOException {
    testHFile("1234560789", true, false);
  }
}