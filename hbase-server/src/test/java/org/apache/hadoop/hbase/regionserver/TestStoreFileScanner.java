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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test StoreFileScanner
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileScanner.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String TEST_FAMILY = "cf";

  @Rule
  public TestName name = new TestName();

  private Configuration conf;
  private Path testDir;
  private FileSystem fs;
  private CacheConfig cacheConf;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    testDir = TEST_UTIL.getDataTestDir(name.getMethodName());
    fs = testDir.getFileSystem(conf);
    cacheConf = new CacheConfig(conf);
  }

  private void writeStoreFile(final StoreFileWriter writer) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    byte[] family = Bytes.toBytes(TEST_FAMILY);
    byte[] qualifier = Bytes.toBytes("col");
    for (char d = 'a'; d <= 'z'; d++) {
      for (char e = 'a'; e <= 'z'; e++) {
        byte[] row = new byte[] { (byte) d, (byte) e };
        writer.append(new KeyValue(row, family, qualifier, now, row));
      }
    }
  }

  @Test
  public void testGetFilesRead() throws Exception {
    // Setup: region info, region fs, and HFile context; create store file and write data.
    final RegionInfo hri =
      RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(conf, fs,
      new Path(testDir, hri.getTable().getNameAsString()), hri);
    HFileContext hFileContext = new HFileContextBuilder().withBlockSize(8 * 1024).build();

    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withFilePath(regionFs.createTempName()).withFileContext(hFileContext).build();
    writeStoreFile(writer);
    Path hsfPath = regionFs.commitStoreFile(TEST_FAMILY, writer.getPath());
    writer.close();

    // Open HStoreFile and reader; get qualified path and create StoreFileScanner.
    StoreFileTracker sft = StoreFileTrackerFactory.create(conf, false,
      StoreContext.getBuilder()
        .withFamilyStoreDirectoryPath(new Path(regionFs.getRegionDir(), TEST_FAMILY))
        .withColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY))
        .withRegionFileSystem(regionFs).build());
    HStoreFile file = new HStoreFile(fs, hsfPath, conf, cacheConf, BloomType.NONE, true, sft);
    file.initReader();
    StoreFileReader r = file.getReader();
    assertNotNull(r);
    Path qualifiedPath = fs.makeQualified(hsfPath);
    StoreFileScanner scanner = r.getStoreFileScanner(false, false, false, 0, 0, false);

    // Before close: getFilesRead must be empty.
    Set<Path> filesRead = scanner.getFilesRead();
    assertTrue("Should return empty set before closing scanner", filesRead.isEmpty());

    scanner.close();

    // After close: set must contain the single qualified store file path.
    filesRead = scanner.getFilesRead();
    assertEquals("Should return set with one file path after closing", 1, filesRead.size());
    assertTrue("Should contain the qualified file path", filesRead.contains(qualifiedPath));
  }
}
