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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotArchiver;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Filesystem-level tests for {@link MapreduceHFileArchiver}, the MapReduce-local fork of
 * {@code HFileArchiver}. They confirm it implements {@link RestoreSnapshotArchiver} and that its
 * region/family archiving moves store files into the archive directory while removing the source.
 */
@Tag(MapReduceTests.TAG)
@Tag(MediumTests.TAG)
public class TestMapreduceHFileArchiver {

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;

  @BeforeEach
  public void setup() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    rootDir = TEST_UTIL.getDataTestDir("testMrArchiver-" + UUID.randomUUID());
    fs = rootDir.getFileSystem(conf);
    CommonFSUtils.setRootDir(conf, rootDir);
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(rootDir, true);
  }

  @Test
  public void testImplementsRestoreSnapshotArchiver() {
    assertTrue(new MapreduceHFileArchiver() instanceof RestoreSnapshotArchiver);
  }

  @Test
  public void testArchiveFamilyByFamilyDir() throws IOException {
    TableName tableName = TableName.valueOf("testArchiveFamily");
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    byte[] family = Bytes.toBytes("cf");
    Path familyDir = new Path(FSUtils.getRegionDirFromRootDir(rootDir, region), "cf");
    Path storeFile = new Path(familyDir, "f1");
    writeFile(storeFile);
    assertTrue(fs.exists(storeFile));

    new MapreduceHFileArchiver().archiveFamilyByFamilyDir(fs, conf, region, familyDir, family);

    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, region, family);
    assertTrue(fs.exists(new Path(storeArchiveDir, "f1")), "store file should be moved to archive");
    assertFalse(fs.exists(storeFile), "source store file should be gone after archiving");
  }

  @Test
  public void testArchiveRegion() throws IOException {
    TableName tableName = TableName.valueOf("testArchiveRegion");
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, region);
    Path storeFile = new Path(new Path(regionDir, "cf"), "f1");
    writeFile(storeFile);
    assertTrue(fs.exists(storeFile));

    new MapreduceHFileArchiver().archiveRegion(conf, fs, region, rootDir, tableDir);

    assertFalse(fs.exists(regionDir), "region dir should be removed after archiving");
    Path regionArchiveDir =
      HFileArchiveUtil.getRegionArchiveDir(rootDir, tableName, regionDir.getName());
    assertTrue(fs.exists(new Path(new Path(regionArchiveDir, "cf"), "f1")),
      "store file should be moved to the region archive");
  }

  private void writeFile(Path path) throws IOException {
    fs.mkdirs(path.getParent());
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(Bytes.toBytes("data"));
    }
  }
}
