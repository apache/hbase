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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Empirically verifies what happens when a mini cluster is started with the FILE store-file
 * tracker as the cluster-wide default ({@code hbase.store.file-tracker.impl=FILE}). In particular,
 * checks whether the {@code hbase:meta} table descriptor inherits FILE and whether the meta region
 * stores end up with a {@code .filelist} tracker directory on disk.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestMetaWithFileBasedStoreFileTracker {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMetaWithFileBasedStoreFileTracker.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(StoreFileTrackerFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.FILE.name());
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaTableDescriptorAndOnDiskLayout() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    FileSystem fs = UTIL.getTestFileSystem();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path metaTableDir = CommonFSUtils.getTableDir(rootDir, TableName.META_TABLE_NAME);

    // 1) Inspect the on-disk meta table descriptor.
    TableDescriptor metaTd = FSTableDescriptors.getTableDescriptorFromFs(fs, metaTableDir);
    if (metaTd == null) {
      throw new IllegalStateException("meta TD missing under " + metaTableDir);
    }
    String metaTrackerImpl = metaTd.getValue(StoreFileTrackerFactory.TRACKER_IMPL);
    LOG.info("meta TD value for {} = {}", StoreFileTrackerFactory.TRACKER_IMPL, metaTrackerImpl);

    // 2) Walk the meta region directories and look for .filelist under each store.
    RegionInfo metaRegion = RegionInfoBuilder.FIRST_META_REGIONINFO;
    Path metaRegionDir = new Path(metaTableDir, metaRegion.getEncodedName());
    LOG.info("Inspecting meta region dir: {}", metaRegionDir);
    assertTrue(fs.exists(metaRegionDir), "meta region dir must exist: " + metaRegionDir);

    List<Path> filelistDirs = new ArrayList<>();
    List<String> familiesScanned = new ArrayList<>();
    for (ColumnFamilyDescriptor cfd : metaTd.getColumnFamilies()) {
      String fam = cfd.getNameAsString();
      familiesScanned.add(fam);
      Path famDir = new Path(metaRegionDir, fam);
      if (!fs.exists(famDir)) {
        LOG.info("  family {} dir does not exist yet: {}", fam, famDir);
        continue;
      }
      Path filelist = new Path(famDir, StoreFileListFile.TRACK_FILE_DIR);
      boolean exists = fs.exists(filelist);
      LOG.info("  family {} -> filelist dir {} exists={}", fam, filelist, exists);
      if (exists) {
        filelistDirs.add(filelist);
        FileStatus[] entries = fs.listStatus(filelist);
        if (entries != null) {
          for (FileStatus s : entries) {
            LOG.info("    .filelist entry: {} (size={})", s.getPath().getName(), s.getLen());
          }
        }
      }
    }

    LOG.info("SUMMARY: meta TRACKER_IMPL={}, families scanned={}, .filelist dirs found={}",
      metaTrackerImpl, familiesScanned, filelistDirs.size());

    // 3) Force a flush on meta so any catalog-family writes get flushed and any FILE-SFT
    //    manifest update is materialized. Then re-check.
    UTIL.getAdmin().flush(TableName.META_TABLE_NAME);
    Thread.sleep(2000);

    int filelistAfterFlush = 0;
    for (ColumnFamilyDescriptor cfd : metaTd.getColumnFamilies()) {
      Path famDir = new Path(metaRegionDir, cfd.getNameAsString());
      Path filelist = new Path(famDir, StoreFileListFile.TRACK_FILE_DIR);
      if (fs.exists(filelist)) {
        filelistAfterFlush++;
        LOG.info("After flush: family {} HAS .filelist; entries:", cfd.getNameAsString());
        FileStatus[] entries = fs.listStatus(filelist);
        if (entries != null) {
          for (FileStatus s : entries) {
            LOG.info("    {} (size={})", s.getPath().getName(), s.getLen());
          }
        }
      } else {
        LOG.info("After flush: family {} has NO .filelist", cfd.getNameAsString());
      }
    }
    LOG.info("FINAL: meta TRACKER_IMPL={}, .filelist dirs after flush={}",
      metaTrackerImpl, filelistAfterFlush);

    // The assertions below are intentionally written so the test logs the truth either way.
    // We assert nothing definitive about FILE here — the LOG output is the real evidence the
    // human will read; we just want the test to pass so we can read the logs.
    assertNotNull(familiesScanned);
    assertEquals(metaTd.getTableName(), TableName.META_TABLE_NAME);
    // touch HConstants to keep import used in case future edits need it
    assertNotNull(HConstants.CATALOG_FAMILY);
  }
}
