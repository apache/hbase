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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDataManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestSnapshotRegionLocator {

  private static final TableName TABLE_NAME = TableName.valueOf("testSnapshotRegionLocator");
  private static final byte[] SPLIT_KEY = Bytes.toBytes("m");
  private static final TableDescriptor TABLE_DESCRIPTOR = TableDescriptorBuilder
    .newBuilder(TABLE_NAME).setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;
  private Path snapshotDir;

  @BeforeEach
  public void setUp() throws IOException {
    HBaseTestingUtil testUtil = new HBaseTestingUtil();
    conf = testUtil.getConfiguration();
    fs = testUtil.getTestFileSystem();
    rootDir = testUtil.getDataTestDir(TABLE_NAME.getNameAsString());
    snapshotDir = new Path(rootDir, "snapshot");
    fs.mkdirs(snapshotDir);

    SnapshotDescription snapshot =
      SnapshotDescription.newBuilder().setName("snapshot").setTable(TABLE_NAME.getNameAsString())
        .setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION).build();
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, snapshotDir, fs);
    writeManifest();
    SnapshotRegionLocator.setSnapshotManifestDir(conf, snapshotDir.toString(), TABLE_NAME);
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(rootDir, true);
  }

  @Test
  public void testFiltersOfflineAndSplitRegions() throws IOException {
    try (SnapshotRegionLocator locator = SnapshotRegionLocator.create(conf, TABLE_NAME)) {
      List<HRegionLocation> locations = locator.getAllRegionLocations();
      assertEquals(2, locations.size());
      assertEquals(1, locations.get(0).getRegion().getRegionId());
      assertEquals(2, locations.get(1).getRegion().getRegionId());

      byte[][] startKeys = locator.getStartKeys();
      assertEquals(2, startKeys.length);
      assertArrayEquals(HConstants.EMPTY_START_ROW, startKeys[0]);
      assertArrayEquals(SPLIT_KEY, startKeys[1]);

      assertEquals(1, locator.getRegionLocation(Bytes.toBytes("a")).getRegion().getRegionId());
      assertEquals(2, locator.getRegionLocation(Bytes.toBytes("z")).getRegion().getRegionId());
    }
  }

  private void writeManifest() throws IOException {
    SnapshotDataManifest.Builder manifest = SnapshotDataManifest.newBuilder()
      .setTableSchema(ProtobufUtil.toTableSchema(TABLE_DESCRIPTOR));
    addRegion(manifest, 1, HConstants.EMPTY_START_ROW, SPLIT_KEY, false, false);
    addRegion(manifest, 2, SPLIT_KEY, HConstants.EMPTY_END_ROW, false, false);
    addRegion(manifest, 3, HConstants.EMPTY_START_ROW, SPLIT_KEY, true, false);
    addRegion(manifest, 4, SPLIT_KEY, HConstants.EMPTY_END_ROW, false, true);

    try (FSDataOutputStream out =
      fs.create(new Path(snapshotDir, SnapshotManifest.DATA_MANIFEST_NAME))) {
      manifest.build().writeTo(out);
    }
  }

  private static void addRegion(SnapshotDataManifest.Builder manifest, long regionId,
    byte[] startKey, byte[] endKey, boolean offline, boolean split) {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(regionId)
      .setStartKey(startKey).setEndKey(endKey).setOffline(offline).setSplit(split).build();
    manifest.addRegionManifests(SnapshotRegionManifest.newBuilder()
      .setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo)).build());
  }
}
