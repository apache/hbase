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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDataManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

@Category({MasterTests.class, MediumTests.class})
public class TestSnapshotManifest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotManifest.class);

  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private static final String TABLE_NAME_STR = "testSnapshotManifest";
  private static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STR);
  private static final int TEST_NUM_REGIONS = 16000;
  private static final int TEST_NUM_REGIONFILES = 1000000;

  private static HBaseTestingUtility TEST_UTIL;
  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;
  private Path snapshotDir;
  private SnapshotDescription snapshotDesc;
  private SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();

    rootDir = TEST_UTIL.getDataTestDir(TABLE_NAME_STR);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();

    SnapshotTestingUtils.SnapshotMock snapshotMock =
      new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    builder = snapshotMock.createSnapshotV2("snapshot", TABLE_NAME_STR, 0);
    snapshotDir = builder.commit();
    snapshotDesc = builder.getSnapshotDescription();
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(rootDir,true);
  }

  @Test
  public void testReadSnapshotManifest() throws IOException {

    Path p = createDataManifest();
    try {
      SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
      fail("fail to test snapshot manifest because message size is too small.");
    } catch (CorruptedSnapshotException cse) {
      try {
        conf.setInt(SnapshotManifest.SNAPSHOT_MANIFEST_SIZE_LIMIT_CONF_KEY, 128 * 1024 * 1024);
        SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
        LOG.info("open snapshot manifest succeed.");
      } catch (CorruptedSnapshotException cse2) {
        fail("fail to take snapshot because Manifest proto-message too large.");
      }
    } finally {
      fs.delete(p, false);
    }
  }

  @Test
  public void testReadSnapshotRegionManifest() throws IOException {

    // remove datamanifest file
    fs.delete(new Path(snapshotDir, SnapshotManifest.DATA_MANIFEST_NAME), true);
    Path regionPath = createRegionManifest();

    try {
      conf.setInt(SnapshotManifest.SNAPSHOT_MANIFEST_SIZE_LIMIT_CONF_KEY, 128 * 1024 * 1024);
      SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    } catch (CorruptedSnapshotException e) {
      fail("fail to test snapshot manifest because region message size is too small.");
    } finally {
      fs.delete(regionPath, false);
    }
  }

  private Path createDataManifest() throws IOException {
    SnapshotDataManifest.Builder dataManifestBuilder =
        SnapshotDataManifest.newBuilder();
    byte[] startKey = null;
    byte[] stopKey = null;
    for (int i = 1; i <= TEST_NUM_REGIONS; i++) {
      stopKey = Bytes.toBytes(String.format("%016d", i));
      HRegionInfo regionInfo = new HRegionInfo(TABLE_NAME, startKey, stopKey, false);
      SnapshotRegionManifest.Builder dataRegionManifestBuilder =
          SnapshotRegionManifest.newBuilder();

      for (ColumnFamilyDescriptor hcd: builder.getTableDescriptor().getColumnFamilies()) {
        SnapshotRegionManifest.FamilyFiles.Builder family =
            SnapshotRegionManifest.FamilyFiles.newBuilder();
        family.setFamilyName(UnsafeByteOperations.unsafeWrap(hcd.getName()));
        for (int j = 0; j < 100; ++j) {
          SnapshotRegionManifest.StoreFile.Builder sfManifest =
              SnapshotRegionManifest.StoreFile.newBuilder();
          sfManifest.setName(String.format("%032d", i));
          sfManifest.setFileSize((1 + i) * (1 + i) * 1024);
          family.addStoreFiles(sfManifest.build());
        }
        dataRegionManifestBuilder.addFamilyFiles(family.build());
      }

      dataRegionManifestBuilder.setRegionInfo(HRegionInfo.convert(regionInfo));
      dataManifestBuilder.addRegionManifests(dataRegionManifestBuilder.build());

      startKey = stopKey;
    }

    dataManifestBuilder
        .setTableSchema(ProtobufUtil.toTableSchema(builder.getTableDescriptor()));

    SnapshotDataManifest dataManifest = dataManifestBuilder.build();
    return writeDataManifest(dataManifest);
  }

  private Path createRegionManifest() throws IOException {
    byte[] startKey = Bytes.toBytes("AAAAAA");
    byte[] stopKey = Bytes.toBytes("BBBBBB");
    HRegionInfo regionInfo = new HRegionInfo(TABLE_NAME, startKey, stopKey, false);
    SnapshotRegionManifest.Builder dataRegionManifestBuilder = SnapshotRegionManifest.newBuilder();
    dataRegionManifestBuilder.setRegionInfo(HRegionInfo.convert(regionInfo));

    for (ColumnFamilyDescriptor hcd: builder.getTableDescriptor().getColumnFamilies()) {
      SnapshotRegionManifest.FamilyFiles.Builder family =
          SnapshotRegionManifest.FamilyFiles.newBuilder();
      family.setFamilyName(UnsafeByteOperations.unsafeWrap(hcd.getName()));
      for (int j = 0; j < TEST_NUM_REGIONFILES; ++j) {
        SnapshotRegionManifest.StoreFile.Builder sfManifest =
              SnapshotRegionManifest.StoreFile.newBuilder();
        sfManifest.setName(String.format("%064d", j));
        sfManifest.setFileSize(j * 1024);
        family.addStoreFiles(sfManifest.build());
      }
      dataRegionManifestBuilder.addFamilyFiles(family.build());
    }

    SnapshotRegionManifest manifest = dataRegionManifestBuilder.build();
    Path regionPath = new Path(snapshotDir,
        SnapshotManifestV2.SNAPSHOT_MANIFEST_PREFIX + regionInfo.getEncodedName());

    FSDataOutputStream stream = fs.create(regionPath);
    try {
      manifest.writeTo(stream);
    } finally {
      stream.close();
    }

    return regionPath;
  }

  private Path writeDataManifest(final SnapshotDataManifest manifest)
      throws IOException {
    Path dataRegionPath = new Path(snapshotDir, SnapshotManifest.DATA_MANIFEST_NAME);
    FSDataOutputStream stream = fs.create(dataRegionPath);
    try {
      manifest.writeTo(stream);
    } finally {
      stream.close();
    }

    return dataRegionPath;
  }
}
