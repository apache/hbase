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
package org.apache.hadoop.hbase.mob;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestMobFileCleanupUtilWithOldStoreFile {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("f");

  @Test
  public void testCleanupWithOldStoreFile() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    Path rootDir = TEST_UTIL.getDataTestDir("old-store-file");
    FileSystem fs = rootDir.getFileSystem(conf);
    fs.delete(rootDir, true);
    CommonFSUtils.setRootDir(conf, rootDir);
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, 0);

    TableName tableName = TableName.valueOf("oldStoreFile");
    ColumnFamilyDescriptor familyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMobEnabled(true).build();
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(familyDescriptor).build();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    HRegionFileSystem regionFs =
      HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, regionInfo);

    String nonexistentRegionName = "deadbeef";
    Path mobFamilyDir =
      MobUtils.getMobFamilyPath(conf, tableName, familyDescriptor.getNameAsString());
    StoreFileWriter mobWriter = MobUtils.createWriter(conf, fs, familyDescriptor,
      MobUtils.formatDate(new Date()), mobFamilyDir, 1, Compression.Algorithm.NONE, "start",
      CacheConfig.DISABLED, Encryption.Context.NONE, false, nonexistentRegionName);
    mobWriter.append(
      new KeyValue(Bytes.toBytes("row"), FAMILY, Bytes.toBytes("q"), Bytes.toBytes("mob-value")));
    mobWriter.close();

    // Old MOB file names do not contain the region suffix used by the current cleanup logic.
    Path mobFileWithRegionSuffix = mobWriter.getPath();
    Path oldMobFile = new Path(mobFileWithRegionSuffix.getParent(),
      mobFileWithRegionSuffix.getName().split("_")[0]);
    assertTrue(fs.rename(mobFileWithRegionSuffix, oldMobFile));
    fs.setTimes(oldMobFile, 1, -1);

    // This represents a store file written before MOB_FILE_REFS metadata was added.
    StoreFileWriter oldStoreFileWriter = new StoreFileWriter.Builder(conf, CacheConfig.DISABLED, fs)
      .withOutputDir(regionFs.getStoreDir(familyDescriptor.getNameAsString()))
      .withFileContext(new HFileContextBuilder().withIncludesTags(true).build()).build();
    ExtendedCell originalCell =
      new KeyValue(Bytes.toBytes("row"), FAMILY, Bytes.toBytes("q"), Bytes.toBytes("mob-value"));
    oldStoreFileWriter
      .append(MobUtils.createMobRefCell(originalCell, Bytes.toBytes(oldMobFile.getName()),
        new ArrayBackedTag(TagType.MOB_TABLE_NAME_TAG_TYPE, tableName.getName())));
    oldStoreFileWriter.appendMetadata(1, false);
    oldStoreFileWriter.close();

    Admin admin = mock(Admin.class);
    when(admin.getDescriptor(tableName)).thenReturn(tableDescriptor);

    MobFileCleanupUtil.cleanupObsoleteMobFiles(conf, tableName, admin);

    assertTrue(fs.exists(oldMobFile), "MOB file referenced by an old store file was archived");
  }
}
