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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSetMultimap;

@Tag(SmallTests.TAG)
public class TestMobUtils {
  public static final TableName TEST_TABLE_1 = TableName.valueOf("testTable1");
  public static final TableName TEST_TABLE_2 = TableName.valueOf("testTable2");
  public static final TableName TEST_TABLE_3 = TableName.valueOf("testTable3");

  @Test
  public void testCleanExpiredMobFilesUsesListedStatuses() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_DIR, "file:///hbase");
    conf.setInt(MobConstants.MOB_CLEANER_BATCH_SIZE_UPPER_BOUND, 2);
    ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f"))
      .setMobEnabled(true).setTimeToLive(24 * 60 * 60).build();
    TableDescriptor table =
      TableDescriptorBuilder.newBuilder(TEST_TABLE_1).setColumnFamily(family).build();
    Path familyPath = MobUtils.getMobFamilyPath(conf, TEST_TABLE_1, "f");
    FileStatus expired = mobFileStatus(familyPath, "20000101", false);
    String legacyName = expired.getPath().getName().split(MobFileName.REGION_SEP)[0];
    FileStatus legacy = new FileStatus(10, false, 1, 1024, 1, new Path(familyPath, legacyName));
    FileStatus directory = mobFileStatus(familyPath, "20000101", true);
    long now = EnvironmentEdgeManager.currentTime();
    FileStatus current = mobFileStatus(familyPath, MobUtils.formatDate(new Date(now)), false);
    // HFileLink recognizes the legacy MOB filename without the region suffix.
    String linkName = HFileLink.createHFileLinkName(TEST_TABLE_2,
      MobUtils.getMobRegionInfo(TEST_TABLE_2).getEncodedName(), legacyName);
    FileStatus link = new FileStatus(0, false, 1, 1024, 1, new Path(familyPath, linkName));
    assertTrue(HFileLink.isHFileLink(link.getPath()));
    FileSystem fs = mock(FileSystem.class);
    when(fs.listStatus(familyPath))
      .thenReturn(new FileStatus[] { expired, directory, legacy, current, link });
    when(fs.mkdirs(any())).thenReturn(true);
    when(fs.rename(any(), any())).thenReturn(true);

    MobUtils.cleanExpiredMobFiles(fs, conf, table, family, now);

    Path archiveDir = HFileArchiveUtil.getStoreArchivePath(conf,
      MobUtils.getMobRegionInfo(TEST_TABLE_1), family.getName());
    for (FileStatus status : new FileStatus[] { expired, legacy, link }) {
      // Archive the listed path, including the link itself, rather than its referenced HFile.
      verify(fs).rename(status.getPath(), new Path(archiveDir, status.getPath().getName()));
    }
    verify(fs, times(3)).rename(any(), any());
    verify(fs, never()).getFileStatus(any());
    verify(fs, never()).isFile(any());
  }

  @Test
  public void testCleanExpiredMobFilesSkipsInvalidNames() throws IOException {
    HBaseTestingUtil util = new HBaseTestingUtil();
    Configuration conf = util.getConfiguration();
    Path rootDir = util.getDataTestDir();
    conf.set(HConstants.HBASE_DIR, rootDir.toString());
    ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f"))
      .setMobEnabled(true).setTimeToLive(24 * 60 * 60).build();
    TableDescriptor table =
      TableDescriptorBuilder.newBuilder(TEST_TABLE_1).setColumnFamily(family).build();
    Path familyPath = MobUtils.getMobFamilyPath(conf, TEST_TABLE_1, "f");
    Path expired = mobFileStatus(familyPath, "20000101", false).getPath();
    // Each invalid name contains a parsable, expired date at the expected offset.
    List<Path> invalidPaths = Arrays.asList(new Path(familyPath, "z".repeat(32) + "20000101junk"),
      new Path(familyPath, "0".repeat(32) + "20000101junk"),
      new Path(familyPath, "z".repeat(32) + "20000101" + "0".repeat(32) + "_abc"));
    List<Path> paths = new ArrayList<>(invalidPaths);
    paths.add(expired);
    FileSystem fs = FileSystem.getLocal(conf);
    try {
      for (Path path : paths) {
        try (FSDataOutputStream out = fs.create(path)) {
          out.write(1);
        }
      }

      MobUtils.cleanExpiredMobFiles(fs, conf, table, family, EnvironmentEdgeManager.currentTime());

      Path archiveDir = HFileArchiveUtil.getStoreArchivePath(conf,
        MobUtils.getMobRegionInfo(TEST_TABLE_1), family.getName());
      assertTrue(fs.exists(new Path(archiveDir, expired.getName())));
      for (Path path : invalidPaths) {
        assertTrue(fs.exists(path), "Invalid name must be left in the MOB directory: " + path);
      }
      assertEquals(1, fs.listStatus(archiveDir).length);
    } finally {
      fs.delete(rootDir, true);
    }
  }

  private FileStatus mobFileStatus(Path familyPath, String date, boolean directory) {
    String name =
      MobFileName.create(Bytes.toBytes("row"), date, UUID.randomUUID().toString().replace("-", ""),
        MobUtils.getMobRegionInfo(TEST_TABLE_1).getEncodedName()).getFileName();
    return new FileStatus(10, directory, 1, 1024, 1, new Path(familyPath, name));
  }

  @Test
  public void serializeSingleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      ImmutableSetMultimap.<TableName, String> builder().putAll(TEST_TABLE_1, "file1a").build();
    byte[] result = MobUtils.serializeMobFileRefs(mobRefSet);
    assertEquals("testTable1/file1a", Bytes.toString(result));
  }

  @Test
  public void serializeMultipleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      ImmutableSetMultimap.<TableName, String> builder().putAll(TEST_TABLE_1, "file1a", "file1b")
        .putAll(TEST_TABLE_2, "file2a").putAll(TEST_TABLE_3, "file3a", "file3b").build();
    byte[] result = MobUtils.serializeMobFileRefs(mobRefSet);
    assertEquals("testTable1/file1a,file1b//testTable2/file2a//testTable3/file3a,file3b",
      Bytes.toString(result));
  }

  @Test
  public void deserializeSingleMobFileRefs() {
    ImmutableSetMultimap<TableName, String> mobRefSet =
      MobUtils.deserializeMobFileRefs(Bytes.toBytes("testTable1/file1a")).build();
    assertEquals(1, mobRefSet.size());
    ImmutableSet<String> testTable1Refs = mobRefSet.get(TEST_TABLE_1);
    assertEquals(1, testTable1Refs.size());
    assertTrue(testTable1Refs.contains("file1a"));
  }

  @Test
  public void deserializeMultipleMobFileRefs() {
    ImmutableSetMultimap<TableName,
      String> mobRefSet = MobUtils
        .deserializeMobFileRefs(
          Bytes.toBytes("testTable1/file1a,file1b//testTable2/file2a//testTable3/file3a,file3b"))
        .build();
    assertEquals(5, mobRefSet.size());
    ImmutableSet<String> testTable1Refs = mobRefSet.get(TEST_TABLE_1);
    ImmutableSet<String> testTable2Refs = mobRefSet.get(TEST_TABLE_2);
    ImmutableSet<String> testTable3Refs = mobRefSet.get(TEST_TABLE_3);
    assertEquals(2, testTable1Refs.size());
    assertEquals(1, testTable2Refs.size());
    assertEquals(2, testTable3Refs.size());
    assertTrue(testTable1Refs.contains("file1a"));
    assertTrue(testTable1Refs.contains("file1b"));
    assertTrue(testTable2Refs.contains("file2a"));
    assertTrue(testTable3Refs.contains("file3a"));
    assertTrue(testTable3Refs.contains("file3b"));
  }

  public static String getTableName(String testMethodName) {
    return testMethodName.replace("[", "-").replace("]", "");
  }
}
