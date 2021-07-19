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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to verify that metadata is consistent before and after a snapshot attempt.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestSnapshotMetadata {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotMetadata.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotMetadata.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "TestSnapshotMetadata";

  private static final String MAX_VERSIONS_FAM_STR = "fam_max_columns";
  private static final byte[] MAX_VERSIONS_FAM = Bytes.toBytes(MAX_VERSIONS_FAM_STR);

  private static final String COMPRESSED_FAM_STR = "fam_compressed";
  private static final byte[] COMPRESSED_FAM = Bytes.toBytes(COMPRESSED_FAM_STR);

  private static final String BLOCKSIZE_FAM_STR = "fam_blocksize";
  private static final byte[] BLOCKSIZE_FAM = Bytes.toBytes(BLOCKSIZE_FAM_STR);

  private static final String BLOOMFILTER_FAM_STR = "fam_bloomfilter";
  private static final byte[] BLOOMFILTER_FAM = Bytes.toBytes(BLOOMFILTER_FAM_STR);

  private static final String TEST_CONF_CUSTOM_VALUE = "TestCustomConf";
  private static final String TEST_CUSTOM_VALUE = "TestCustomValue";

  private static final byte[][] families = {
    MAX_VERSIONS_FAM, BLOOMFILTER_FAM, COMPRESSED_FAM, BLOCKSIZE_FAM
  };

  private static final DataBlockEncoding DATA_BLOCK_ENCODING_TYPE = DataBlockEncoding.FAST_DIFF;
  private static final BloomType BLOOM_TYPE = BloomType.ROW;
  private static final int BLOCK_SIZE = 98;
  private static final int MAX_VERSIONS = 8;

  private Admin admin;
  private String originalTableDescription;
  private TableDescriptor originalTableDescriptor;
  TableName originalTableName;

  private static FileSystem fs;
  private static Path rootDir;

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);

    fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  private static void setupConf(Configuration conf) {
    // enable snapshot support
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around
    // some files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    // Avoid potentially aggressive splitting which would cause snapshot to fail
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
  }

  @Before
  public void setup() throws Exception {
    admin = UTIL.getAdmin();
    createTableWithNonDefaultProperties();
  }

  @After
  public void tearDown() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(admin);
  }

  /*
   *  Create a table that has non-default properties so we can see if they hold
   */
  private void createTableWithNonDefaultProperties() throws Exception {
    final long startTime = EnvironmentEdgeManager.currentTime();
    final String sourceTableNameAsString = STRING_TABLE_NAME + startTime;
    originalTableName = TableName.valueOf(sourceTableNameAsString);

    // enable replication on a column family
    ColumnFamilyDescriptor maxVersionsColumn = ColumnFamilyDescriptorBuilder
      .newBuilder(MAX_VERSIONS_FAM).setMaxVersions(MAX_VERSIONS).build();
    ColumnFamilyDescriptor bloomFilterColumn = ColumnFamilyDescriptorBuilder
      .newBuilder(BLOOMFILTER_FAM).setBloomFilterType(BLOOM_TYPE).build();
    ColumnFamilyDescriptor dataBlockColumn = ColumnFamilyDescriptorBuilder
      .newBuilder(COMPRESSED_FAM).setDataBlockEncoding(DATA_BLOCK_ENCODING_TYPE).build();
    ColumnFamilyDescriptor blockSizeColumn =
      ColumnFamilyDescriptorBuilder.newBuilder(BLOCKSIZE_FAM).setBlocksize(BLOCK_SIZE).build();

    TableDescriptor tableDescriptor = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(sourceTableNameAsString)).setColumnFamily(maxVersionsColumn)
      .setColumnFamily(bloomFilterColumn).setColumnFamily(dataBlockColumn)
      .setColumnFamily(blockSizeColumn).setValue(TEST_CUSTOM_VALUE, TEST_CUSTOM_VALUE)
      .setValue(TEST_CONF_CUSTOM_VALUE, TEST_CONF_CUSTOM_VALUE).build();
    assertTrue(tableDescriptor.getValues().size() > 0);

    admin.createTable(tableDescriptor);
    Table original = UTIL.getConnection().getTable(originalTableName);
    originalTableName = TableName.valueOf(sourceTableNameAsString);
    originalTableDescriptor = admin.getDescriptor(originalTableName);
    originalTableDescription = originalTableDescriptor.toStringCustomizedValues();

    original.close();
  }


  /**
   * Verify that the describe for a cloned table matches the describe from the original.
   */
  @Test
  public void testDescribeMatchesAfterClone() throws Exception {
    // Clone the original table
    final String clonedTableNameAsString = "clone" + originalTableName;
    final TableName clonedTableName = TableName.valueOf(clonedTableNameAsString);
    final String snapshotNameAsString = "snapshot" + originalTableName
        + EnvironmentEdgeManager.currentTime();
    final String snapshotName = snapshotNameAsString;

    // restore the snapshot into a cloned table and examine the output
    List<byte[]> familiesList = new ArrayList<>();
    Collections.addAll(familiesList, families);

    // Create a snapshot in which all families are empty
    SnapshotTestingUtils.createSnapshotAndValidate(admin, originalTableName, null,
      familiesList, snapshotNameAsString, rootDir, fs, /* onlineSnapshot= */ false);

    admin.cloneSnapshot(snapshotName, clonedTableName);
    Table clonedTable = UTIL.getConnection().getTable(clonedTableName);
    TableDescriptor cloneHtd = admin.getDescriptor(clonedTableName);
    assertEquals(
      originalTableDescription.replace(originalTableName.getNameAsString(),clonedTableNameAsString),
      cloneHtd.toStringCustomizedValues());

    // Verify the custom fields
    assertEquals(originalTableDescriptor.getValues().size(),
                        cloneHtd.getValues().size());
    assertEquals(TEST_CUSTOM_VALUE, cloneHtd.getValue(TEST_CUSTOM_VALUE));
    assertEquals(TEST_CONF_CUSTOM_VALUE, cloneHtd.getValue(TEST_CONF_CUSTOM_VALUE));
    assertEquals(originalTableDescriptor.getValues(), cloneHtd.getValues());

    admin.enableTable(originalTableName);
    clonedTable.close();
  }

  /**
   * Verify that the describe for a restored table matches the describe for one the original.
   */
  @Test
  public void testDescribeMatchesAfterRestore() throws Exception {
    runRestoreWithAdditionalMetadata(false);
  }

  /**
   * Verify that if metadata changed after a snapshot was taken, that the old metadata replaces the
   * new metadata during a restore
   */
  @Test
  public void testDescribeMatchesAfterMetadataChangeAndRestore() throws Exception {
    runRestoreWithAdditionalMetadata(true);
  }

  /**
   * Verify that when the table is empty, making metadata changes after the restore does not affect
   * the restored table's original metadata
   * @throws Exception
   */
  @Test
  public void testDescribeOnEmptyTableMatchesAfterMetadataChangeAndRestore() throws Exception {
    runRestoreWithAdditionalMetadata(true, false);
  }

  private void runRestoreWithAdditionalMetadata(boolean changeMetadata) throws Exception {
    runRestoreWithAdditionalMetadata(changeMetadata, true);
  }

  private void runRestoreWithAdditionalMetadata(boolean changeMetadata, boolean addData)
      throws Exception {

    if (admin.isTableDisabled(originalTableName)) {
      admin.enableTable(originalTableName);
    }

    // populate it with data
    final byte[] familyForUpdate = BLOCKSIZE_FAM;

    List<byte[]> familiesWithDataList = new ArrayList<>();
    List<byte[]> emptyFamiliesList = new ArrayList<>();
    if (addData) {
      Table original = UTIL.getConnection().getTable(originalTableName);
      UTIL.loadTable(original, familyForUpdate); // family arbitrarily chosen
      original.close();

      for (byte[] family : families) {
        if (family != familyForUpdate) {
          emptyFamiliesList.add(family);
        }
      }
      familiesWithDataList.add(familyForUpdate);
    } else {
      Collections.addAll(emptyFamiliesList, families);
    }

    // take a "disabled" snapshot
    final String snapshotNameAsString = "snapshot" + originalTableName
        + EnvironmentEdgeManager.currentTime();

    SnapshotTestingUtils.createSnapshotAndValidate(admin, originalTableName,
      familiesWithDataList, emptyFamiliesList, snapshotNameAsString, rootDir, fs,
      /* onlineSnapshot= */ false);

    admin.enableTable(originalTableName);

    if (changeMetadata) {
      final String newFamilyNameAsString = "newFamily" + EnvironmentEdgeManager.currentTime();
      final byte[] newFamilyName = Bytes.toBytes(newFamilyNameAsString);

      admin.disableTable(originalTableName);
      ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(newFamilyName);
      admin.addColumnFamily(originalTableName, familyDescriptor);
      assertTrue("New column family was not added.",
        admin.getDescriptor(originalTableName).toString().contains(newFamilyNameAsString));
    }

    // restore it
    if (!admin.isTableDisabled(originalTableName)) {
      admin.disableTable(originalTableName);
    }

    admin.restoreSnapshot(snapshotNameAsString);
    admin.enableTable(originalTableName);

    // verify that the descrption is reverted
    try (Table original = UTIL.getConnection().getTable(originalTableName)) {
      assertEquals(originalTableDescriptor, admin.getDescriptor(originalTableName));
      assertEquals(originalTableDescriptor, original.getDescriptor());
    }
  }
}
