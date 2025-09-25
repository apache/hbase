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
package org.apache.hadoop.hbase.backup.util;

import static org.apache.hadoop.hbase.wal.WALEdit.METAFAMILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Unit tests for {@link BulkLoadProcessor}.
 * <p>
 * These tests validate the extraction of bulk-loaded file paths from WAL entries under different
 * scenarios, including:
 * <ul>
 * <li>Valid replicable bulk load entries</li>
 * <li>Non-replicable bulk load entries</li>
 * <li>Entries with no bulk load qualifier</li>
 * <li>Entries containing multiple column families</li>
 * </ul>
 */
@Category({ SmallTests.class })
public class TestBulkLoadProcessor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadProcessor.class);

  /**
   * Creates a WAL.Entry containing a {@link WALProtos.BulkLoadDescriptor} with the given
   * parameters.
   * @param tableName  The table name
   * @param regionName The encoded region name
   * @param replicate  Whether the bulk load is marked for replication
   * @param family     Column family name
   * @param storeFiles One or more store file names to include
   * @return A WAL.Entry representing the bulk load event
   */
  private WAL.Entry createBulkLoadWalEntry(TableName tableName, String regionName,
    boolean replicate, String family, String... storeFiles) {

    // Build StoreDescriptor
    WALProtos.StoreDescriptor.Builder storeDescBuilder =
      WALProtos.StoreDescriptor.newBuilder().setFamilyName(ByteString.copyFromUtf8(family))
        .setStoreHomeDir(family).addAllStoreFile(Arrays.asList(storeFiles));

    // Build BulkLoadDescriptor
    WALProtos.BulkLoadDescriptor.Builder bulkDescBuilder = WALProtos.BulkLoadDescriptor.newBuilder()
      .setReplicate(replicate).setEncodedRegionName(ByteString.copyFromUtf8(regionName))
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).setBulkloadSeqNum(1000) // Random
      .addStores(storeDescBuilder);

    byte[] value = bulkDescBuilder.build().toByteArray();

    // Build Cell with BULK_LOAD qualifier
    Cell cell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setType(Cell.Type.Put)
      .setRow(new byte[] { 1 }).setFamily(METAFAMILY).setQualifier(WALEdit.BULK_LOAD)
      .setValue(value).build();

    WALEdit edit = new WALEdit();
    edit.add(cell);

    WALKeyImpl key = new WALKeyImpl(Bytes.toBytes(regionName), // region
      tableName, 0L, 0L, null);

    return new WAL.Entry(key, edit);
  }

  /**
   * Verifies that a valid replicable bulk load WAL entry produces the correct number and structure
   * of file paths.
   */
  @Test
  public void testProcessBulkLoadFiles_validEntry() throws IOException {
    WAL.Entry entry = createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "region123", true,
      "cf1", "file1", "file2");

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(Collections.singletonList(entry));

    assertEquals(2, paths.size());
    assertTrue(paths.get(0).toString().contains("ns/tbl/region123/cf1/file1"));
    assertTrue(paths.get(1).toString().contains("ns/tbl/region123/cf1/file2"));
  }

  @Test
  public void testProcessBulkLoadFiles_validEntry_singleEntryApi() throws IOException {
    WAL.Entry entry = createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "region123", true,
      "cf1", "file1", "file2");

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(entry.getKey(), entry.getEdit());

    assertEquals(2, paths.size());
    assertTrue(paths.get(0).toString().contains("ns/tbl/region123/cf1/file1"));
    assertTrue(paths.get(1).toString().contains("ns/tbl/region123/cf1/file2"));
  }

  /**
   * Verifies that a non-replicable bulk load entry is ignored.
   */
  @Test
  public void testProcessBulkLoadFiles_nonReplicableSkipped() throws IOException {
    WAL.Entry entry =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "region123", false, "cf1", "file1");

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(Collections.singletonList(entry));

    assertTrue(paths.isEmpty());
  }

  @Test
  public void testProcessBulkLoadFiles_nonReplicableSkipped_singleEntryApi() throws IOException {
    WAL.Entry entry =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "region123", false, "cf1", "file1");

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(entry.getKey(), entry.getEdit());

    assertTrue(paths.isEmpty());
  }

  /**
   * Verifies that entries without the BULK_LOAD qualifier are ignored.
   */
  @Test
  public void testProcessBulkLoadFiles_noBulkLoadQualifier() throws IOException {
    WALEdit edit = new WALEdit();
    WALKeyImpl key = new WALKeyImpl(new byte[] {}, TableName.valueOf("ns", "tbl"), 0L, 0L, null);
    WAL.Entry entry = new WAL.Entry(key, edit);

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(Collections.singletonList(entry));

    assertTrue(paths.isEmpty());
  }

  @Test
  public void testProcessBulkLoadFiles_noBulkLoadQualifier_singleEntryApi() throws IOException {
    WALEdit edit = new WALEdit();
    WALKeyImpl key = new WALKeyImpl(new byte[] {}, TableName.valueOf("ns", "tbl"), 0L, 0L, null);

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(key, edit);

    assertTrue(paths.isEmpty());
  }

  /**
   * Verifies that multiple WAL entries with different column families produce the correct set of
   * file paths.
   */
  @Test
  public void testProcessBulkLoadFiles_multipleFamilies() throws IOException {
    WAL.Entry entry =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "regionXYZ", true, "cf1", "file1");
    WAL.Entry entry2 =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "regionXYZ", true, "cf2", "fileA");

    List<Path> paths = BulkLoadProcessor.processBulkLoadFiles(Arrays.asList(entry, entry2));

    assertEquals(2, paths.size());
    assertTrue(paths.stream().anyMatch(p -> p.toString().contains("cf1/file1")));
    assertTrue(paths.stream().anyMatch(p -> p.toString().contains("cf2/fileA")));
  }

  @Test
  public void testProcessBulkLoadFiles_multipleFamilies_singleEntryApi() throws IOException {
    WAL.Entry entry =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "regionXYZ", true, "cf1", "file1");
    WAL.Entry entry2 =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "regionXYZ", true, "cf2", "fileA");

    List<Path> paths1 = BulkLoadProcessor.processBulkLoadFiles(entry.getKey(), entry.getEdit());
    List<Path> paths2 = BulkLoadProcessor.processBulkLoadFiles(entry2.getKey(), entry2.getEdit());

    // combine to mimic processing multiple entries
    paths1.addAll(paths2);

    assertEquals(2, paths1.size());
    assertTrue(paths1.stream().anyMatch(p -> p.toString().contains("cf1/file1")));
    assertTrue(paths1.stream().anyMatch(p -> p.toString().contains("cf2/fileA")));
  }

  /**
   * Sanity check: list-based API should still work and return the same results as invoking the
   * single-entry API for the same entry (ensures delegation/backwards compatibility).
   */
  @Test
  public void testProcessBulkLoadFiles_listApi_delegatesToSingle() throws IOException {
    WAL.Entry entry =
      createBulkLoadWalEntry(TableName.valueOf("ns", "tbl"), "region123", true, "cf1", "file1");

    List<Path> single = BulkLoadProcessor.processBulkLoadFiles(entry.getKey(), entry.getEdit());
    List<Path> listApi = BulkLoadProcessor.processBulkLoadFiles(Collections.singletonList(entry));

    assertEquals(single.size(), listApi.size());
    assertTrue(listApi.get(0).toString().contains("ns/tbl/region123/cf1/file1"));
  }
}
