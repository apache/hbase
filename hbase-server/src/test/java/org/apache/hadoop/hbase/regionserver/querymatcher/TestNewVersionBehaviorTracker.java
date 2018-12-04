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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.util.TreeSet;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({ RegionServerTests.class, SmallTests.class })
public class TestNewVersionBehaviorTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNewVersionBehaviorTracker.class);

  private final byte[] col0 = Bytes.toBytes("col0");
  private final byte[] col1 = Bytes.toBytes("col1");
  private final byte[] col2 = Bytes.toBytes("col2");
  private final byte[] col3 = Bytes.toBytes("col3");
  private final byte[] col4 = Bytes.toBytes("col4");
  private final byte[] row = Bytes.toBytes("row");
  private final byte[] family = Bytes.toBytes("family");
  private final byte[] value = Bytes.toBytes("value");
  private final CellComparator comparator = CellComparatorImpl.COMPARATOR;

  @Test
  public void testColumns() throws IOException {
    TreeSet<byte[]> trackedColumns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    trackedColumns.add(col1);
    trackedColumns.add(col3);

    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(trackedColumns, comparator, 1, 3, 3, 10000);

    KeyValue keyValue = new KeyValue(row, family, col0, 20000, KeyValue.Type.Put, value);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL, tracker.checkColumn(keyValue, keyValue.getTypeByte()));

    keyValue = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE, tracker.checkColumn(keyValue, keyValue.getTypeByte()));

    keyValue = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL, tracker.checkColumn(keyValue, keyValue.getTypeByte()));

    keyValue = new KeyValue(row, family, col3, 20000, KeyValue.Type.Put, value);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE, tracker.checkColumn(keyValue, keyValue.getTypeByte()));

    keyValue = new KeyValue(row, family, col4, 20000, KeyValue.Type.Put, value);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_ROW, tracker.checkColumn(keyValue, keyValue.getTypeByte()));
  }

  @Test
  public void testMaxVersionMask() {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 10000);

    KeyValue keyValue = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    keyValue.setTimestamp(20000);
    keyValue.setSequenceId(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19998);
    keyValue.setSequenceId(997);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19997);
    keyValue.setSequenceId(996);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));

    keyValue = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    keyValue.setTimestamp(20000);
    keyValue.setSequenceId(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(1001);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19998);
    keyValue.setSequenceId(1003);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setTimestamp(19997);
    keyValue.setSequenceId(1004);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
  }

  @Test
  public void testVersionsDelete() {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 10000);
    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    KeyValue delete = new KeyValue(row, family, col1, 20000, KeyValue.Type.DeleteColumn, value);
    delete.setSequenceId(1000);
    delete.setTimestamp(20000);
    tracker.add(delete);
    put.setSequenceId(1001);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(19998);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));

    delete = new KeyValue(row, family, col2, 20000, KeyValue.Type.DeleteColumn, value);
    delete.setSequenceId(1002);
    delete.setTimestamp(20000);
    tracker.add(delete);
    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setSequenceId(1001);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(19998);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
  }

  @Test
  public void testVersionDelete() {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 10000);
    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    KeyValue delete = new KeyValue(row, family, col1, 20000, KeyValue.Type.Delete, value);
    delete.setSequenceId(1000);
    delete.setTimestamp(20000);
    tracker.add(delete);
    put.setSequenceId(1001);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));

    delete = new KeyValue(row, family, col2, 20000, KeyValue.Type.Delete, value);
    delete.setSequenceId(1002);
    delete.setTimestamp(20000);
    tracker.add(delete);
    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setSequenceId(1001);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put.setSequenceId(1002);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(998);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(put));
  }

  @Test
  public void testFamilyVersionsDelete() {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 10000);

    KeyValue delete = new KeyValue(row, family, null, 20000, KeyValue.Type.DeleteFamily, value);
    delete.setSequenceId(1000);
    delete.setTimestamp(20000);

    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    tracker.add(delete);
    put.setSequenceId(1001);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(19998);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));

    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setSequenceId(998);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(19998);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
  }

  @Test
  public void testFamilyVersionDelete() {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 10000);

    KeyValue delete = new KeyValue(row, family, null, 20000, KeyValue.Type.DeleteFamilyVersion,
        value);
    delete.setSequenceId(1000);
    delete.setTimestamp(20000);
    tracker.add(delete);

    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    put.setSequenceId(1001);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));

    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setSequenceId(1001);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(999);
    put.setTimestamp(20000);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put.setSequenceId(1002);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setSequenceId(998);
    put.setTimestamp(19999);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(put));
  }

  @Test
  public void testMinVersionsAndTTL() throws IOException {
    NewVersionBehaviorTracker tracker =
        new NewVersionBehaviorTracker(null, comparator, 1, 3, 3, 30000);

    KeyValue keyValue = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    keyValue.setTimestamp(20000);
    keyValue.setSequenceId(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(
        MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19998);
    keyValue.setSequenceId(997);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19997);
    keyValue.setSequenceId(996);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));

    keyValue = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    keyValue.setTimestamp(20000);
    keyValue.setSequenceId(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19999);
    keyValue.setSequenceId(1001);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19998);
    keyValue.setSequenceId(1003);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setTimestamp(19997);
    keyValue.setSequenceId(1004);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(keyValue, keyValue.getTimestamp(), keyValue.getTypeByte(), false));
  }
}
