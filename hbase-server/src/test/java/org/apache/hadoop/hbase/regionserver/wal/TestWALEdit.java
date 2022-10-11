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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.wal.WALEdit.METAFAMILY;
import static org.apache.hadoop.hbase.wal.WALEdit.REPLICATION_MARKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestWALEdit {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALEdit.class);

  private static final String RS_NAME = "test-region-server-name";

  /**
   * Tests that
   * {@link org.apache.hadoop.hbase.wal.WALEdit#createReplicationMarkerEdit(byte[], long)} method is
   * creating WALEdit with correct family and qualifier.
   */
  @Test
  public void testCreateReplicationMarkerEdit() {
    long timestamp = EnvironmentEdgeManager.currentTime();

    byte[] rowkey = ReplicationMarkerChore.getRowKey(RS_NAME, timestamp);
    WALEdit edit = WALEdit.createReplicationMarkerEdit(rowkey, timestamp);
    assertEquals(1, edit.getCells().size());
    Cell cell = edit.getCells().get(0);
    assertTrue(CellUtil.matchingFamily(cell, METAFAMILY));
    assertTrue(CellUtil.matchingQualifier(cell, REPLICATION_MARKER));
    assertTrue(WALEdit.isReplicationMarkerEdit(edit));
  }
}
