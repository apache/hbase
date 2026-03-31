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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestFSWALEntry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFSWALEntry.class);

  @Test
  public void testCollectFamilies() {
    byte[] family0 = Bytes.toBytes("family0");
    byte[] family1 = Bytes.toBytes("family1");
    byte[] family2 = Bytes.toBytes("family2");

    List<ExtendedCell> cells = new ArrayList<>();
    assertEquals(0, FSWALEntry.collectFamilies(cells).size());

    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(family0)
      .setFamily(family0).setQualifier(family0).setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode()).setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    assertEquals(1, FSWALEntry.collectFamilies(cells).size());

    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(family1)
      .setFamily(family1).setQualifier(family1).setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode()).setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    assertEquals(2, FSWALEntry.collectFamilies(cells).size());

    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(family0)
      .setFamily(family0).setQualifier(family0).setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode()).setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(family1)
      .setFamily(family1).setQualifier(family1).setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode()).setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    assertEquals(2, FSWALEntry.collectFamilies(cells).size());

    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(family2)
      .setFamily(family2).setQualifier(family2).setTimestamp(HConstants.LATEST_TIMESTAMP)
      .setType(KeyValue.Type.Maximum.getCode()).setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    assertEquals(3, FSWALEntry.collectFamilies(cells).size());

    cells.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(WALEdit.METAFAMILY).setFamily(WALEdit.METAFAMILY).setQualifier(WALEdit.METAFAMILY)
      .setTimestamp(HConstants.LATEST_TIMESTAMP).setType(KeyValue.Type.Maximum.getCode())
      .setValue(HConstants.EMPTY_BYTE_ARRAY).build());
    assertEquals(3, FSWALEntry.collectFamilies(cells).size());
  }
}
