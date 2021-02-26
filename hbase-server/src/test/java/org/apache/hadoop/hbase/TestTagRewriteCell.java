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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTagRewriteCell {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTagRewriteCell.class);

  @Test
  public void testHeapSize() {
    Cell originalCell = CellUtil.createCell(Bytes.toBytes("row"), Bytes.toBytes("value"));
    final int fakeTagArrayLength = 10;
    Cell trCell = PrivateCellUtil.createCell(originalCell, new byte[fakeTagArrayLength]);

    // Get the heapSize before the internal tags array in trCell are nuked
    long trCellHeapSize = ((HeapSize)trCell).heapSize();

    // Make another TagRewriteCell with the original TagRewriteCell
    // This happens on systems with more than one RegionObserver/Coproc loaded (such as
    // VisibilityController and AccessController)
    Cell trCell2 = PrivateCellUtil.createCell(trCell, new byte[fakeTagArrayLength]);

    assertTrue("TagRewriteCell containing a TagRewriteCell's heapsize should be " +
            "larger than a single TagRewriteCell's heapsize",
        trCellHeapSize < ((HeapSize)trCell2).heapSize());
    assertTrue("TagRewriteCell should have had nulled out tags array",
        ((HeapSize)trCell).heapSize() < trCellHeapSize);
  }
}
