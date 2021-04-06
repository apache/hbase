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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestHBaseRpcControllerImpl {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseRpcControllerImpl.class);

  @Test
  public void testListOfCellScannerables() throws IOException {
    final int count = 10;
    List<CellScannable> cells = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      cells.add(createCell(i));
    }
    HBaseRpcController controller = new HBaseRpcControllerImpl(null, cells);
    CellScanner cellScanner = controller.cellScanner();
    int index = 0;
    for (; cellScanner.advance(); index++) {
      Cell cell = cellScanner.current();
      byte[] indexBytes = Bytes.toBytes(index);
      assertTrue("" + index, Bytes.equals(indexBytes, 0, indexBytes.length, cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    }
    assertEquals(count, index);
  }

  /**
   * @param index the index of the cell to use as its value
   * @return A faked out 'Cell' that does nothing but return index as its value
   */
  static CellScannable createCell(final int index) {
    return new CellScannable() {
      @Override
      public CellScanner cellScanner() {
        return new CellScanner() {
          @Override
          public Cell current() {
            // Fake out a Cell. All this Cell has is a value that is an int in size and equal
            // to the above 'index' param serialized as an int.
            return new Cell() {
              @Override
              public long heapSize() {
                return 0;
              }

              private final int i = index;

              @Override
              public byte[] getRowArray() {
                return null;
              }

              @Override
              public int getRowOffset() {
                return 0;
              }

              @Override
              public short getRowLength() {
                return 0;
              }

              @Override
              public byte[] getFamilyArray() {
                return null;
              }

              @Override
              public int getFamilyOffset() {
                return 0;
              }

              @Override
              public byte getFamilyLength() {
                return 0;
              }

              @Override
              public byte[] getQualifierArray() {
                return null;
              }

              @Override
              public int getQualifierOffset() {
                return 0;
              }

              @Override
              public int getQualifierLength() {
                return 0;
              }

              @Override
              public long getTimestamp() {
                return 0;
              }

              @Override
              public byte getTypeByte() {
                return 0;
              }

              @Override
              public long getSequenceId() {
                return 0;
              }

              @Override
              public byte[] getValueArray() {
                return Bytes.toBytes(this.i);
              }

              @Override
              public int getValueOffset() {
                return 0;
              }

              @Override
              public int getValueLength() {
                return Bytes.SIZEOF_INT;
              }

              @Override
              public int getSerializedSize() {
                return 0;
              }

              @Override
              public int getTagsOffset() {
                return 0;
              }

              @Override
              public int getTagsLength() {
                return 0;
              }

              @Override
              public byte[] getTagsArray() {
                return null;
              }

              @Override
              public Type getType() {
                return null;
              }
            };
          }

          private boolean hasCell = true;

          @Override
          public boolean advance() {
            // We have one Cell only so return true first time then false ever after.
            if (!hasCell) {
              return hasCell;
            }

            hasCell = false;
            return true;
          }
        };
      }
    };
  }
}
