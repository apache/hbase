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
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class TestPayloadCarryingRpcController {
  @Test
  public void testListOfCellScannerables() throws IOException {
    List<CellScannable> cells = new ArrayList<CellScannable>();
    final int count = 10;
    for (int i = 0; i < count; i++) {
      cells.add(createCell(i));
    }
    PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cells);
    CellScanner cellScanner = controller.cellScanner();
    int index = 0;
    for (; cellScanner.advance(); index++) {
      Cell cell = cellScanner.current();
      byte [] indexBytes = Bytes.toBytes(index);
      assertTrue("" + index, Bytes.equals(indexBytes, 0, indexBytes.length, cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    }
    assertEquals(count, index);
  }

  /**
   * @param index
   * @return A faked out 'Cell' that does nothing but return index as its value
   */
  static CellScannable createCell(final int index) {
    return new CellScannable() {
      @Override
      public CellScanner cellScanner() {
        return new CellScanner() {
          @Override
          public Cell current() {
            // Fake out a Cell.  All this Cell has is a value that is an int in size and equal
            // to the above 'index' param serialized as an int.
            return new Cell() {
              private final int i = index;

              @Override
              public byte[] getRowArray() {
                // unused
                return null;
              }

              @Override
              public int getRowOffset() {
                // unused
                return 0;
              }

              @Override
              public short getRowLength() {
                // unused
                return 0;
              }

              @Override
              public byte[] getFamilyArray() {
                // unused
                return null;
              }

              @Override
              public int getFamilyOffset() {
                // unused
                return 0;
              }

              @Override
              public byte getFamilyLength() {
                // unused
                return 0;
              }

              @Override
              public byte[] getQualifierArray() {
                // unused
                return null;
              }

              @Override
              public int getQualifierOffset() {
                // unused
                return 0;
              }

              @Override
              public int getQualifierLength() {
                // unused
                return 0;
              }

              @Override
              public long getTimestamp() {
                // unused
                return 0;
              }

              @Override
              public byte getTypeByte() {
                // unused
                return 0;
              }

              @Override
              public long getMvccVersion() {
                // unused
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
              public int getTagsOffset() {
                // unused
                return 0;
              }

              @Override
              public int getTagsLengthUnsigned() {
                // unused
                return 0;
              }

              @Override
              public short getTagsLength() {
                // unused
                return 0;
              }

              @Override
              public byte[] getTagsArray() {
                // unused
                return null;
              }

              @Override
              public byte[] getValue() {
                // unused
                return null;
              }

              @Override
              public byte[] getFamily() {
                // unused
                return null;
              }

              @Override
              public byte[] getQualifier() {
                // unused
                return null;
              }

              @Override
              public byte[] getRow() {
                // unused
                return null;
              }
            };
          }

          private boolean hasCell = true;
          @Override
          public boolean advance() {
            // We have one Cell only so return true first time then false ever after.
            if (!hasCell) return hasCell;
            hasCell = false;
            return true;
          }
        };
      }
    };
  }
}
