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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.jupiter.api.Test;

public abstract class TestLittleEndianBytesBase {

  @Test
  public void testToInt() {
    byte[] b = generateByteArray(32);

    for (int i = 0; i <= b.length - Integer.BYTES; i++) {
      long expected = readIntLE(b, i);
      assertEquals(expected, LittleEndianBytes.toInt(b, i));
    }
  }

  @Test
  public void testByteBufferToInt() {
    byte[] b = generateByteArray(32);
    ByteBuffer buf = ByteBuffer.wrap(b);

    for (int i = 0; i <= b.length - Integer.BYTES; i++) {
      long expected = readIntLE(b, i);
      assertEquals(expected, LittleEndianBytes.toInt(buf, i));
    }
  }

  @Test
  public void testPutInt() {
    byte[] b = new byte[16];

    int offset = 5;
    int value = 0x12345678;
    LittleEndianBytes.putInt(b, offset, value);
    int expected = readIntLE(b, offset);
    assertEquals(value, expected);

    offset += Integer.BYTES;
    value = 0x9ABCDEF0;
    LittleEndianBytes.putInt(b, offset, value);
    expected = readIntLE(b, offset);
    assertEquals(value, expected);
  }

  @Test
  public void testGetLongLE() {
    byte[] b = generateByteArray(40);

    for (int i = 0; i <= b.length - Long.BYTES; i++) {
      long expected = readLongLE(b, i);
      assertEquals(expected, LittleEndianBytes.toLong(b, i));
    }
  }

  @Test
  public void testByteBufferGetLongLE() {
    byte[] b = generateByteArray(40);
    ByteBuffer buf = ByteBuffer.wrap(b);

    for (int i = 0; i <= b.length - Long.BYTES; i++) {
      long expected = readLongLE(b, i);
      assertEquals(expected, LittleEndianBytes.toLong(buf, i));
    }
  }

  @Test
  public void testPutLong() {
    byte[] b = new byte[24];

    int offset = 4;
    long value = 0x0123456789ABCDEFL;
    LittleEndianBytes.putLong(b, offset, value);
    long expected = readLongLE(b, offset);
    assertEquals(value, expected);

    offset += Long.BYTES;
    value = 0xFEDCBA9876543210L;
    LittleEndianBytes.putLong(b, offset, value);
    expected = readLongLE(b, offset);
    assertEquals(value, expected);
  }

  @Test
  public void testGetRowAsIntFromByteBufferExtendedCell() {
    Cell bbCell = createByteBufferExtendedCell();
    byte[] row = bbCell.getRowArray();

    for (int i = bbCell.getRowOffset(); i <= bbCell.getRowLength() - Integer.BYTES; i++) {
      int expected = readIntLE(row, i);
      assertEquals(expected, LittleEndianBytes.getRowAsInt(bbCell, i));
    }
  }

  @Test
  public void testGetRowAsIntFromCell() {
    KeyValue cell = createCell();
    byte[] row = cell.getRowArray();

    for (int i = cell.getRowOffset(); i <= cell.getRowLength() - Integer.BYTES; i++) {
      int expected = readIntLE(row, cell.getRowOffset() + i);
      assertEquals(expected, LittleEndianBytes.getRowAsInt(cell, i));
    }
  }

  @Test
  public void testGetRowAsLongFromByteBufferExtendedCell() {
    Cell bbCell = createByteBufferExtendedCell();
    byte[] row = bbCell.getRowArray();

    for (int i = bbCell.getRowOffset(); i <= bbCell.getRowLength() - Long.BYTES; i++) {
      long expected = readLongLE(row, i);
      assertEquals(expected, LittleEndianBytes.getRowAsLong(bbCell, i));
    }
  }

  @Test
  public void testGetRowAsLongFromCell() {
    KeyValue cell = createCell();
    byte[] row = cell.getRowArray();

    for (int i = cell.getRowOffset(); i <= cell.getRowLength() - Long.BYTES; i++) {
      long expected = readLongLE(row, cell.getRowOffset() + i);
      assertEquals(expected, LittleEndianBytes.getRowAsLong(cell, i));
    }
  }

  @Test
  public void testGetQualifierAsIntFromByteBufferExtendedCell() {
    Cell bbCell = createByteBufferExtendedCell();
    byte[] qual = bbCell.getQualifierArray();

    for (int i = bbCell.getQualifierOffset(); i
        <= bbCell.getQualifierLength() - Integer.BYTES; i++) {
      int expected = readIntLE(qual, i);
      assertEquals(expected, LittleEndianBytes.getQualifierAsInt(bbCell, i));
    }
  }

  @Test
  public void testGetQualifierAsIntFromCell() {
    KeyValue cell = createCell();
    byte[] qual = cell.getQualifierArray();

    for (int i = cell.getQualifierOffset(); i <= cell.getQualifierLength() - Integer.BYTES; i++) {
      int expected = readIntLE(qual, cell.getQualifierOffset() + i);
      assertEquals(expected, LittleEndianBytes.getQualifierAsInt(cell, i));
    }
  }

  private static KeyValue createCell() {
    byte[] row = Bytes.toBytes("row_key_for_test_12345");
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("qualifier_12345");
    byte[] value = Bytes.toBytes(123456789);
    return new KeyValue(row, family, qualifier, value);
  }

  private static ByteBufferExtendedCell createByteBufferExtendedCell() {
    KeyValue kv = createCell();
    ByteBuffer buffer = ByteBuffer.wrap(kv.getBuffer());
    return new ByteBufferKeyValue(buffer, 0, buffer.remaining());
  }

  private static byte[] generateByteArray(int size) {
    byte[] b = new byte[size];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 3 + 7);
    }
    return b;
  }

  private static int readIntLE(byte[] b, int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16)
      | ((b[off + 3] & 0xFF) << 24);
  }

  private static long readLongLE(byte[] b, int off) {
    return ((long) (b[off] & 0xFF)) | ((long) (b[off + 1] & 0xFF) << 8)
      | ((long) (b[off + 2] & 0xFF) << 16) | ((long) (b[off + 3] & 0xFF) << 24)
      | ((long) (b[off + 4] & 0xFF) << 32) | ((long) (b[off + 5] & 0xFF) << 40)
      | ((long) (b[off + 6] & 0xFF) << 48) | ((long) (b[off + 7] & 0xFF) << 56);
  }
}
