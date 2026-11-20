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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for reading and writing little-endian integers from byte[] and ByteBuffer. Used
 * by hashing components to perform fast, low-level LE conversions with optional Unsafe
 * acceleration.
 */
@InterfaceAudience.Private
public final class LittleEndianBytes {
  private static final VarHandle BYTE_BUFFER_INT_VIEW_LITTLE_ENDIAN_VAR_HANDLE =
    MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle BYTE_ARRAY_INT_VIEW_LITTLE_ENDIAN_VAR_HANDLE =
    MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  static final class Converter {
    static final Converter INSTANCE = new Converter();

    public Converter() {
    }

    int toInt(byte[] bytes, int offset) {
      return (int) BYTE_ARRAY_INT_VIEW_LITTLE_ENDIAN_VAR_HANDLE.get(bytes, offset);
    }

    int toInt(ByteBuffer buffer, int offset) {
      return (int) BYTE_BUFFER_INT_VIEW_LITTLE_ENDIAN_VAR_HANDLE.get(buffer, offset);
    }

    void putInt(byte[] bytes, int offset, int val) {
      BYTE_ARRAY_INT_VIEW_LITTLE_ENDIAN_VAR_HANDLE.set(bytes, offset, val);
    }
  }

  /*
   * Writes an int in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static void putInt(byte[] bytes, int offset, int val) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_INT;
    Converter.INSTANCE.putInt(bytes, offset, val);
  }

  /*
   * Reads an int in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static int toInt(byte[] bytes, int offset) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_INT;
    return Converter.INSTANCE.toInt(bytes, offset);
  }

  /*
   * Reads an int in little-endian order from ByteBuffer. Caller must ensure bounds; no checks are
   * performed.
   */
  public static int toInt(ByteBuffer buffer, int offset) {
    assert offset >= 0 && buffer.capacity() - offset >= Bytes.SIZEOF_INT;
    return Converter.INSTANCE.toInt(buffer, offset);
  }

  /*
   * Reads an int in little-endian order from the row portion of the Cell, at the given offset.
   */
  public static int getRowAsInt(Cell cell, int offset) {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) cell;
      return toInt(bbCell.getRowByteBuffer(), bbCell.getRowPosition() + offset);
    }
    return toInt(cell.getRowArray(), cell.getRowOffset() + offset);
  }

  /*
   * Reads an int in little-endian order from the qualifier portion of the Cell, at the given
   * offset.
   */
  public static int getQualifierAsInt(Cell cell, int offset) {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) cell;
      return toInt(bbCell.getQualifierByteBuffer(), bbCell.getQualifierPosition() + offset);
    }
    return toInt(cell.getQualifierArray(), cell.getQualifierOffset() + offset);
  }

  private LittleEndianBytes() {
  }
}
