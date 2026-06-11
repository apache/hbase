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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for reading and writing little-endian integers and longs from byte[] and
 * ByteBuffer. Used by hashing components to perform fast, low-level LE conversions with optional
 * Unsafe acceleration.
 */
@InterfaceAudience.Private
public final class LittleEndianBytes {
  final static boolean UNSAFE_UNALIGNED = HBasePlatformDependent.unaligned();

  static abstract class Converter {
    abstract int toInt(byte[] bytes, int offset);

    abstract int toInt(ByteBuffer buffer, int offset);

    abstract int putInt(byte[] bytes, int offset, int val);

    abstract long toLong(byte[] bytes, int offset);

    abstract long toLong(ByteBuffer buffer, int offset);

    abstract int putLong(byte[] bytes, int offset, long val);
  }

  static class ConverterHolder {
    static final String UNSAFE_CONVERTER_NAME =
      ConverterHolder.class.getName() + "$UnsafeConverter";
    static final Converter BEST_CONVERTER = getBestConverter();

    static Converter getBestConverter() {
      try {
        Class<? extends Converter> theClass =
          Class.forName(UNSAFE_CONVERTER_NAME).asSubclass(Converter.class);
        return theClass.getConstructor().newInstance();
      } catch (Throwable t) {
        return PureJavaConverter.INSTANCE;
      }
    }

    static final class PureJavaConverter extends Converter {
      static final PureJavaConverter INSTANCE = new PureJavaConverter();

      private PureJavaConverter() {
      }

      @Override
      int toInt(byte[] bytes, int offset) {
        int n = 0;
        for (int i = offset + 3; i >= offset; i--) {
          n <<= 8;
          n ^= (bytes[i] & 0xFF);
        }
        return n;
      }

      @Override
      int toInt(ByteBuffer buffer, int offset) {
        return Integer.reverseBytes(buffer.getInt(offset));
      }

      @Override
      int putInt(byte[] bytes, int offset, int val) {
        for (int i = offset; i < offset + 3; i++) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }
        bytes[offset + 3] = (byte) val;
        return offset + Bytes.SIZEOF_INT;
      }

      @Override
      long toLong(byte[] bytes, int offset) {
        long l = 0;
        for (int i = offset + 7; i >= offset; i--) {
          l <<= 8;
          l ^= (bytes[i] & 0xFFL);
        }
        return l;
      }

      @Override
      long toLong(ByteBuffer buffer, int offset) {
        return Long.reverseBytes(buffer.getLong(offset));
      }

      @Override
      int putLong(byte[] bytes, int offset, long val) {
        for (int i = offset; i < offset + 7; i++) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }
        bytes[offset + 7] = (byte) val;
        return offset + Bytes.SIZEOF_LONG;
      }
    }

    static final class UnsafeConverter extends Converter {
      static final UnsafeConverter INSTANCE = new UnsafeConverter();

      public UnsafeConverter() {
      }

      static {
        if (!UNSAFE_UNALIGNED) {
          throw new Error();
        }
      }

      @Override
      int toInt(byte[] bytes, int offset) {
        return UnsafeAccess.toIntLE(bytes, offset);
      }

      @Override
      int toInt(ByteBuffer buffer, int offset) {
        return UnsafeAccess.toIntLE(buffer, offset);
      }

      @Override
      int putInt(byte[] bytes, int offset, int val) {
        return UnsafeAccess.putIntLE(bytes, offset, val);
      }

      @Override
      long toLong(byte[] bytes, int offset) {
        return UnsafeAccess.toLongLE(bytes, offset);
      }

      @Override
      long toLong(ByteBuffer buffer, int offset) {
        return UnsafeAccess.toLongLE(buffer, offset);
      }

      @Override
      int putLong(byte[] bytes, int offset, long val) {
        return UnsafeAccess.putLongLE(bytes, offset, val);
      }
    }
  }

  /*
   * Writes an int in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static void putInt(byte[] bytes, int offset, int val) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_INT;
    ConverterHolder.BEST_CONVERTER.putInt(bytes, offset, val);
  }

  /*
   * Reads an int in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static int toInt(byte[] bytes, int offset) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_INT;
    return ConverterHolder.BEST_CONVERTER.toInt(bytes, offset);
  }

  /*
   * Reads an int in little-endian order from ByteBuffer. Caller must ensure bounds; no checks are
   * performed.
   */
  public static int toInt(ByteBuffer buffer, int offset) {
    assert offset >= 0 && buffer.capacity() - offset >= Bytes.SIZEOF_INT;
    return ConverterHolder.BEST_CONVERTER.toInt(buffer, offset);
  }

  /*
   * Writes a long in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static void putLong(byte[] bytes, int offset, long val) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_LONG;
    ConverterHolder.BEST_CONVERTER.putLong(bytes, offset, val);
  }

  /*
   * Reads a long in little-endian order. Caller must ensure bounds; no checks are performed.
   */
  public static long toLong(byte[] bytes, int offset) {
    assert offset >= 0 && bytes.length - offset >= Bytes.SIZEOF_LONG;
    return ConverterHolder.BEST_CONVERTER.toLong(bytes, offset);
  }

  /*
   * Reads a long in little-endian order from ByteBuffer. Caller must ensure bounds; no checks are
   * performed.
   */
  public static long toLong(ByteBuffer buffer, int offset) {
    assert offset >= 0 && buffer.capacity() - offset >= Bytes.SIZEOF_LONG;
    return ConverterHolder.BEST_CONVERTER.toLong(buffer, offset);
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
   * Reads a long in little-endian order from the row portion of the Cell, at the given offset.
   */
  public static long getRowAsLong(Cell cell, int offset) {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) cell;
      return toLong(bbCell.getRowByteBuffer(), bbCell.getRowPosition() + offset);
    }
    return toLong(cell.getRowArray(), cell.getRowOffset() + offset);
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

  /*
   * Reads a long in little-endian order from the qualifier portion of the Cell, at the given
   * offset.
   */
  public static long getQualifierAsLong(Cell cell, int offset) {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) cell;
      return toLong(bbCell.getQualifierByteBuffer(), bbCell.getQualifierPosition() + offset);
    }
    return toLong(cell.getQualifierArray(), cell.getQualifierOffset() + offset);
  }

  private LittleEndianBytes() {
  }
}
