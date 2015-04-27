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

package org.apache.hadoop.hbase.util.vint;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * UFInt is an abbreviation for Unsigned Fixed-width Integer.
 *
 * This class converts between positive ints and 1-4 bytes that represent the int.  All input ints
 * must be positive.  Max values stored in N bytes are:
 *
 * N=1: 2^8  =&gt;           256
 * N=2: 2^16 =&gt;        65,536
 * N=3: 2^24 =&gt;    16,777,216
 * N=4: 2^31 =&gt; 2,147,483,648 (Integer.MAX_VALUE)
 *
 * This was created to get most of the memory savings of a variable length integer when encoding
 * an array of input integers, but to fix the number of bytes for each integer to the number needed
 * to store the maximum integer in the array.  This enables a binary search to be performed on the
 * array of encoded integers.
 *
 * PrefixTree nodes often store offsets into a block that can fit into 1 or 2 bytes.  Note that if
 * the maximum value of an array of numbers needs 2 bytes, then it's likely that a majority of the
 * numbers will also require 2 bytes.
 *
 * warnings:
 *  * no input validation for max performance
 *  * no negatives
 */
@InterfaceAudience.Private
public class UFIntTool {

  private static final int NUM_BITS_IN_LONG = 64;

  public static long maxValueForNumBytes(int numBytes) {
    return (1L << (numBytes * 8)) - 1;
  }

  public static int numBytes(final long value) {
    if (value == 0) {// 0 doesn't work with the formula below
      return 1;
    }
    return (NUM_BITS_IN_LONG + 7 - Long.numberOfLeadingZeros(value)) / 8;
  }

  public static byte[] getBytes(int outputWidth, final long value) {
    byte[] bytes = new byte[outputWidth];
    writeBytes(outputWidth, value, bytes, 0);
    return bytes;
  }

  public static void writeBytes(int outputWidth, final long value, byte[] bytes, int offset) {
    bytes[offset + outputWidth - 1] = (byte) value;
    for (int i = outputWidth - 2; i >= 0; --i) {
      bytes[offset + i] = (byte) (value >>> (outputWidth - i - 1) * 8);
    }
  }

  private static final long[] MASKS = new long[] {
    (long) 255,
    (long) 255 << 8,
    (long) 255 << 16,
    (long) 255 << 24,
    (long) 255 << 32,
    (long) 255 << 40,
    (long) 255 << 48,
    (long) 255 << 56
  };

  public static void writeBytes(int outputWidth, final long value, OutputStream os) throws IOException {
    for (int i = outputWidth - 1; i >= 0; --i) {
      os.write((byte) ((value & MASKS[i]) >>> (8 * i)));
    }
  }

  public static long fromBytes(final byte[] bytes) {
    long value = 0;
    value |= bytes[0] & 0xff;// these seem to do ok without casting the byte to int
    for (int i = 1; i < bytes.length; ++i) {
      value <<= 8;
      value |= bytes[i] & 0xff;
    }
    return value;
  }

  public static long fromBytes(final byte[] bytes, final int offset, final int width) {
    long value = 0;
    value |= bytes[0 + offset] & 0xff;// these seem to do ok without casting the byte to int
    for (int i = 1; i < width; ++i) {
      value <<= 8;
      value |= bytes[i + offset] & 0xff;
    }
    return value;
  }

}
