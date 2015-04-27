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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Simple Variable Length Integer encoding.  Left bit of 0 means we are on the last byte.  If left
 * bit of the current byte is 1, then there is at least one more byte.
 */
@InterfaceAudience.Private
public class UVIntTool {

  public static final byte
    BYTE_7_RIGHT_BITS_SET = 127,
    BYTE_LEFT_BIT_SET = -128;

  public static final long
    INT_7_RIGHT_BITS_SET = 127,
    INT_8TH_BIT_SET = 128;

  public static final byte[]
    MAX_VALUE_BYTES = new byte[] { -1, -1, -1, -1, 7 };

  /********************* int -&gt; bytes **************************/

  public static int numBytes(int in) {
    if (in == 0) {
      // doesn't work with the formula below
      return 1;
    }
    return (38 - Integer.numberOfLeadingZeros(in)) / 7;// 38 comes from 32+(7-1)
  }

  public static byte[] getBytes(int value) {
    int numBytes = numBytes(value);
    byte[] bytes = new byte[numBytes];
    int remainder = value;
    for (int i = 0; i < numBytes - 1; ++i) {
      // set the left bit
      bytes[i] = (byte) ((remainder & INT_7_RIGHT_BITS_SET) | INT_8TH_BIT_SET);
      remainder >>= 7;
    }
    // do not set the left bit
    bytes[numBytes - 1] = (byte) (remainder & INT_7_RIGHT_BITS_SET);
    return bytes;
  }

  public static int writeBytes(int value, OutputStream os) throws IOException {
    int numBytes = numBytes(value);
    int remainder = value;
    for (int i = 0; i < numBytes - 1; ++i) {
      // set the left bit
      os.write((byte) ((remainder & INT_7_RIGHT_BITS_SET) | INT_8TH_BIT_SET));
      remainder >>= 7;
    }
    // do not set the left bit
    os.write((byte) (remainder & INT_7_RIGHT_BITS_SET));
    return numBytes;
  }

  /******************** bytes -&gt; int **************************/

  public static int getInt(byte[] bytes) {
    return getInt(bytes, 0);
  }

  public static int getInt(byte[] bytes, int offset) {
    int value = 0;
    for (int i = 0;; ++i) {
      byte b = bytes[offset + i];
      int shifted = BYTE_7_RIGHT_BITS_SET & b;// kill leftmost bit
      shifted <<= 7 * i;
      value |= shifted;
      if (b >= 0) {
        break;
      }
    }
    return value;
  }

  public static int getInt(InputStream is) throws IOException {
    int value = 0;
    int i = 0;
    int b;
    do{
      b = is.read();
      int shifted = BYTE_7_RIGHT_BITS_SET & b;// kill leftmost bit
      shifted <<= 7 * i;
      value |= shifted;
      ++i;
    }while(b > Byte.MAX_VALUE);
    return value;
  }
}
