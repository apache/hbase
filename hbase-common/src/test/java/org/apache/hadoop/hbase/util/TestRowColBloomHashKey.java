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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Category({ MiscTests.class, SmallTests.class })
public class TestRowColBloomHashKey {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowColBloomHashKey.class);

  private KeyValue kv;
  private RowColBloomHashKey hashKey;

  @BeforeEach
  public void setup() {
    byte[] row = Bytes.toBytes("row_key_test");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("1234567890");
    kv = new KeyValue(row, family, qualifier, value);
    hashKey = new RowColBloomHashKey(kv);
  }

  @Test
  public void testGet() {
    final int rowLen = kv.getRowLength();
    final int qualLen = kv.getQualifierLength();

    // Expected virtual layout:
    // [rowLen(2 bytes)][row bytes][famLen(1 byte, always 0)][qualifier bytes]
    // [timestamp(8 bytes, HConstants.LATEST_TIMESTAMP)][type(1 byte, KeyValue.Type.Maximum)]
    final int expectedLength = KeyValue.ROW_LENGTH_SIZE + rowLen + KeyValue.FAMILY_LENGTH_SIZE
      + qualLen + KeyValue.TIMESTAMP_TYPE_SIZE;
    assertEquals(expectedLength, hashKey.length());

    int offset = 0;

    // 1) Row length field: MSB then LSB
    int msb = hashKey.get(offset++) & 0xFF;
    int lsb = hashKey.get(offset++) & 0xFF;
    int decodedRowLen = (msb << 8) | lsb;
    assertEquals(rowLen, decodedRowLen);

    // 2) Row bytes
    for (int i = 0; i < rowLen; i++) {
      int expected = PrivateCellUtil.getRowByte(kv, i) & 0xFF;
      int actual = hashKey.get(offset++) & 0xFF;
      assertEquals(expected, actual, "row byte mismatch at i=" + i);
    }

    // 3) Family length byte
    assertEquals(0, hashKey.get(offset++) & 0xFF);

    // 4) Qualifier bytes
    for (int i = 0; i < qualLen; i++) {
      int expected = PrivateCellUtil.getQualifierByte(kv, i) & 0xFF;
      int actual = hashKey.get(offset++) & 0xFF;
      assertEquals(expected, actual, "qualifier byte mismatch at i=" + i);
    }

    // 5) Timestamp bytes: should match HConstants.LATEST_TIMESTAMP in big-endian
    // RowColBloomHashKey uses LATEST_TS byte[] from CellHashKey which corresponds to latest
    // timestamp.
    long ts = HConstants.LATEST_TIMESTAMP;
    for (int i = 0; i < KeyValue.TIMESTAMP_SIZE; i++) {
      // KeyValue timestamp serialization is big-endian
      int expected = (int) ((ts >>> (8 * (KeyValue.TIMESTAMP_SIZE - 1 - i))) & 0xFF);
      int actual = hashKey.get(offset++) & 0xFF;
      assertEquals(expected, actual, "timestamp byte mismatch at i=" + i);
    }

    // 6) Type byte: should be Maximum
    assertEquals(KeyValue.Type.Maximum.getCode(), hashKey.get(offset++));

    // consumed exactly all bytes
    assertEquals(hashKey.length(), offset);
  }

  @Test
  public void testGetIntLE() {
    for (int i = 0; i <= hashKey.length() - Bytes.SIZEOF_INT; i++) {
      int expected = expectedIntLEFromGet(i);
      int actual = hashKey.getIntLE(i);
      assertEquals(expected, actual, "sequential mismatch at offset=" + i);
    }
  }

  @Test
  public void testGetLongLE() {
    for (int i = 0; i <= hashKey.length() - Bytes.SIZEOF_LONG; i++) {
      long expected = expectedLongLEFromGet(i);
      long actual = hashKey.getLongLE(i);
      assertEquals(expected, actual, "sequential mismatch at offset=" + i);
    }
  }

  private int expectedIntLEFromGet(int offset) {
    int b0 = hashKey.get(offset) & 0xFF;
    int b1 = hashKey.get(offset + 1) & 0xFF;
    int b2 = hashKey.get(offset + 2) & 0xFF;
    int b3 = hashKey.get(offset + 3) & 0xFF;
    return (b0) | (b1 << 8) | (b2 << 16) | (b3 << 24);
  }

  private long expectedLongLEFromGet(int offset) {
    long result = 0L;
    for (int i = 0; i < Bytes.SIZEOF_LONG; i++) {
      long b = hashKey.get(offset + i) & 0xFFL;
      result |= b << (8 * i);
    }
    return result;
  }
}
