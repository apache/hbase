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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter.KeyOnlyByteBufferExtendedCell;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter.KeyOnlyCell;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({ MiscTests.class, SmallTests.class })
@RunWith(Parameterized.class)
public class TestKeyOnlyFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyOnlyFilter.class);

  @Parameterized.Parameter
  public boolean lenAsVal;

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseCommonTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  @Test
  public void testKeyOnly() throws Exception {
    byte[] r = Bytes.toBytes("row1");
    byte[] f = Bytes.toBytes("cf1");
    byte[] q = Bytes.toBytes("qual1");
    byte[] v = Bytes.toBytes("val1");
    byte[] tags = Bytes.toBytes("tag1");
    KeyValue kv = new KeyValue(r, f, q, 0, q.length, 1234L, Type.Put, v, 0,
        v.length, tags);

    ByteBuffer buffer = ByteBuffer.wrap(kv.getBuffer());
    ByteBufferKeyValue bbCell = new ByteBufferKeyValue(buffer, 0,
        buffer.remaining());

    // KV format: <keylen:4><valuelen:4><key:keylen><value:valuelen>
    // Rebuild as: <keylen:4><0:4><key:keylen>
    int dataLen = lenAsVal ? Bytes.SIZEOF_INT : 0;
    int keyOffset = (2 * Bytes.SIZEOF_INT);
    int keyLen = KeyValueUtil.keyLength(kv);
    byte[] newBuffer = new byte[keyLen + keyOffset + dataLen];
    Bytes.putInt(newBuffer, 0, keyLen);
    Bytes.putInt(newBuffer, Bytes.SIZEOF_INT, dataLen);
    KeyValueUtil.appendKeyTo(kv, newBuffer, keyOffset);
    if (lenAsVal) {
      Bytes.putInt(newBuffer, newBuffer.length - dataLen, kv.getValueLength());
    }
    KeyValue KeyOnlyKeyValue = new KeyValue(newBuffer);

    KeyOnlyCell keyOnlyCell = new KeyOnlyCell(kv, lenAsVal);
    KeyOnlyByteBufferExtendedCell keyOnlyByteBufferedCell = new KeyOnlyByteBufferExtendedCell(
        bbCell, lenAsVal);

    assertTrue(CellUtil.matchingRows(KeyOnlyKeyValue, keyOnlyCell));
    assertTrue(CellUtil.matchingRows(KeyOnlyKeyValue, keyOnlyByteBufferedCell));

    assertTrue(CellUtil.matchingFamily(KeyOnlyKeyValue, keyOnlyCell));
    assertTrue(CellUtil
        .matchingFamily(KeyOnlyKeyValue, keyOnlyByteBufferedCell));

    assertTrue(CellUtil.matchingQualifier(KeyOnlyKeyValue, keyOnlyCell));
    assertTrue(CellUtil.matchingQualifier(KeyOnlyKeyValue,
        keyOnlyByteBufferedCell));

    assertTrue(CellUtil.matchingValue(KeyOnlyKeyValue, keyOnlyCell));
    assertTrue(KeyOnlyKeyValue.getValueLength() == keyOnlyByteBufferedCell
        .getValueLength());
    assertEquals(8 + keyLen + (lenAsVal ? 4 : 0), KeyOnlyKeyValue.getSerializedSize());
    assertEquals(8 + keyLen + (lenAsVal ? 4 : 0), keyOnlyCell.getSerializedSize());
    if (keyOnlyByteBufferedCell.getValueLength() > 0) {
      assertTrue(CellUtil.matchingValue(KeyOnlyKeyValue,
          keyOnlyByteBufferedCell));
    }

    assertTrue(KeyOnlyKeyValue.getTimestamp() == keyOnlyCell.getTimestamp());
    assertTrue(KeyOnlyKeyValue.getTimestamp() == keyOnlyByteBufferedCell
        .getTimestamp());

    assertTrue(KeyOnlyKeyValue.getTypeByte() == keyOnlyCell.getTypeByte());
    assertTrue(KeyOnlyKeyValue.getTypeByte() == keyOnlyByteBufferedCell
        .getTypeByte());

    assertTrue(KeyOnlyKeyValue.getTagsLength() == keyOnlyCell.getTagsLength());
    assertTrue(KeyOnlyKeyValue.getTagsLength() == keyOnlyByteBufferedCell
        .getTagsLength());
  }

}
