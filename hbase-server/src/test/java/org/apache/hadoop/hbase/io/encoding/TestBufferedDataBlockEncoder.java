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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.codec.Codec.Decoder;
import org.apache.hadoop.hbase.codec.Codec.Encoder;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.io.encoding.BufferedDataBlockEncoder.OnheapDecodedCell;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IOTests.class, MediumTests.class})
public class TestBufferedDataBlockEncoder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBufferedDataBlockEncoder.class);

  byte[] row1 = Bytes.toBytes("row1");
  byte[] row2 = Bytes.toBytes("row2");
  byte[] row_1_0 = Bytes.toBytes("row10");

  byte[] fam1 = Bytes.toBytes("fam1");
  byte[] fam2 = Bytes.toBytes("fam2");
  byte[] fam_1_2 = Bytes.toBytes("fam12");

  byte[] qual1 = Bytes.toBytes("qual1");
  byte[] qual2 = Bytes.toBytes("qual2");

  byte[] val = Bytes.toBytes("val");

  @Test
  public void testEnsureSpaceForKey() {
    BufferedDataBlockEncoder.SeekerState state = new BufferedDataBlockEncoder.SeekerState(
        new ObjectIntPair<>(), false);
    for (int i = 1; i <= 65536; ++i) {
      state.keyLength = i;
      state.ensureSpaceForKey();
      state.keyBuffer[state.keyLength - 1] = (byte) ((i - 1) % 0xff);
      for (int j = 0; j < i - 1; ++j) {
        // Check that earlier bytes were preserved as the buffer grew.
        assertEquals((byte) (j % 0xff), state.keyBuffer[j]);
      }
    }
  }

  @Test
  public void testCommonPrefixComparators() {
    KeyValue kv1 = new KeyValue(row1, fam1, qual1, 1L, Type.Put);
    KeyValue kv2 = new KeyValue(row1, fam_1_2, qual1, 1L, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonFamilyPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1L, Type.Put);
    kv2 = new KeyValue(row_1_0, fam_1_2, qual1, 1L, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonRowPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual2, 1L, Type.Put);
    kv2 = new KeyValue(row1, fam1, qual1, 1L, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonQualifierPrefix(kv1, kv2, 4) > 0));
  }

  @Test
  public void testKVCodecWithTagsForDecodedCellsWithNoTags() throws Exception {
    KeyValue kv1 = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("1"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("1"));
    // kv1.getKey() return a copy of the Key bytes which starts from RK_length. Means from offsets,
    // we need to reduce the KL and VL parts.
    OnheapDecodedCell c1 = new OnheapDecodedCell(kv1.getKey(), kv1.getRowLength(),
        kv1.getFamilyOffset() - KeyValue.ROW_OFFSET, kv1.getFamilyLength(),
        kv1.getQualifierOffset() - KeyValue.ROW_OFFSET, kv1.getQualifierLength(),
        kv1.getTimestamp(), kv1.getTypeByte(), kv1.getValueArray(), kv1.getValueOffset(),
        kv1.getValueLength(), kv1.getSequenceId(), kv1.getTagsArray(), kv1.getTagsOffset(),
        kv1.getTagsLength());
    KeyValue kv2 = new KeyValue(Bytes.toBytes("r2"), Bytes.toBytes("f"), Bytes.toBytes("2"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("2"));
    OnheapDecodedCell c2 = new OnheapDecodedCell(kv2.getKey(), kv2.getRowLength(),
        kv2.getFamilyOffset() - KeyValue.ROW_OFFSET, kv2.getFamilyLength(),
        kv2.getQualifierOffset() - KeyValue.ROW_OFFSET, kv2.getQualifierLength(),
        kv2.getTimestamp(), kv2.getTypeByte(), kv2.getValueArray(), kv2.getValueOffset(),
        kv2.getValueLength(), kv2.getSequenceId(), kv2.getTagsArray(), kv2.getTagsOffset(),
        kv2.getTagsLength());
    KeyValue kv3 = new KeyValue(Bytes.toBytes("r3"), Bytes.toBytes("cf"), Bytes.toBytes("qual"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes("3"));
    BufferedDataBlockEncoder.OffheapDecodedExtendedCell
        c3 = new BufferedDataBlockEncoder.OffheapDecodedExtendedCell(ByteBuffer.wrap(kv2.getKey()),
        kv2.getRowLength(), kv2.getFamilyOffset() - KeyValue.ROW_OFFSET, kv2.getFamilyLength(),
        kv2.getQualifierOffset() - KeyValue.ROW_OFFSET, kv2.getQualifierLength(),
        kv2.getTimestamp(), kv2.getTypeByte(), ByteBuffer.wrap(kv2.getValueArray()),
        kv2.getValueOffset(), kv2.getValueLength(), kv2.getSequenceId(),
        ByteBuffer.wrap(kv2.getTagsArray()), kv2.getTagsOffset(), kv2.getTagsLength());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    KeyValueCodecWithTags codec = new KeyValueCodecWithTags();
    Encoder encoder = codec.getEncoder(os);
    encoder.write(c1);
    encoder.write(c2);
    encoder.write(c3);
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    Decoder decoder = codec.getDecoder(is);
    assertTrue(decoder.advance());
    assertTrue(CellUtil.equals(c1, decoder.current()));
    assertTrue(decoder.advance());
    assertTrue(CellUtil.equals(c2, decoder.current()));
    assertTrue(decoder.advance());
    assertTrue(CellUtil.equals(c3, decoder.current()));
    assertFalse(decoder.advance());
  }
}
