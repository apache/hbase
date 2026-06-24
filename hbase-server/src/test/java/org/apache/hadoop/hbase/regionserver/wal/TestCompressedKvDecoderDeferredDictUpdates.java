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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCompressedKvDecoderDeferredDictUpdates {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompressedKvDecoderDeferredDictUpdates.class);

  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private static final Set<CompressionContext.DictionaryIndex> CELL_DICTIONARIES =
    EnumSet.of(CompressionContext.DictionaryIndex.ROW, CompressionContext.DictionaryIndex.FAMILY,
      CompressionContext.DictionaryIndex.QUALIFIER);

  private static byte[] safeGetEntry(Dictionary dict, short idx) {
    try {
      return dict.getEntry(idx);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  private KeyValue createKV(String row, String qualifier, String value, int numTags) {
    List<Tag> tags = new ArrayList<>(numTags);
    for (int i = 1; i <= numTags; i++) {
      tags.add(new ArrayBackedTag((byte) i, Bytes.toBytes("tag" + row + "-" + i)));
    }
    return new KeyValue(Bytes.toBytes(row), FAMILY, Bytes.toBytes(qualifier),
      HConstants.LATEST_TIMESTAMP, Bytes.toBytes(value), tags);
  }

  private byte[] encodeCells(List<KeyValue> cells, CompressionContext ctx, Configuration conf)
    throws IOException {
    WALCellCodec codec = new WALCellCodec(conf, ctx);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Codec.Encoder encoder = codec.getEncoder(bos);
    for (KeyValue kv : cells) {
      encoder.write(kv);
    }
    encoder.flush();
    return bos.toByteArray();
  }

  private int readUntilEof(Codec.Decoder decoder) throws IOException {
    int count = 0;
    boolean hitEof = false;
    while (!hitEof) {
      try {
        if (!decoder.advance()) {
          hitEof = true;
        } else {
          count++;
        }
      } catch (Exception e) {
        hitEof = true;
      }
    }
    return count;
  }

  private void assertDictionariesMatch(CompressionContext actual, CompressionContext expected,
    int truncLen, int successfulCells) {
    for (CompressionContext.DictionaryIndex idx : CELL_DICTIONARIES) {
      Dictionary actualDict = actual.getDictionary(idx);
      Dictionary expectedDict = expected.getDictionary(idx);
      for (short s = 0; s < Short.MAX_VALUE; s++) {
        byte[] actualEntry = safeGetEntry(actualDict, s);
        byte[] expectedEntry = safeGetEntry(expectedDict, s);
        if (actualEntry == null && expectedEntry == null) {
          break;
        }
        assertArrayEquals(
          String.format("Dictionary %s entry %d mismatch at truncLen=%d, successfulCells=%d", idx,
            s, truncLen, successfulCells),
          expectedEntry, actualEntry);
      }
    }
  }

  private void verifyDictsMatchAfterTruncation(List<KeyValue> cells, boolean hasTagCompression,
    boolean hasValueCompression, Compression.Algorithm valueAlgo) throws Exception {
    Configuration conf = new Configuration(false);
    if (hasTagCompression) {
      conf.setBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    }
    CompressionContext writeCtx =
      new CompressionContext(LRUDictionary.class, false, hasTagCompression, hasValueCompression,
        hasValueCompression ? valueAlgo : Compression.Algorithm.NONE);
    byte[] fullData = encodeCells(cells, writeCtx, conf);

    for (int truncLen = 1; truncLen < fullData.length; truncLen++) {
      byte[] truncated = Arrays.copyOf(fullData, truncLen);
      CompressionContext readCtx =
        new CompressionContext(LRUDictionary.class, false, hasTagCompression, hasValueCompression,
          hasValueCompression ? valueAlgo : Compression.Algorithm.NONE);
      Codec.Decoder decoder =
        new WALCellCodec(conf, readCtx).getDecoder(new ByteArrayInputStream(truncated));
      int successfulCells = readUntilEof(decoder);

      CompressionContext verifyCtx =
        new CompressionContext(LRUDictionary.class, false, hasTagCompression, hasValueCompression,
          hasValueCompression ? valueAlgo : Compression.Algorithm.NONE);
      Codec.Decoder verifyDecoder =
        new WALCellCodec(conf, verifyCtx).getDecoder(new ByteArrayInputStream(fullData));
      for (int i = 0; i < successfulCells; i++) {
        assertTrue("verifyDecoder.advance() should return true for cell " + i,
          verifyDecoder.advance());
      }

      assertDictionariesMatch(readCtx, verifyCtx, truncLen, successfulCells);
    }
  }

  @Test
  public void itPreservesDictionaryStateOnTruncatedStream() throws Exception {
    List<KeyValue> cells = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      cells.add(createKV("row-" + i, "qual-" + i, "value-" + i, 0));
    }
    verifyDictsMatchAfterTruncation(cells, false, false, null);
  }

  @Test
  public void itPreservesDictionaryStateWithTagCompression() throws Exception {
    List<KeyValue> cells = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      cells.add(createKV("row-" + i, "qual-" + i, "value-" + i, 2));
    }
    verifyDictsMatchAfterTruncation(cells, true, false, null);
  }

  @Test
  public void itPreservesDictionaryStateWithValueCompression() throws Exception {
    List<KeyValue> cells = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      byte[] value = new byte[64];
      Bytes.random(value);
      cells.add(createKV("row-" + i, "qual-" + i, Bytes.toString(value), 0));
    }
    verifyDictsMatchAfterTruncation(cells, false, true, Compression.Algorithm.GZ);
  }

  @Test
  public void itCanResumeAfterTruncation() throws Exception {
    List<KeyValue> cells = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      cells.add(createKV("row-" + i, "qual-" + i, "value-" + i, 0));
    }

    Configuration conf = new Configuration(false);
    CompressionContext writeCtx = new CompressionContext(LRUDictionary.class, false, false);
    byte[] fullData = encodeCells(cells, writeCtx, conf);

    int[] cellEndOffsets = new int[cells.size()];
    {
      CompressionContext scanCtx = new CompressionContext(LRUDictionary.class, false, false);
      WALCellCodec scanCodec = new WALCellCodec(conf, scanCtx);
      for (int i = 0; i < cells.size(); i++) {
        ByteArrayOutputStream cellBos = new ByteArrayOutputStream();
        Codec.Encoder cellEncoder = scanCodec.getEncoder(cellBos);
        cellEncoder.write(cells.get(i));
        cellEncoder.flush();
        cellEndOffsets[i] = (i == 0) ? cellBos.size() : cellEndOffsets[i - 1] + cellBos.size();
      }
    }

    for (int cellIdx = 0; cellIdx < cells.size() - 1; cellIdx++) {
      int truncPoint = cellEndOffsets[cellIdx] + 1;
      if (truncPoint >= fullData.length) {
        continue;
      }
      byte[] truncated = Arrays.copyOf(fullData, truncPoint);

      CompressionContext readCtx = new CompressionContext(LRUDictionary.class, false, false);
      WALCellCodec readCodec = new WALCellCodec(conf, readCtx);
      Codec.Decoder decoder = readCodec.getDecoder(new ByteArrayInputStream(truncated));
      int successfulCells = readUntilEof(decoder);
      assertEquals("successfulCells at cellIdx=" + cellIdx, cellIdx + 1, successfulCells);

      int resumeOffset = cellEndOffsets[cellIdx];
      Codec.Decoder resumeDecoder = readCodec.getDecoder(
        new ByteArrayInputStream(fullData, resumeOffset, fullData.length - resumeOffset));

      CompressionContext verifyCtx = new CompressionContext(LRUDictionary.class, false, false);
      Codec.Decoder verifyDecoder =
        new WALCellCodec(conf, verifyCtx).getDecoder(new ByteArrayInputStream(fullData));
      for (int i = 0; i < successfulCells; i++) {
        assertTrue(verifyDecoder.advance());
      }

      for (int i = successfulCells; i < cells.size(); i++) {
        assertTrue("resume should advance for cell " + i, resumeDecoder.advance());
        assertTrue("verify should advance for cell " + i, verifyDecoder.advance());
        assertArrayEquals(String.format("cell %d content mismatch at cellIdx=%d", i, cellIdx),
          ((KeyValue) verifyDecoder.current()).getBuffer(),
          ((KeyValue) resumeDecoder.current()).getBuffer());
      }
    }
  }
}
