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
package org.apache.hadoop.hbase.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.CellCodec;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.codec.MessageCodec;
import org.apache.hadoop.hbase.io.CellOutputStream;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Do basic codec performance eval.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CodecPerformance {
  /** @deprecated LOG variable would be made private. since 1.2, remove in 3.0 */
  @Deprecated
  public static final Log LOG = LogFactory.getLog(CodecPerformance.class);

  static Cell [] getCells(final int howMany) {
    Cell [] cells = new Cell[howMany];
    for (int i = 0; i < howMany; i++) {
      byte [] index = Bytes.toBytes(i);
      KeyValue kv = new KeyValue(index, Bytes.toBytes("f"), index, index);
      cells[i] = kv;
    }
    return cells;
  }

  static int getRoughSize(final Cell [] cells) {
    int size = 0;
    for (Cell c: cells) {
      size += c.getRowLength() + c.getFamilyLength() + c.getQualifierLength() + c.getValueLength();
      size += Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE;
    }
    return size;
  }

  static byte [] runEncoderTest(final int index, final int initialBufferSize,
      final ByteArrayOutputStream baos, final CellOutputStream encoder, final Cell [] cells)
  throws IOException {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < cells.length; i++) {
      encoder.write(cells[i]);
    }
    encoder.flush();
    LOG.info("" + index + " encoded count=" + cells.length + " in " +
      (System.currentTimeMillis() - startTime) + "ms for encoder " + encoder);
    // Ensure we did not have to grow the backing buffer.
    assertTrue(baos.size() < initialBufferSize);
    return baos.toByteArray();
  }

  static Cell [] runDecoderTest(final int index, final int count, final CellScanner decoder)
  throws IOException {
    Cell [] cells = new Cell[count];
    long startTime = System.currentTimeMillis();
    for (int i = 0; decoder.advance(); i++) {
      cells[i] = decoder.current();
    }
    LOG.info("" + index + " decoded count=" + cells.length + " in " +
      (System.currentTimeMillis() - startTime) + "ms for decoder " + decoder);
    // Ensure we did not have to grow the backing buffer.
    assertTrue(cells.length == count);
    return cells;
  }

  static void verifyCells(final Cell [] input, final Cell [] output) {
    assertEquals(input.length, output.length);
    for (int i = 0; i < input.length; i ++) {
      input[i].equals(output[i]);
    }
  }

  static void doCodec(final Codec codec, final Cell [] cells, final int cycles, final int count,
      final int initialBufferSize)
  throws IOException {
    byte [] bytes = null;
    Cell [] cellsDecoded = null;
    for (int i = 0; i < cycles; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(initialBufferSize);
      Codec.Encoder encoder = codec.getEncoder(baos);
      bytes = runEncoderTest(i, initialBufferSize, baos, encoder, cells);
    }
    for (int i = 0; i < cycles; i++) {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      Codec.Decoder decoder = codec.getDecoder(bais);
      cellsDecoded = CodecPerformance.runDecoderTest(i, count, decoder);
    }
    verifyCells(cells, cellsDecoded);
  }

  public static void main(String[] args) throws IOException {
    // How many Cells to encode/decode on each cycle.
    final int count = 100000;
    // How many times to do an operation; repeat gives hotspot chance to warm up.
    final int cycles = 30;

    Cell [] cells = getCells(count);
    int size = getRoughSize(cells);
    int initialBufferSize = 2 * size; // Multiply by 2 to ensure we don't have to grow buffer

    // Test KeyValue codec.
    doCodec(new KeyValueCodec(), cells, cycles, count, initialBufferSize);
    doCodec(new CellCodec(), cells, cycles, count, initialBufferSize);
    doCodec(new MessageCodec(), cells, cycles, count, initialBufferSize);
  }
}
