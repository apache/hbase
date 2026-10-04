/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestFastDiffEncodingState {

  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE_A = Bytes.toBytes("valueA");
  private static final byte[] VALUE_B = Bytes.toBytes("valueB");
  private static final byte[] CORRUPTED_VALUE = Bytes.toBytes("broken");
  private static final byte[] EMPTY_VALUE = new byte[0];

  private final Configuration configuration = HBaseConfiguration.create();

  private FastDiffDeltaEncoder encoder;
  private HFileContext fileContext;
  private HFileBlockDefaultEncodingContext context;
  private ByteArrayOutputStream encodedBlock;
  private DataOutputStream out;

  @BeforeEach
  public void setUp() throws Exception {
    initializeContext(new HFileContextBuilder().withIncludesMvcc(false).build());
  }

  @Test
  public void testUnencodedCellSize() throws Exception {
    KeyValue cell = createCell("row1", VALUE_A);

    encoder.encode(cell, context, out);

    int expectedSize = KeyValueUtil.keyLength(cell) + cell.getValueLength()
      + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
    assertEquals(expectedSize, getState().getUnencodedDataSizeWritten());
  }

  @Test
  public void testFailedCellDoesNotCommitMaterializedValueOffset() throws Exception {
    initializeContext(new HFileContextBuilder().withIncludesTags(true).build());
    KeyValue firstCell = createCell("row1", VALUE_A);
    KeyValue failedCell = new KeyValue(Bytes.toBytes("row2"), FAMILY, QUALIFIER, VALUE_B) {
      @Override
      public int getTagsLength() {
        throw new IllegalStateException("expected failure");
      }
    };
    encoder.encode(firstCell, context, out);

    try {
      encoder.encode(failedCell, context, out);
      fail("Cell encoding should fail after writing its value");
    } catch (IllegalStateException e) {
      assertEquals("expected failure", e.getMessage());
    }

    FastDiffEncodingState state = getState();
    beforeShipped(state);

    assertTrue(state.matchingPreviousValue(createCell("candidate", VALUE_A)));
  }

  private void initializeContext(HFileContext newFileContext) throws Exception {
    encoder = new FastDiffDeltaEncoder();
    fileContext = newFileContext;
    context = new HFileBlockDefaultEncodingContext(configuration, DataBlockEncoding.FAST_DIFF,
        HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
    encodedBlock = new ByteArrayOutputStream();
    encodedBlock.write(HConstants.HFILEBLOCK_DUMMY_HEADER);
    out = new DataOutputStream(encodedBlock);
    encoder.startBlockEncoding(context, out);
  }

  @Test
  public void testSameValueAfterShipped() throws Exception {
    KeyValue firstCell = createCell("row1", VALUE_A);
    KeyValue secondCell = createCell("row2", VALUE_A);
    encoder.encode(firstCell, context, out);
    encoder.encode(secondCell, context, out);

    FastDiffEncodingState state = shipAndCorrupt(secondCell);

    assertTrue(state.prevCell instanceof KeyValue.KeyOnlyKeyValue);
    assertTrue(state.matchingPreviousValue(createCell("candidate", VALUE_A)));
  }

  @Test
  public void testDifferentValueAfterShipped() throws Exception {
    KeyValue firstCell = createCell("row1", VALUE_A);
    KeyValue secondCell = createCell("row2", VALUE_B);
    encoder.encode(firstCell, context, out);
    encoder.encode(secondCell, context, out);

    FastDiffEncodingState state = shipAndCorrupt(secondCell);

    assertTrue(state.prevCell instanceof KeyValue.KeyOnlyKeyValue);
    assertTrue(state.matchingPreviousValue(createCell("candidate", VALUE_B)));
    assertFalse(state.matchingPreviousValue(createCell("candidate", VALUE_A)));
  }

  @Test
  public void testRepeatedEncode() throws Exception {
    byte[][] values = { VALUE_A, VALUE_A, VALUE_B, VALUE_B, EMPTY_VALUE, EMPTY_VALUE, VALUE_A,
      VALUE_B };
    List<KeyValue> expectedCells = new ArrayList<>();
    FastDiffEncodingState state = getState();

    for (int i = 0; i < values.length; i++) {
      KeyValue scannerCell = createCell(String.format("row%03d", i), values[i]);
      expectedCells.add(KeyValueUtil.copyToNewKeyValue(scannerCell));
      encoder.encode(scannerCell, context, out);
      beforeShipped(state);
      if ((i & 1) == 0) {
        beforeShipped(state);
      }
      Arrays.fill(scannerCell.getBuffer(), (byte) 0x5a);
    }

    ByteBuffer expected = KeyValueTestUtil.toByteBufferAndRewind(expectedCells, false);
    assertEquals(expected, finishAndDecode());
  }

  @Test
  public void testByteBufferKeyValue() throws Exception {
    KeyValue sourceCell = createCell("row1", VALUE_A);
    ByteBuffer scannerBuffer = ByteBuffer.allocateDirect(sourceCell.getLength());
    scannerBuffer.put(sourceCell.getBuffer(), sourceCell.getOffset(), sourceCell.getLength());
    scannerBuffer.rewind();
    ByteBufferKeyValue scannerCell =
        new ByteBufferKeyValue(scannerBuffer, 0, scannerBuffer.remaining());
    KeyValue expectedFirstCell = KeyValueUtil.copyToNewKeyValue(scannerCell);
    KeyValue secondCell = createCell("row2", VALUE_A);

    encoder.encode(scannerCell, context, out);
    FastDiffEncodingState state = getState();
    beforeShipped(state);
    assertTrue(state.prevCell instanceof KeyValue.KeyOnlyKeyValue);
    for (int i = 0; i < scannerBuffer.capacity(); i++) {
      scannerBuffer.put(i, (byte) 0x5a);
    }
    encoder.encode(secondCell, context, out);

    List<KeyValue> expectedCells =
        Arrays.asList(expectedFirstCell, KeyValueUtil.copyToNewKeyValue(secondCell));
    ByteBuffer expected = KeyValueTestUtil.toByteBufferAndRewind(expectedCells, false);
    assertEquals(expected, finishAndDecode());
  }

  private FastDiffEncodingState shipAndCorrupt(KeyValue cell) {
    FastDiffEncodingState state = getState();
    beforeShipped(state);
    Bytes.putBytes(cell.getValueArray(), cell.getValueOffset(), CORRUPTED_VALUE, 0,
      cell.getValueLength());
    return state;
  }

  private FastDiffEncodingState getState() {
    return (FastDiffEncodingState) context.getEncodingState();
  }

  private void beforeShipped(FastDiffEncodingState state) {
    state.beforeShipped(encodedBlock.getBuffer(), getStreamBaseOffset(), encodedBlock.size());
  }

  private int getStreamBaseOffset() {
    return encodedBlock.size() - out.size();
  }

  private ByteBuffer finishAndDecode() throws Exception {
    encoder.endBlockEncoding(context, out, encodedBlock.getBuffer());
    int encodedDataOffset = HConstants.HFILEBLOCK_HEADER_SIZE + DataBlockEncoding.ID_SIZE;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(encodedBlock.getBuffer(),
        encodedDataOffset, encodedBlock.size() - encodedDataOffset));
    ByteBuffer decoded = encoder.decodeKeyValues(in,
      encoder.newDataBlockDecodingContext(configuration, fileContext));
    decoded.rewind();
    return decoded;
  }

  private static KeyValue createCell(String row, byte[] value) {
    return new KeyValue(Bytes.toBytes(row), FAMILY, QUALIFIER, value);
  }
}
