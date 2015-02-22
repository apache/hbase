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

package org.apache.hadoop.hbase.codec.prefixtree.row;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.decode.PrefixTreeArraySearcher;
import org.apache.hadoop.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@Category({MiscTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestRowEncoder {

  protected static int BLOCK_START = 7;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (TestRowData testRows : TestRowData.InMemory.getAll()) {
      parameters.add(new Object[] { testRows });
    }
    return parameters;
  }

  protected TestRowData rows;
  protected List<KeyValue> inputKvs;
  protected boolean includeMemstoreTS = true;
  protected ByteArrayOutputStream os;
  protected PrefixTreeEncoder encoder;
  protected int totalBytes;
  protected PrefixTreeBlockMeta blockMetaWriter;
  protected byte[] outputBytes;
  protected ByteBuffer buffer;
  protected ByteArrayInputStream is;
  protected PrefixTreeBlockMeta blockMetaReader;
  protected byte[] inputBytes;
  protected PrefixTreeArraySearcher searcher;

  public TestRowEncoder(TestRowData testRows) {
    this.rows = testRows;
  }

  @Before
  public void compile() throws IOException {
    // Always run with tags. But should also ensure that KVs without tags work fine
    os = new ByteArrayOutputStream(1 << 20);
    encoder = new PrefixTreeEncoder(os, includeMemstoreTS);

    inputKvs = rows.getInputs();
    for (KeyValue kv : inputKvs) {
      encoder.write(kv);
    }
    encoder.flush();
    totalBytes = encoder.getTotalBytes();
    blockMetaWriter = encoder.getBlockMeta();
    outputBytes = os.toByteArray();

    // start reading, but save the assertions for @Test methods
    buffer = ByteBuffer.wrap(outputBytes);
    blockMetaReader = new PrefixTreeBlockMeta(buffer);

    searcher = new PrefixTreeArraySearcher(blockMetaReader, blockMetaReader.getRowTreeDepth(),
        blockMetaReader.getMaxRowLength(), blockMetaReader.getMaxQualifierLength(),
        blockMetaReader.getMaxTagsLength());
    searcher.initOnBlock(blockMetaReader, outputBytes, includeMemstoreTS);
  }

  @Test
  public void testEncoderOutput() throws IOException {
    Assert.assertEquals(totalBytes, outputBytes.length);
    Assert.assertEquals(blockMetaWriter, blockMetaReader);
  }

  @Test
  public void testForwardScanner() {
    int counter = -1;
    while (searcher.advance()) {
      ++counter;
      KeyValue inputKv = rows.getInputs().get(counter);
      KeyValue outputKv = KeyValueUtil.copyToNewKeyValue(searcher.current());
      assertKeyAndValueEqual(inputKv, outputKv);
    }
    // assert same number of cells
    Assert.assertEquals(rows.getInputs().size(), counter + 1);
  }


  /**
   * probably not needed since testReverseScannerWithJitter() below is more thorough
   */
  @Test
  public void testReverseScanner() {
    searcher.positionAfterLastCell();
    int counter = -1;
    while (searcher.previous()) {
      ++counter;
      int oppositeIndex = rows.getInputs().size() - counter - 1;
      KeyValue inputKv = rows.getInputs().get(oppositeIndex);
      KeyValue outputKv = KeyValueUtil.copyToNewKeyValue(searcher.current());
      assertKeyAndValueEqual(inputKv, outputKv);
    }
    Assert.assertEquals(rows.getInputs().size(), counter + 1);
  }


  /**
   * Exercise the nubCellsRemain variable by calling next+previous.  NubCellsRemain is basically
   * a special fan index.
   */
  @Test
  public void testReverseScannerWithJitter() {
    searcher.positionAfterLastCell();
    int counter = -1;
    while (true) {
      boolean foundCell = searcher.previous();
      if (!foundCell) {
        break;
      }
      ++counter;

      // a next+previous should cancel out
      if (!searcher.isAfterLast()) {
        searcher.advance();
        searcher.previous();
      }

      int oppositeIndex = rows.getInputs().size() - counter - 1;
      KeyValue inputKv = rows.getInputs().get(oppositeIndex);
      KeyValue outputKv = KeyValueUtil.copyToNewKeyValue(searcher.current());
      assertKeyAndValueEqual(inputKv, outputKv);
    }
    Assert.assertEquals(rows.getInputs().size(), counter + 1);
  }

  @Test
  public void testIndividualBlockMetaAssertions() {
    rows.individualBlockMetaAssertions(blockMetaReader);
  }


  /**************** helper **************************/

  protected void assertKeyAndValueEqual(Cell expected, Cell actual) {
    // assert keys are equal (doesn't compare values)
    Assert.assertEquals(expected, actual);
    if (includeMemstoreTS) {
      Assert.assertEquals(expected.getMvccVersion(), actual.getMvccVersion());
    }
    // assert values equal
    Assert.assertTrue(Bytes.equals(expected.getValueArray(), expected.getValueOffset(),
      expected.getValueLength(), actual.getValueArray(), actual.getValueOffset(),
      actual.getValueLength()));
  }

}
