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

package org.apache.hadoop.hbase.codec.prefixtree.column;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.decode.column.ColumnReader;
import org.apache.hadoop.hbase.codec.prefixtree.encode.column.ColumnSectionWriter;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.ColumnNodeType;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.Tokenizer;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ByteRangeUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.byterange.impl.ByteRangeTreeSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@Category({MiscTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestColumnBuilder {

  @Parameters
  public static Collection<Object[]> parameters() {
    return new TestColumnData.InMemory().getAllAsObjectArray();
  }

  /*********** fields **********************************/

  protected TestColumnData columns;
  protected ByteRangeTreeSet columnSorter;
  protected List<ByteRange> sortedUniqueColumns;
  protected PrefixTreeBlockMeta blockMeta;
  protected Tokenizer builder;
  protected ColumnSectionWriter writer;
  protected byte[] bytes;
  protected byte[] buffer;
  protected ColumnReader reader;

  /*************** construct ****************************/

  public TestColumnBuilder(TestColumnData columns) {
    this.columns = columns;
    List<ByteRange> inputs = columns.getInputs();
    this.columnSorter = new ByteRangeTreeSet(inputs);
    this.sortedUniqueColumns = columnSorter.compile().getSortedRanges();
    List<byte[]> copies = ByteRangeUtils.copyToNewArrays(sortedUniqueColumns);
    Assert.assertTrue(Bytes.isSorted(copies));
    this.blockMeta = new PrefixTreeBlockMeta();
    this.blockMeta.setNumMetaBytes(0);
    this.blockMeta.setNumRowBytes(0);
    this.builder = new Tokenizer();
  }

  /************* methods ********************************/

  @Test
  public void testReaderRoundTrip() throws IOException {
    for (int i = 0; i < sortedUniqueColumns.size(); ++i) {
      ByteRange column = sortedUniqueColumns.get(i);
      builder.addSorted(column);
    }
    List<byte[]> builderOutputArrays = builder.getArrays();
    for (int i = 0; i < builderOutputArrays.size(); ++i) {
      byte[] inputArray = sortedUniqueColumns.get(i).deepCopyToNewArray();
      byte[] outputArray = builderOutputArrays.get(i);
      boolean same = Bytes.equals(inputArray, outputArray);
      Assert.assertTrue(same);
    }
    Assert.assertEquals(sortedUniqueColumns.size(), builderOutputArrays.size());

    writer = new ColumnSectionWriter(blockMeta, builder, ColumnNodeType.QUALIFIER);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writer.compile().writeBytes(baos);
    bytes = baos.toByteArray();
    buffer = new byte[blockMeta.getMaxQualifierLength()];
    reader = new ColumnReader(buffer, ColumnNodeType.QUALIFIER);
    reader.initOnBlock(blockMeta, bytes);

    List<TokenizerNode> builderNodes = Lists.newArrayList();
    builder.appendNodes(builderNodes, true, true);
    int i = 0;
    for (TokenizerNode builderNode : builderNodes) {
      if (!builderNode.hasOccurrences()) {
        continue;
      }
      Assert.assertEquals(1, builderNode.getNumOccurrences());// we de-duped before adding to
                                                              // builder
      int position = builderNode.getOutputArrayOffset();
      byte[] output = reader.populateBuffer(position).copyBufferToNewArray();
      boolean same = Bytes.equals(sortedUniqueColumns.get(i).deepCopyToNewArray(), output);
      Assert.assertTrue(same);
      ++i;
    }
  }

}
