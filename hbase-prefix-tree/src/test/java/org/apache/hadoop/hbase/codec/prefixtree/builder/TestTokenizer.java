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

package org.apache.hadoop.hbase.codec.prefixtree.builder;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.Tokenizer;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerNode;
import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.TokenizerRowSearchResult;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestTokenizer {

  @Parameters
  public static Collection<Object[]> parameters() {
    return new TestTokenizerData.InMemory().getAllAsObjectArray();
  }

  private List<byte[]> inputs;
  private Tokenizer builder;
  private List<byte[]> roundTripped;

  public TestTokenizer(TestTokenizerData sortedByteArrays) {
    this.inputs = sortedByteArrays.getInputs();
    this.builder = new Tokenizer();
    for (byte[] array : inputs) {
      builder.addSorted(new SimpleByteRange(array));
    }
    this.roundTripped = builder.getArrays();
  }

  @Test
  public void testReaderRoundTrip() {
    Assert.assertEquals(inputs.size(), roundTripped.size());
    Assert.assertTrue(Bytes.isSorted(roundTripped));
    Assert.assertTrue(Bytes.equals(inputs, roundTripped));
  }

  @Test
  public void testSearching() {
    for (byte[] input : inputs) {
      TokenizerRowSearchResult resultHolder = new TokenizerRowSearchResult();
      builder.getNode(resultHolder, input, 0, input.length);
      TokenizerNode n = resultHolder.getMatchingNode();
      byte[] output = n.getNewByteArray();
      Assert.assertTrue(Bytes.equals(input, output));
    }
  }

}
