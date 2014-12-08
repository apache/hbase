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

import java.util.List;

import org.apache.hadoop.hbase.codec.prefixtree.encode.tokenize.Tokenizer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestTreeDepth {

  @Test
  public void testSingleNode() {
    List<String> inputs = Lists.newArrayList("a");
    testInternal(inputs, 1);
  }

  @Test
  public void testSimpleBranch() {
    List<String> inputs = Lists.newArrayList("a", "aa", "ab");
    testInternal(inputs, 2);
  }

  @Test
  public void testEmptyRoot() {
    List<String> inputs = Lists.newArrayList("a", "b");
    testInternal(inputs, 2);
  }

  @Test
  public void testRootAsNub() {
    List<String> inputs = Lists.newArrayList("a", "aa");
    testInternal(inputs, 2);
  }

  @Test
  public void testRootAsNubPlusNub() {
    List<String> inputs = Lists.newArrayList("a", "aa", "aaa");
    testInternal(inputs, 3);
  }

  @Test
  public void testEmptyRootPlusNub() {
    List<String> inputs = Lists.newArrayList("a", "aa", "b");
    testInternal(inputs, 3);
  }

  @Test
  public void testSplitDistantAncestor() {
    List<String> inputs = Lists.newArrayList("a", "ac", "acd", "b");
    testInternal(inputs, 4);
  }

  protected void testInternal(List<String> inputs, int expectedTreeDepth) {
    Tokenizer builder = new Tokenizer();
    for (String s : inputs) {
      SimpleByteRange b = new SimpleByteRange(Bytes.toBytes(s));
      builder.addSorted(b);
    }
    Assert.assertEquals(1, builder.getRoot().getNodeDepth());
    Assert.assertEquals(expectedTreeDepth, builder.getTreeDepth());
  }

}
