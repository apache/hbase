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
package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestPrefixTreeUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrefixTreeUtil.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefixTreeUtil.class);

  @Test
  public void testSearchPrefixTree() throws IOException {
    List<byte[]> childs = new ArrayList<>();
    childs.add(Bytes.toBytes("00c7-202206201519-wx0t"));
    childs.add(Bytes.toBytes("00c7-202206201519-wx0zcldi7lnsiyas-N"));
    childs.add(Bytes.toBytes("00c7-202206201520-wx0re"));
    childs.add(Bytes.toBytes("00c7-202206201520-wx0ulgrwi7d542tm-N"));
    childs.add(Bytes.toBytes("00c7-202206201520-wx0x7"));
    childs.add(Bytes.toBytes("00c7-202206201521"));
    childs.add(Bytes.toBytes("00c7-202206201521-wx05xfbtw2mopyhs-C"));
    childs.add(Bytes.toBytes("00c7-202206201521-wx08"));
    childs.add(Bytes.toBytes("00c7-202206201521-wx0c"));
    childs.add(Bytes.toBytes("00c7-202206201521-wx0go"));
    childs.add(Bytes.toBytes("00c7-202206201522-wx0t"));
    childs.add(Bytes.toBytes("00c8-202206200751-wx0ah4gnbwptdyna-F"));

    PrefixTreeUtil.TokenizerNode node = PrefixTreeUtil.buildPrefixTree(childs);
    PrefixTreeUtil.PrefixTreeDataWidth dataWidth = new PrefixTreeUtil.PrefixTreeDataWidth();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrefixTreeUtil.serializePrefixTree(node, dataWidth, outputStream);
    byte[] data = outputStream.toByteArray();
    ByteBuffer prefixTreeNodeData = ByteBuffer.wrap(data);
    for (int i = 0; i < childs.size(); i++) {
      byte[] result = PrefixTreeUtil.get(prefixTreeNodeData, 0, dataWidth, i);
      Assert.assertTrue(Bytes.compareTo(result, childs.get(i)) == 0);
    }

    for (int i = 0; i < childs.size(); i++) {
      int result = PrefixTreeUtil.search(prefixTreeNodeData, 0, childs.get(i), 0, dataWidth);
      Assert.assertEquals(result, i);
    }

    byte[] skey = Bytes.toBytes("00c7-202206201519");
    int result = PrefixTreeUtil.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(-1, result);

    skey = Bytes.toBytes("00c7-202206201520");
    result = PrefixTreeUtil.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(1, result);

    skey = Bytes.toBytes("00c7-202206201520-wx0x7-");
    result = PrefixTreeUtil.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(4, result);

    skey = Bytes.toBytes("00c7-202206201521-wx0");
    result = PrefixTreeUtil.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(5, result);

    skey = Bytes.toBytes("00c8-202206200751-wx0ah4gnbwptdyna-F-");
    result = PrefixTreeUtil.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(11, result);
  }

}
