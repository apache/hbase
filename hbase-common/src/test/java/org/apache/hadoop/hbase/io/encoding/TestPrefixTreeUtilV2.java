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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.PrefixTreeDataWidth;
import org.apache.hadoop.hbase.io.encoding.PrefixTreeUtil.TokenizerNode;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestPrefixTreeUtilV2 {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrefixTreeUtilV2.class);
  private static byte[] FAM = Bytes.toBytes("cf");

  @Test
  public void testSearchPrefixTree() throws IOException {
    List<KeyValue> rows = new ArrayList<>();

    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201519-wx0t"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201519-wx0zcldi7lnsiyas-N"), FAM,
      Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201520-wx0re"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201520-wx0x7"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201521-wx05xfbtw2mopyhs-C"), FAM,
      Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201521-wx08"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201521-wx0c"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c7-202206201522-wx0t"), FAM, Bytes.toBytes("qh")));
    rows.add(new KeyValue(Bytes.toBytes("00c8-202206200751-wx0ah4gnbwptdyna-F"), FAM,
      Bytes.toBytes("qh")));

    List<KeyValue.KeyOnlyKeyValue> childs = new ArrayList<>(15);
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(0)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(1)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(2)))));

    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(rows.get(3)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(
      PrivateCellUtil.getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(
        new KeyValue(Bytes.toBytes("00c7-202206201520-wx0x7"), FAM, Bytes.toBytes("qo"))))));

    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(4)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(
      PrivateCellUtil.getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(
        new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qb"))))));
    childs.add(new KeyValue.KeyOnlyKeyValue(
      PrivateCellUtil.getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(
        new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qf"))))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(rows.get(4)))));

    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(5)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(6)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(7)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(8)))));
    childs.add(new KeyValue.KeyOnlyKeyValue(PrivateCellUtil
      .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRow(rows.get(9)))));

    TokenizerNode node = PrefixTreeUtilV2.buildPrefixTree(childs);

    PrefixTreeDataWidth dataWidth = new PrefixTreeDataWidth();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrefixTreeUtilV2.serializePrefixTree(node, dataWidth, outputStream);
    byte[] data = outputStream.toByteArray();
    ByteBuffer prefixTreeNodeData = ByteBuffer.wrap(data);
    for (int i = 0; i < childs.size(); i++) {
      byte[] result = PrefixTreeUtilV2.get(prefixTreeNodeData, 0, dataWidth, i);
      Assert.assertTrue(Bytes.compareTo(result, CellUtil.cloneRow(childs.get(i))) == 0);
    }

    for (int i = 0; i < childs.size(); i++) {
      int result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, childs.get(i), 0, dataWidth);
      Assert.assertEquals(i, result);
    }

    Cell skey = PrivateCellUtil.createFirstOnRow(Bytes.toBytes("00c7-202206201519"));
    int result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(-1, result);

    skey = PrivateCellUtil.createFirstOnRow(Bytes.toBytes("00c7-202206201520"));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(1, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201520-wx0x7"), FAM, Bytes.toBytes("qa")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(2, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201520-wx0x7"), FAM, Bytes.toBytes("qm")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(3, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201520-wx0x7"), FAM, Bytes.toBytes("qs")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(4, result);

    skey = PrivateCellUtil.createFirstOnRow(Bytes.toBytes("00c7-202206201520-wx0x7-"));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(4, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qa")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(5, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qe")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(6, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qg")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(7, result);

    skey = PrivateCellUtil.createFirstOnRowCol(
      new KeyValue(Bytes.toBytes("00c7-202206201521"), FAM, Bytes.toBytes("qu")));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(8, result);

    skey = PrivateCellUtil.createFirstOnRow(Bytes.toBytes("00c7-202206201521-wx0"));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(8, result);

    skey = PrivateCellUtil.createFirstOnRow(Bytes.toBytes("00c8-202206200751-wx0ah4gnbwptdyna-F-"));
    result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, skey, 0, dataWidth);
    Assert.assertEquals(13, result);
  }

  @Test
  public void testSearchPrefixTreeWithTimeStampType() throws IOException {
    List<KeyValue> rows = new ArrayList<>();

    rows.add(new KeyValue(Bytes.toBytes("00073123012802202_121_9223370375843575807"), FAM,
      Bytes.toBytes("bg_id"), 1661023473524L, KeyValue.Type.Put));
    rows.add(new KeyValue(Bytes.toBytes("00073124151608102_121_9223370375238775807"), FAM,
      Bytes.toBytes("cur_run_date"), 1661713633365L, KeyValue.Type.Put));
    rows.add(new KeyValue(Bytes.toBytes("00073124151608102_121_9223370375670775807"), FAM,
      Bytes.toBytes("run"), Long.MAX_VALUE, KeyValue.Type.Maximum));

    List<KeyValue.KeyOnlyKeyValue> childs = new ArrayList<>(3);
    childs.add(
      new KeyValue.KeyOnlyKeyValue(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(rows.get(0))));
    childs.add(
      new KeyValue.KeyOnlyKeyValue(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(rows.get(1))));
    childs.add(
      new KeyValue.KeyOnlyKeyValue(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(rows.get(2))));

    TokenizerNode node = PrefixTreeUtilV2.buildPrefixTree(childs);

    PrefixTreeDataWidth dataWidth = new PrefixTreeDataWidth();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrefixTreeUtilV2.serializePrefixTree(node, dataWidth, outputStream);
    byte[] data = outputStream.toByteArray();
    ByteBuffer prefixTreeNodeData = ByteBuffer.wrap(data);

    for (int i = 0; i < childs.size(); i++) {
      byte[] result = PrefixTreeUtilV2.get(prefixTreeNodeData, 0, dataWidth, i);
      Assert.assertTrue(Bytes.compareTo(result, CellUtil.cloneRow(childs.get(i))) == 0);
    }

    for (int i = 0; i < childs.size(); i++) {
      int result = PrefixTreeUtilV2.search(prefixTreeNodeData, 0, childs.get(i), 0, dataWidth);
      Assert.assertEquals(i, result);
    }
  }

}
