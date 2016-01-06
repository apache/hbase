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
package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class TestRowDataTrivialWithTags extends BaseTestRowData{
  static byte[] rA = Bytes.toBytes("rA"), rB = Bytes.toBytes("rB"),// turn "r"
                                                                   // into a
                                                                   // branch for
                                                                   // the
                                                                   // Searcher
                                                                   // tests
      cf = Bytes.toBytes("fam"), cq0 = Bytes.toBytes("q0"), v0 = Bytes.toBytes("v0");

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    List<Tag> tagList = new ArrayList<Tag>();
    Tag t = new ArrayBackedTag((byte) 1, "visisbility");
    tagList.add(t);
    t = new ArrayBackedTag((byte) 2, "ACL");
    tagList.add(t);
    d.add(new KeyValue(rA, cf, cq0, ts, v0, tagList));
    d.add(new KeyValue(rB, cf, cq0, ts, v0, tagList));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

  @Override
  public void individualBlockMetaAssertions(PrefixTreeBlockMeta blockMeta) {
    // node[0] -> root[r]
    // node[1] -> leaf[A], etc
    Assert.assertEquals(2, blockMeta.getRowTreeDepth());
  }

  @Override
  public void individualSearcherAssertions(CellSearcher searcher) {
    /**
     * The searcher should get a token mismatch on the "r" branch. Assert that
     * it skips not only rA, but rB as well.
     */
    KeyValue afterLast = KeyValueUtil.createFirstOnRow(Bytes.toBytes("zzz"));
    CellScannerPosition position = searcher.positionAtOrAfter(afterLast);
    Assert.assertEquals(CellScannerPosition.AFTER_LAST, position);
    Assert.assertNull(searcher.current());
  }
}
