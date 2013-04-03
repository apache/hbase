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

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import com.google.common.collect.Lists;

/*
 * Goes beyond a trivial trie to add a branch on the "cf" node
 */
public class TestRowDataDeeper extends BaseTestRowData{

	static byte[]
        cdc = Bytes.toBytes("cdc"),
        cf6 = Bytes.toBytes("cf6"),
        cfc = Bytes.toBytes("cfc"),
        f = Bytes.toBytes("f"),
        q = Bytes.toBytes("q"),
        v = Bytes.toBytes("v");

	static long
		ts = 55L;

	static List<KeyValue> d = Lists.newArrayList();
	static{
		d.add(new KeyValue(cdc, f, q, ts, v));
    d.add(new KeyValue(cf6, f, q, ts, v));
    d.add(new KeyValue(cfc, f, q, ts, v));
	}

	@Override
	public List<KeyValue> getInputs() {
		return d;
	}

	@Override
	public void individualBlockMetaAssertions(PrefixTreeBlockMeta blockMeta) {
	  //0: token:c; fan:d,f
	  //1: token:f; fan:6,c
	  //2: leaves
		Assert.assertEquals(3, blockMeta.getRowTreeDepth());
	}

  @Override
  public void individualSearcherAssertions(CellSearcher searcher) {
    /**
     * The searcher should get a token mismatch on the "r" branch.  Assert that it skips not only
     * rA, but rB as well.
     */
    KeyValue cfcRow = KeyValue.createFirstOnRow(Bytes.toBytes("cfc"));
    CellScannerPosition position = searcher.positionAtOrAfter(cfcRow);
    Assert.assertEquals(CellScannerPosition.AFTER, position);
    Assert.assertEquals(d.get(2), searcher.current());
    searcher.previous();
    Assert.assertEquals(d.get(1), searcher.current());
  }
}


