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
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeTestConstants;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.hbase.util.byterange.impl.ByteRangeTreeSet;

import com.google.common.collect.Lists;

/*
 * test different timestamps
 * 
 * http://pastebin.com/7ks8kzJ2
 * http://pastebin.com/MPn03nsK
 */
public class TestRowDataUrls extends BaseTestRowData{

  static List<ByteRange> rows;
  static{
    List<String> rowStrings = new ArrayList<String>();
    rowStrings.add("com.edsBlog/directoryAa/pageAaa");
    rowStrings.add("com.edsBlog/directoryAa/pageBbb");
    rowStrings.add("com.edsBlog/directoryAa/pageCcc");
    rowStrings.add("com.edsBlog/directoryAa/pageDdd");
    rowStrings.add("com.edsBlog/directoryBb/pageEee");
    rowStrings.add("com.edsBlog/directoryBb/pageFff");
    rowStrings.add("com.edsBlog/directoryBb/pageGgg");
    rowStrings.add("com.edsBlog/directoryBb/pageHhh");
    rowStrings.add("com.isabellasBlog/directoryAa/pageAaa");
    rowStrings.add("com.isabellasBlog/directoryAa/pageBbb");
    rowStrings.add("com.isabellasBlog/directoryAa/pageCcc");
    rowStrings.add("com.isabellasBlog/directoryAa/pageDdd");
    rowStrings.add("com.isabellasBlog/directoryBb/pageEee");
    rowStrings.add("com.isabellasBlog/directoryBb/pageFff");
    rowStrings.add("com.isabellasBlog/directoryBb/pageGgg");
    rowStrings.add("com.isabellasBlog/directoryBb/pageHhh");
    ByteRangeTreeSet ba = new ByteRangeTreeSet();
    for (String row : rowStrings) {
      ba.add(new SimpleMutableByteRange(Bytes.toBytes(row)));
    }
    rows = ba.compile().getSortedRanges();
  }

  static List<String> cols = Lists.newArrayList();
  static {
    cols.add("Chrome");
    cols.add("Chromeb");
    cols.add("Firefox");
    cols.add("InternetExplorer");
    cols.add("Opera");
    cols.add("Safari");
  }

  static long ts = 1234567890;

  static int MAX_VALUE = 50;

  static List<KeyValue> kvs = Lists.newArrayList();
  static {
    for (ByteRange row : rows) {
      for (String col : cols) {
        KeyValue kv = new KeyValue(row.deepCopyToNewArray(), PrefixTreeTestConstants.TEST_CF,
            Bytes.toBytes(col), ts, KeyValue.Type.Put, Bytes.toBytes("VALUE"));
        kvs.add(kv);
        // System.out.println("TestRows5:"+kv);
      }
    }
  }

  @Override
  public List<KeyValue> getInputs() {
    return kvs;
  }

}
