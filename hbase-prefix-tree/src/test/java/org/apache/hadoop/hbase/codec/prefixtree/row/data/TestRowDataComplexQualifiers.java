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
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeTestConstants;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestRowDataComplexQualifiers extends BaseTestRowData{

  static byte[]
    Arow = Bytes.toBytes("Arow"),
    cf = PrefixTreeTestConstants.TEST_CF,
    v0 = Bytes.toBytes("v0");

  static List<byte[]> qualifiers = Lists.newArrayList();
  static {
    List<String> qualifierStrings = Lists.newArrayList();
    qualifierStrings.add("cq");
    qualifierStrings.add("cq0");
    qualifierStrings.add("cq1");
    qualifierStrings.add("cq2");
    qualifierStrings.add("dq0");// second root level fan
    qualifierStrings.add("dq1");// nub
    qualifierStrings.add("dq111");// leaf on nub
    qualifierStrings.add("dq11111a");// leaf on leaf
    for (String s : qualifierStrings) {
      qualifiers.add(Bytes.toBytes(s));
    }
  }

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    for (byte[] qualifier : qualifiers) {
      d.add(new KeyValue(Arow, cf, qualifier, ts, v0));
    }
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}
