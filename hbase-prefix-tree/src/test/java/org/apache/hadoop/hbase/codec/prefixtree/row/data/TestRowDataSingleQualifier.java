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

public class TestRowDataSingleQualifier extends BaseTestRowData{

  static byte[]
    rowA = Bytes.toBytes("rowA"),
    rowB = Bytes.toBytes("rowB"),
    cf = PrefixTreeTestConstants.TEST_CF,
    cq0 = Bytes.toBytes("cq0"),
    v0 = Bytes.toBytes("v0");

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    d.add(new KeyValue(rowA, cf, cq0, ts, v0));
    d.add(new KeyValue(rowB, cf, cq0, ts, v0));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}
