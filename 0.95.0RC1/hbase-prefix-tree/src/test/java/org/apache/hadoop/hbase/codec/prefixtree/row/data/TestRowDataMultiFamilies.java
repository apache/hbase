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
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestRowDataMultiFamilies extends BaseTestRowData{

  static byte[] 
        rowA = Bytes.toBytes("rowA"),
        rowB = Bytes.toBytes("rowB"),
        famA = Bytes.toBytes("famA"),
        famB = Bytes.toBytes("famB"),
        famBB = Bytes.toBytes("famBB"),
        q0 = Bytes.toBytes("q0"),
        q1 = Bytes.toBytes("q1"),//start with a different character
        vvv = Bytes.toBytes("vvv");

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    d.add(new KeyValue(rowA, famA, q0, ts, vvv));
    d.add(new KeyValue(rowA, famB, q1, ts, vvv));
    d.add(new KeyValue(rowA, famBB, q0, ts, vvv));
    d.add(new KeyValue(rowB, famA, q0, ts, vvv));
    d.add(new KeyValue(rowB, famA, q1, ts, vvv));
    d.add(new KeyValue(rowB, famB, q0, ts, vvv));
    d.add(new KeyValue(rowB, famBB, q0, ts, vvv));
    d.add(new KeyValue(rowB, famBB, q1, ts, vvv));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}
