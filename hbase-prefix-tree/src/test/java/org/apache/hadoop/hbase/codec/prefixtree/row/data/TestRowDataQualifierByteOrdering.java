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

public class TestRowDataQualifierByteOrdering extends BaseTestRowData{

  static byte[]
    Arow = Bytes.toBytes("Arow"),
    Brow = Bytes.toBytes("Brow"),
    Brow2 = Bytes.toBytes("Brow2"),
    fam = Bytes.toBytes("HappyFam"),
    cq0 = Bytes.toBytes("cq0"),
    cq1 = Bytes.toBytes("cq1tail"),//make sure tail does not come back as liat
    cq2 = Bytes.toBytes("cq2"),
    v0 = Bytes.toBytes("v0");

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    d.add(new KeyValue(Arow, fam, cq0, ts, v0));
    d.add(new KeyValue(Arow, fam, cq1, ts, v0));
    d.add(new KeyValue(Brow, fam, cq0, ts, v0));
    d.add(new KeyValue(Brow, fam, cq2, ts, v0));
    d.add(new KeyValue(Brow2, fam, cq1, ts, v0));
    d.add(new KeyValue(Brow2, fam, cq2, ts, v0));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}
